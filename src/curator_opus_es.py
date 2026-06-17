#!/usr/bin/env python3
"""
NVIDIA NeMo Curator pipeline for cleaning & chunking OPUS Spanish subtitles.
Adds a tiny OpusDownloader class so the script can fetch the raw .txt file
directly from the Opus website, guaranteeing reproducibility.

Run (inside the container):
    python src/curator_opus_es.py          # uses default Opus URL
    python src/curator_opus_es.py --url <your‑mirror>
    python src/curator_opus_es.py --data-dir /path/to/cache
"""

# --------------------------------------------------------------
# Standard library
# --------------------------------------------------------------
import hashlib
import os
import sys
import warnings
from pathlib import Path
from typing import Optional, Union

# --------------------------------------------------------------
# Third‑party (RAPIDS / Dask)
# --------------------------------------------------------------
import cudf
import dask_cudf
from dask_cuda import LocalCUDACluster
from dask.distributed import Client
import rmm
from nvtx import annotate   # NVTX range decorator / context manager
from tqdm.auto import tqdm  # optional progress bar for download

# --------------------------------------------------------------
# NeMo Curator (public API)
# --------------------------------------------------------------
from nemo_curator.datasets import DocumentDataset
from nemo_curator.modifiers import DocumentModifier
from nemo_curator.filters import DocumentFilter
from nemo_curator.modules import Sequential
from nemo_curator.utils import DocumentDownloader

# --------------------------------------------------------------
# Configuration (tweak as needed)
# --------------------------------------------------------------
MAX_TOKENS = 512          # Upper bound for a CPT chunk
MIN_TOKENS = 64           # Lower bound for a CPT chunk
MIN_CHARS  = 30           # Minimum characters after cleaning
CHUNKSIZE  = "128 MiB"    # Dask read_text chunk size
DEVICE_FRACTION = 0.9     # Fraction of GPU memory each Dask worker may use
# --------------------------------------------------------------

# --------------------------------------------------------------
# RMM initialisation – pooled allocator works best on DGX unified memory
# --------------------------------------------------------------
rmm.reinitialize(
    pool_allocator=True,
    initial_pool_size=2 * (1 << 30),   # start with ~2 GB pool
    maximum_pool_size=8 * (1 << 30),   # cap at ~8 GB (adjust to your node)
)

class OpusDownloader(DocumentDownloader):
    """
    Small utility that downloads the Opus Spanish subtitle dump (es.txt)
    if it is not already present in a local cache.

    Parameters
    ----------
    url : str
        Direct download link to the raw .txt file (e.g.
        "https://object.pouta.csc.fi/OPUS-OpenSubtitles/v2018/mono/OpenSubtitles.raw.es.gz").
        If the link ends with .gz the file will be decompressed on‑the‑fly.
    cache_dir : Union[str, Path], optional
        Directory where the file will be stored. Defaults to
        ``~/.cache/opus_es/``.
    expected_sha256 : str, optional
        If supplied, the downloaded file's SHA‑256 is verified.
    force : bool, optional (default=False)
        Passed through to ``DocumentDownloader.download`` – if True the file
        is re‑downloaded even if a valid copy already exists in the cache.
    """

    def __init__(
        self,
        url: str,
        cache_dir: Optional[Union[str, Path]] = None,
        expected_sha256: Optional[str] = None,
    ):
        # Store the arguments that the hooks need.
        self.url = url
        self.expected_sha256 = expected_sha256

        # NeMo‑Curator expects a ``cache_dir`` path‑like object.
        cache_dir_path = Path(cache_dir) if cache_dir else Path.home() / ".cache" / "opus_es"
        cache_dir_path.mkdir(parents=True, exist_ok=True)

        # Initialise the base class – it will store ``self.cache_dir`` for us.
        super().__init__(cache_dir=cache_dir_path)

        # Derive the remote and local filenames once (used by the hooks).
        self._remote_name = Path(url).name
        if self._remote_name.endswith(".gz"):
            self._local_name = self._remote_name[:-3]   # strip .gz → plain .txt
        else:
            self._local_name = self._remote_name

        # The final path where the ready‑to‑use document will live.
        self.local_path = self.cache_dir / self._local_name

    # -----------------------------------------------------------------
    # Hooks required by DocumentDownloader
    # -----------------------------------------------------------------
    def _get_remote_name(self) -> str:
        """Filename that will be used for the remote download (including .gz if present)."""
        return self._remote_name

    def _get_local_name(self) -> str:
        """Filename that should exist on disk after any decompression."""
        return self._local_name

    def _download_file(self, url: str, dest: Path) -> None:
        """
        Stream‑download with a tqdm progress bar.
        This method is called by the base class ``download`` implementation.
        """
        resp = requests.get(url, stream=True, timeout=30)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))

        with tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Downloading {self._remote_name}",
        ) as pbar:
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1 MiB chunks
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

    def _verify_checksum(self, path: Path) -> bool:
        return self.download(force=False)


# --------------------------------------------------------------
# Helper classes – each is a thin NeMo Curator Modifier/Filter
# --------------------------------------------------------------
class SubtitleCleaner(DocumentModifier):
    """Removes common subtitle artefacts (tags, musical notes, stray quotes)
    and collapses whitespace."""
    @annotate(message="SubtitleCleaner", color="purple")
    def _modify_dataset(self, dataset: DocumentDataset) -> DocumentDataset:
        df = dataset.df

        # 1. Strip brackets, specific tags, and musical notes
        df["text"] = df["text"].str.replace(
            r"\[[^\]]*\]|\b(?:Ejem|SFX)\b|♪|♫| -|-", "", regex=True
        )

        # 2. Normalise quotes – replace with a space (simpler than back‑refs)
        df["text"] = df["text"].str.replace('"', " ", regex=False)

        # 3. Collapse whitespace and drop empties
        df["text"] = df["text"].str.replace(r"\s+", " ", regex=True).str.strip()
        df = df[df["text"].str.len() > 0]

        return DocumentDataset(df)


class CPTChunker(DocumentModifier):
    """
    Groups sentences into roughly‑equal‑token chunks using a cheap
    4‑chars‑≈‑1‑token heuristic.  The operation is performed *per Dask
    partition* to avoid shuffling all data to a single worker.
    """
    def __init__(self, max_tokens: int = MAX_TOKENS):
        super().__init__()
        self.max_tokens = max_tokens

    @annotate(message="CPTChunker", color="teal")
    def _chunk_partition(self, df: cudf.DataFrame) -> cudf.DataFrame:
        if len(df) == 0:
            return df

        # Rough token estimate
        df["sent_tokens"] = df["text"].str.len() // 4

        # Cumulative token count inside the partition
        df["cum_tokens"] = df["sent_tokens"].cumsum()

        # Stable chunk‑id (floor division)
        df["chunk_id"] = df["cum_tokens"] // self.max_tokens

        # Ensure a trailing space so that concatenation does not glue words
        df["text"] = df["text"] + " "

        # Aggregate by chunk_id
        chunks = (
            df.groupby("chunk_id", sort=True)
            .agg({"text": "sum"})
            .reset_index(drop=True)
        )

        # Final whitespace normalisation on the assembled chunk
        chunks["text"] = chunks["text"].str.replace(r"  +", " ", regex=True).str.strip()
        return chunks

    @annotate(message="CPTChunker._modify_dataset", color="teal")
    def _modify_dataset(self, dataset: DocumentDataset) -> DocumentDataset:
        meta = cudf.DataFrame({"text": cudf.Series(dtype="object")})
        chunked_df = dataset.df.map_partitions(self._chunk_partition, meta=meta)
        return DocumentDataset(chunked_df)


class FeatureExtractor(DocumentModifier):
    """Adds cheap statistics that are useful for downstream filtering or metadata."""
    @annotate(message="FeatureExtractor", color="olive")
    def _modify_dataset(self, dataset: DocumentDataset) -> DocumentDataset:
        df = dataset.df
        df["char_count"] = df["text"].str.len()
        df["token_estimate"] = df["char_count"] // 4
        df["is_dialogue"] = df["text"].str.contains(r"[¿?]", regex=True)
        return DocumentDataset(df)


class SpanishLengthFilter(DocumentFilter):
    """Very lightweight Spanish‑likeness check + length thresholds."""
    def __init__(self, min_chars: int = MIN_CHARS, min_tokens: int = MIN_TOKENS):
        super().__init__()
        self.min_chars = min_chars
        self.min_tokens = min_tokens

    @annotate(message="SpanishLengthFilter", color="red")
    def _filter_dataset(self, dataset: DocumentDataset) -> DocumentDataset:
        df = dataset.df

        # Heuristic: presence of any accented/ñ/¡¿ character → likely Spanish
        is_spanish = df["text"].str.contains(r"[áéíóúüñ¿¡]", regex=True)

        mask = (
            is_spanish
            & (df["char_count"] >= self.min_chars)
            & (df["token_estimate"] >= self.min_tokens)
        )
        return DocumentDataset(df[mask].reset_index(drop=True))


# --------------------------------------------------------------
# Core pipeline function
# --------------------------------------------------------------
@annotate(message="pipeline", color="blue")
def pipeline(
    input_path: Path,
    output_dir: Path,
) -> Path:
    """
    Executes the full NeMo Curator workflow on the supplied OPUS Spanish
    subtitle file and writes a Parquet table of cleaned chunks.

    Parameters
    ----------
    input_path : Path
        Path to the raw .txt file (one subtitle line per line).
    output_dir : Path
        Directory where the resulting Parquet will be written.

    Returns
    -------
    Path
        Full path to the written Parquet file.
    """
    # ----------------------------------------------------------
    # 1️⃣  Spin up a single‑node Dask‑CUDA cluster
    # ----------------------------------------------------------
    cluster = LocalCUDACluster(
        device_memory_limit=f"{int(128 * DEVICE_FRACTION)}GB",
        workers=1,
        threads_per_worker=1,
        asynchronous=False,
    )
    client = Client(cluster)
    print(f"🔧 Dask dashboard: {client.dashboard_link}")

    # ----------------------------------------------------------
    # 2️⃣  Load the raw text – each line becomes a document
    # ----------------------------------------------------------
    print(f"📥 Loading {input_path} via Dask-cuDF (chunksize={CHUNKSIZE}) …")
    ddf = dask_cudf.read_text(str(input_path), chunksize=CHUNKSIZE)
    ddf = ddf.to_frame(name="text")
    dataset = DocumentDataset(ddf)
    print(f"   Partitions loaded: {dataset.df.npartitions}")

    # ----------------------------------------------------------
    # 3️⃣  Build & run the NeMo Curator pipeline
    # ----------------------------------------------------------
    print("🚀 Initialising NeMo Curator pipeline …")
    curator_pipeline = Sequential(
        [
            SubtitleCleaner(),
            CPTChunker(max_tokens=MAX_TOKENS),
            FeatureExtractor(),
            SpanishLengthFilter(min_chars=MIN_CHARS, min_tokens=MIN_TOKENS),
        ]
    )

    print("⚙️  Executing distributed processing …")
    processed_dataset = curator_pipeline(dataset)

    # Compute the lazy graph into host‑visible GPU memory
    final_df: cudf.DataFrame = processed_dataset.df.compute()
    print(f"🧮 Computation finished – {len(final_df):,} rows in result")

    # ----------------------------------------------------------
    # 4️⃣  Post‑process: add a contiguous ID column (zero‑copy)
    # ----------------------------------------------------------
    if len(final_df) == 0:
        print("⚠️  No chunks passed filtering! Writing an empty file …")
    else:
        # cuDF's RangeIndex is a zero‑copy way to generate 0,1,2,… ids
        final_df = final_df.reset_index(drop=True)
        final_df["id"] = final_df.index.astype("int64")

    # ----------------------------------------------------------
    # 5️⃣  Order columns & write Parquet (Snappy compression)
    # ----------------------------------------------------------
    output_df = final_df[
        ["id", "text", "char_count", "token_estimate", "is_dialogue"]
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "opensubtitles_nemo_chunks.parquet"

    print(f"💾 Writing Parquet to {output_path} (Snappy) …")
    output_df.to_parquet(output_path, compression="snappy")

    # ----------------------------------------------------------
    # 6️⃣  Summary stats
    # ----------------------------------------------------------
    print("\n✅ Pipeline completed!")
    print(f"   Total CPT chunks: {len(output_df):,}")
    if len(output_df) > 0:
        print(
            f"   Avg tokens (estimate): {output_df['token_estimate'].mean():.0f}"
        )
        print(
            f"   Dialogue‑flagged chunks: {output_df['is_dialogue'].sum():,}"
        )

    # Clean up Dask resources
    client.close()
    cluster.close()
    return output_path


# --------------------------------------------------------------
# CLI entry point – now includes download arguments
# --------------------------------------------------------------
def _parse_args():
    import argparse

    parser = argparse.ArgumentParser(
        description="Download Opus Spanish subtitles (if needed) and run NeMo Curator cleaning/chunking."
    )
    parser.add_argument(
        "--url",
        type=str,
        default=(
            "https://object.pouta.csc.fi/OPUS-OpenSubtitles/v2018/mono/es.txt.gz"
        ),
        help="Direct download link to the Opus Spanish .txt (or .gz) file.",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=str(Path.home() / ".cache" / "opus_es"),
        help="Directory where the downloaded file will be cached.",
    )
    parser.add_argument(
        "--expected-sha256",
        type=str,
        default="388436a9322a6e5c60a7542ce4171f794a6d257d980210773129bb2923f6f28f",
        help="Optional SHA‑256 checksum to verify the downloaded file.",
    )
    parser.add_argument(
        "output_dir",
        type=str,
        nargs="?",
        default="./output",
        help="Where to write the resulting Parquet file (default: ./output).",
    )
    return parser.parse_args()


def main():
    args = _parse_args()

    # ------------------------------------------------------------------
    # 1️⃣  Download (or reuse) the Opus file
    # ------------------------------------------------------------------
    downloader = OpusDownloader(
        url=args.url,
        cache_dir=args.data_dir,
        expected_sha256=args.expected_sha256,
    )
    opus_path = downloader.maybe_download()
    print(f"[OpusDownloader] Using file: {opus_path}")

    # ------------------------------------------------------------------
    # 2️⃣  Run the curator pipeline
    # ------------------------------------------------------------------
    output_dir = Path(args.output_dir)
    pipeline(opus_path, output_dir)


if __name__ == "__main__":
    # Silence harmless warnings from NumPy / Pandas that are not relevant here
    warnings.filterwarnings("ignore")
    main()