from nemo_curator.core.client import RayClient
from nemo_curator.pipeline.pipeline import Pipeline
from nemo_curator.stages.text.download import DocumentDownloader, DocumentIterator, DocumentExtractor, DocumentDownloadExtractStage, URLGenerator
from nemo_curator.stages.base import ProcessingStage
from nemo_curator.stages.text.io.writer.parquet import ParquetWriter

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import gzip
import shutil
import os

from typing import Any
from collections.abc import Iterator
from dataclasses import dataclass

# implement URL Generator class for 2013, 2016, and 2018 OPUS ES datasets
@dataclass
class OPUSURLGenerator(URLGenerator):
    def generate_urls(self) -> list[str]:
        return [
            "https://object.pouta.csc.fi/OPUS-OpenSubtitles/v2018/mono/es.txt.gz",
            "https://object.pouta.csc.fi/OPUS-OpenSubtitles/v2016/mono/es.txt.gz",
            "https://object.pouta.csc.fi/OPUS-OpenSubtitles/v2013/mono/es.txt.gz",
        ]

# Create our custom document downloader class
class OPUSDownloader(DocumentDownloader):
    def __init__(self, download_dir: str):
        super().__init__(download_dir=download_dir)

        # intialize persistent connection
        self.session = requests.Session()

        # Configure exponential backoff
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "OPTIONS", "GET"],
        )

        # mount adapter to session
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)

    def _get_output_filename(self, url: str) -> str:
        # Extract the filename from the URL

        # extract gzip name first (OPUS will give us a GZIP file first)
        gzip_name = url.split("/")[-1]

        # get year of file
        year = url.split("/")[-3]

        # remove .gz extension to get the final filename
        final_name = year + "-" + gzip_name.split(".gz")[0]
        return final_name

    def _download_to_path(self, url: str, path: str) -> tuple[bool, str | None]:
        gz_path = path + ".gz"
    
        # 1. Initialize variables for the retry loop
        max_retries = 10
        retry_delay = 5
        
        print(f"Starting download of {url} to {gz_path}...")

        for attempt in range(max_retries):
            try:
                # 2. Check how much of the file we already have on disk
                headers = {}
                if os.path.exists(gz_path):
                    # Get exact byte count of the partial file
                    downloaded_bytes = os.path.getsize(gz_path)
                    print(f"Resuming download from byte {downloaded_bytes} (Attempt {attempt + 1}/{max_retries})...")
                    # Tell the server to start sending from this exact byte
                    headers['Range'] = f'bytes={downloaded_bytes}-'
                else:
                    downloaded_bytes = 0
                    print(f"Starting fresh download (Attempt {attempt + 1}/{max_retries})...")

                # 3. Request the stream with the custom Range header
                with self.session.get(url, headers=headers, stream=True, timeout=30) as response:
                    
                    # If the server doesn't support Range headers, it returns 200 (OK) and starts from 0.
                    # If it DOES support Range headers, it returns 206 (Partial Content).
                    # 416 means "Range Not Satisfiable" (we already downloaded the whole file).
                    if response.status_code == 416:
                        print("File already fully downloaded.")
                        break
                        
                    response.raise_for_status()

                    # 4. Open file in APPEND mode ('ab') if resuming, WRITE mode ('wb') if starting fresh
                    mode = 'ab' if downloaded_bytes > 0 and response.status_code == 206 else 'wb'
                    
                    with open(gz_path, mode) as f_out:
                        # Stream chunks safely
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f_out.write(chunk)
                                
                # If we exit the context manager without an exception, the file is complete.
                print("Download phase complete.")
                break 

            except requests.exceptions.ChunkedEncodingError as e:
                # This explicitly catches the IncompleteRead error you experienced
                print(f"Connection dropped mid-stream: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                
            except requests.exceptions.RequestException as e:
                print(f"Network error: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            
        # Unzip the file with gzip and shutil
        print("Attempting to unzip the file...")
        try:
            with gzip.open(path + ".gz", "rb") as f_in:
                with open(path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print("Unzipping successful!")
        except Exception as e:
            print(f"Error occurred while unzipping {path + '.gz'}: {e}")
            return False, f"Error occurred while unzipping {path + '.gz'}: {e}"
        finally:
            os.remove(path + ".gz")  # Clean up the gzip file
            print(f"Cleaned up gzip file: {path + '.gz'}")

        return True, None

# Create the custom iterator
class OPUSIterator(DocumentIterator):
    def __init__(self, log_frequency: int = 1000):
        super().__init__()
        self.log_frequency = log_frequency
    def iterate(self, file_path: str) -> Iterator[dict[str, Any]]:
        # extract year from file name
        filename = os.path.basename(file_path)
        year = filename[1:5]

        with open(file_path, "r", encoding="utf-8") as f:
            for idx, line in enumerate(f):
                if idx % self.log_frequency == 0:
                    print(f"Processing line {idx}...")
                yield {"content": line, "metadata": {"family": "OPUS ES", "year": year, "source_file": file_path}, "id": idx}
    def output_columns(self) -> list[str]:
        return ["content", "metadata", "id"]

# Create the custom download extractor
class OPUSExtractor(DocumentExtractor):
    def __init__(self):
        super().__init__()
    def extract(self, record: dict[str, str]) -> dict[str, Any] | None:
        """Transform raw records to final format."""

        # skip invalid records
        if not record.get("content") or record["content"].strip() == "":
            return None

        # build unique reference from metadata
        meta = record["metadata"]
        document_ref = f"opus-es-{meta['year']}-line-{record['id']}"
        
        # extract and clean text
        cleaned_text = self._clean_text(record["content"])     

        # skip generating unique id since we only have 1 dataset and we can use the line number as the id and we want to be able to reassmble data in order
        return {"content": cleaned_text, "document_ref": document_ref, "metadata": record["metadata"], "line_id": record["id"]}
    
    def input_columns(self) -> list[str]:
        return ["content", "metadata", "id"]
    def output_columns(self) -> list[str]:
        return ["content", "document_ref", "metadata", "line_id"]
    def _clean_text(self, text: str) -> str:
        return text.strip()

# Create the custom data stage
class OPUSDataStage(DocumentDownloadExtractStage):
    def __init__(
        self,
        download_dir: str = "/datasets",
        url_limit: int | None = None,
        record_limit: int | None = None,
        add_filename_column: bool | str = True,
    ):
        self.url_generator = OPUSURLGenerator()
        self.downloader = OPUSDownloader(download_dir=download_dir)
        self.iterator = OPUSIterator()
        self.extractor = OPUSExtractor()

        # now we need to initalize the parent composite stage
        super().__init__(
            url_generator=self.url_generator,
            downloader=self.downloader,
            iterator=self.iterator,
            extractor=self.extractor,
            url_limit=url_limit,
            record_limit=record_limit,
            add_filename_column=add_filename_column,
        )

    @property
    def name(self):
        return "OPUS_data"

    def decompose(self) -> list[ProcessingStage]:
        """Decompose the composite stage into its individual stages."""
        return self.stages
    def get_description(self) -> str:
        return "Download and extract OPUS ES 2018 dataset"

# define the main logic
def main():
    # Initalize Ray Client
    # We need to start a Ray client before downloading the data
    try:
        ray_client = RayClient()
        ray_client.start()
    
        # create the pipeline
        pipeline = Pipeline(
            name = "OPUS ES Download and Extract Pipeline",
            description = "Pipeline to download and extract the OPUS ES dataset",
        )
        # Create custom data loading stage
        data_stage = OPUSDataStage(download_dir="/datasets", url_limit=3, record_limit=10000)
        # Add the stage to the pipeline
        pipeline.add_stage(data_stage)

        # save the results to a parquet file
        pipeline.add_stage(ParquetWriter(path="/datasets/opus_es_downloaded"))

        # run the pipeline
        results = pipeline.run()
    except Exception as e:
        raise RuntimeError(f"An error occurred while running the pipeline: {e}")
    finally:
        # always stop ray client no matter what
        ray_client.stop()

if __name__ == "__main__":
    main()