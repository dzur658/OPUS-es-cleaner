## OPUS Spanish (Español) Cleaner

This repository contains a one click runner to extract monologues from
the Spanish Open Subtitles Corpus found [here](https://opus.nlpl.eu/datasets/OpenSubtitles?pair=es&es).

By default the NeMo Curator pipeline will clean the 2013, 2016, and 2018 releases.

## Quick Start

To get started simply run the `run_cleaning.sh` bash script. The script
will set up directories to mount, pull/start the [NeMo Curator docker container 25.09](https://catalog.ngc.nvidia.com/orgs/nvidia/containers/nemo-curator?version=25.09), pull the Spanish OPUS datasets and convert to parquet, clean the data (minimal), and run MinHash/LSH fuzzy deduplication. The resulting datast will be dropped in the `output` directory and is uploaded to [HuggingFace here](#).

Execute the following script in the root of the repository.
```bash
./run_cleaning.sh
```

## Intended Use Case
The intended use case of this project is to clean the OPUS datasets, and prepare them for Continued Pretraining (CPT) of a base LLM, specifically to expose the model to 
conversational, informal, and "run on monologues." Due to the nature of the raw OPUS dumps, the resulting dataset consists of monologues where the same
speaker talks for greater than 100 words without being interupted. This approach was taken to provide the model with coherent context and generates a corpus with 357,244 words,
with each document averaging ~241.2 words.

## System Requirements
This project uses [NeMo Curator](https://github.com/NVIDIA-NeMo/Curator). This project works best with a Nvidia GPU, although the download and cleaning files can be ran
without a GPU (only deduplicaton requires a GPU). This project has been tested on an Ubuntu host enviornment running on a single node MSIEdgexpert.

## Repository Structure
- `Dockerfile`: Contains the docker configuration
- `run_cleaning.sh`: Main orchestrator script that sets up mount directories, pulls the docker image for NeMo Curator, builds the container, and runs the container with the appropriate directory mounts.
- `./src`: This directory is mounted in the container and has all the scripts/pipelines the container needs to perform the download/extract, cleaning, and deduplication tasks.
- `./src/orchestrator.sh`: The main bash script that runs in the container. The script runs only files that are necessary to run. Therefore, if you make modifications to say the cleaning pipeline, and already have the data stored in `./data` the script will intelligently skip redownloading your datasets. The order the orchestrator runs files in is as follows:

1) `./src/download_opus_data.py`
2) `./src/download_fasttext_model.sh`
3) `./src/clean_opus_data.py`
4) `./src/deduplication_opus.py`

- `./src/download_opus_data.py`: The pipeline that fetches the OPUS datasets for 2013, 2016, and 2018 from [here](https://opus.nlpl.eu/datasets/OpenSubtitles?pair=es&es). The pipeline performs downloading, and extracting uninterrupted speaker utterances from the raw text dump. Speaker turnovers in the raw subtitles dump are marked by `-`. NOTE: in order to accomplish this we load each corpus into RAM in full to gurantee accurate splitting, the three corpuses range in size from ~2gb to ~8gb.
- `./src/download_fasttext_model.sh`: This downloads the uncompressed fasttext model for language identification, but this can be swapped to the compressed version. Each of the models can be found [here](https://fasttext.cc/docs/en/language-identification.html). The uncompressed version we pull is 126MB in size and the compressed version can be dropped in with relatively minimal changes which is only 917kB. Refer to the comments in the script for instructions on how to switch to the compressed model.
- `./src/clean_opus_data.py`: Performs minimal cleaning on the OPUS data. It includes regex checks for stripping out square brackets, timestamps, embedded timestamps, simple HTML element cleaning, and whitespace normalization. The pipeline retains documents that are over 100 words (monologues) and only documents that pass a 0.7 language threshold (since the dataset is overwhelmingly comprised of Spanish this works to ensure our monologues are actual Spanish).
- `./src/deduplication_opus.py`: Performs fuzzy deduplication on the remaining monologue documents. This is the final step in the pipeline and dumps the resulting dataset to `./output/opus_es_final/`. NOTE: It is important that the `input_blocksize` parameters for BOTH `FuzzyDeduplicationWorkflow` and `TextDuplicatesRemovalWorkflow` are exactly the same due to the auto id generator method employed. If this is not followed you will receive a [UUID Mismatch Error](https://github.com/NVIDIA-NeMo/Curator/issues/2092). The `input_blocksize` parameter is set as 2GiB, adjust for your specific VRAM needs.

#### Other Mounted Directories
- `./data`: Created by `./run_cleaning.sh` if not present, is mounted and stores all the intermediary steps of the pipeline. Includes the raw `.txt` corpus, result of the download/extract stage, result of the cleaning stage, and auto id generator configs for fuzzy deduplication.
- `./output`: Stores the final parquet dataset of monologues.
- `./models`: Stores the fasttext model weights.

#### `./experiments`
Stores experiments ran during the development of the repository. Methods attempted here were not implemented into the final pipeline, but could be of some value for different extraction tasks.

## Acknowledgements
Raw data is proveded by the [OpenSubtitles Project](http://www.opensubtitles.org/), thank you!