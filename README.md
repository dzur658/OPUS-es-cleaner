## OPUS Spanish Cleaner

This repository contains a one click runner to extract monologues from
the Open Subtitles Corpus found [here](https://opus.nlpl.eu/datasets/OpenSubtitles?pair=es&es).

By default the NeMo Curator pipeline will clean the 2013, 2016, and 2018 releases.

## Quick Start

To get started simply run the `run_cleaning.sh` bash script. The script
will set up directories to mount, pull/start the [NeMo Curator docker container 25.09](https://catalog.ngc.nvidia.com/orgs/nvidia/containers/nemo-curator?version=25.09), pull the datasets and convert to parquet, clean the data (minimal), and run MinHash/LSH deduplication. The resulting datast will be dropped in the `output` directory and is uploaded to [HuggingFace here](#).

```bash
./run_cleaning.sh
```

## Intended Use Case
The intended use case of this project is to clean the OPUS datasets, and prepare them for Continued Pretraining (CPT) of a base LLM, specifically to expose the model to 
conversational, informal, and "run on monologues". Due to the nature of the raw OPUS dumps, the resulting dataset consists of monologues, that is specifically where the same
speaker talks for greater than 100 words without being interupted. This approach was taken to provide the model with coherent context and generates a corpus with 357244 words,
with each sample averaging ~241.2 words.

## System Requirements
This project uses [NeMo Curator](https://github.com/NVIDIA-NeMo/Curator). This project works best with a Nvidia GPU, although the download and cleaning files can be ran
without a GPU (only deduplicaton requires a GPU).