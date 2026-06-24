#!/bin/bash
set -euo pipefail

# Assumes Docker and appropriate Nvidia CUDA drivers are already present on the HOST machine!

# before running container initialize mount directories in the repo if not already present, fail on error
mkdir -p ./data>&2
mkdir -p ./output>&2
mkdir -p ./models>&2

# pull docker image
docker pull nvcr.io/nvidia/nemo-curator:25.09

# build docker image
docker build -t opus-es-cleaner:latest .

# Run cleaning
# Downloads and cleans OPUS 2013, 2016, and 2018 ES (monolingual) by default
docker run --gpus all --rm \
    --ipc=host \
    --pid=host \
    --network=host \
    --mount type=bind,source=./data,target=/datasets \
    --mount type=bind,source=./output,target=/opus_output \
    --mount type=bind,source=./models,target=/models \
    -w /workspace \
    opus-es-cleaner:latest \
    ./src/orchestrator.sh