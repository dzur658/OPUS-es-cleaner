#!/bin/bash

# Assumes Docker and appropriate Nvidia CUDA drivers are already present on the HOST machine!

# before running container initialize mount directories in the repo if not already present
if [ ! -d "./data" ]; then
    mkdir ./data
fi

if [ ! -d "./output" ]; then
    mkdir ./output
fi

if [ ! -d "./models" ]; then
    mkdir ./models
fi

# pull docker image
docker pull nvcr.io/nvidia/nemo-curator:25.09

# build docker image
docker build -t opus-es-cleaner:latest .

# Run cleaning
# Downloads and cleans OPUS 2018 ES (monolingual) by default
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