#!/bin/bash

# Assumes Docker and appropriate Nvidia CUDA drivers are already present on the HOST machine!

# pull docker image
docker pull nvcr.io/nvidia/nemo-curator:25.09

# build docker image
docker build -t opus-es-cleaner:latest .

# Run cleaning
# Downloads and cleans OPUS 2018 ES (monolingual) by default
docker run --gpus all --rm \
    --shm-size=1g \
    -v $HOME/datasets:/data:ro \          # (optional) if you prefer to mount your own data
    -v $HOME/opus_output:/output \
    -w /workspace \
    opus-es-cleaner:latest \
    python src/curator_opus_es.py \
        /output