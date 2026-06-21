#!/bin/bash

# Enable nullglob (unmatched globs resolve to empty) and dotglob (include hidden files)
shopt -s nullglob dotglob
files=(/datasets/*)
shopt -u nullglob dotglob # Revert settings so they don't affect the rest of your script

# check if OPUS datasets are already downloaded, if not, download them
if [ -d "/datasets" ] && [ ${#files[@]} -gt 0 ]; then
    echo "OPUS datasets already downloaded, skipping download step."
else
    echo "OPUS datasets not found, downloading..."
    python src/download_opus_data.py
fi

# check if fasttext model is already downloaded, if not, download it
if test -e "/models/lid.176.bin"; then
    echo "FastText language identification model already downloaded, skipping download step."
elif test -e "/models/lid.176.ftz"; then
    echo "Compressed FastText language identification model already downloaded, skipping download step."
    echo "WARNING: Make sure to update `clean_opus_data.py` to point at the compressed model if using the .ftz version."
else
    echo "FastText language identification model not found, downloading..."
    ./src/download_fasttext_model.sh
fi

# run cleaning script to clean the OPUS datasets
python src/clean_opus_data.py