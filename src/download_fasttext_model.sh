#!/bin/bash

# NOTE: Can be modified to use the compressed version of the model if desired
# Change the URL to point at the compressed model which will be a .ftz file
# Make sure to change the filename in the mv command AND `clean_opus_data.py` to point at the compressed model as well
# https://fasttext.cc/docs/en/language-identification.html

wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin

mv lid.176.bin /models