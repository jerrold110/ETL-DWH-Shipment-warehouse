#!/bin/bash

echo 'Beginning Extract'
# create directory for extracted data
mkdir -p ../extracted_data

python s3_ec2_extract.py

