#!/usr/bin/env bash
#
# This scripts runs the pipeline produced by build_pipeline.py and plots the
# output which can be found in data/
#

set -e

time ceci yaw_pipeline.yml
python plot_output.py
echo "inspect outputs in $(pwd)/data"
