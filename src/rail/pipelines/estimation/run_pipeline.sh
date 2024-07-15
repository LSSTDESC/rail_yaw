#!/usr/bin/env bash
set -e

time ceci yaw_pipeline.yml
python plot_output.py
echo "inspect outputs in $(pwd)/data"
