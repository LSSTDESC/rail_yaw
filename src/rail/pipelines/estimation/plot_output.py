#!/usr/bin/env python3
#
# This scripts plots the redshift estimate produced by the example pipeline.
# Automatically run by run_pipeline.sh
#

# pylint: skip-file
import pickle
import os


with open(os.path.join("data", "output_summarize.pkl"), "rb") as f:
    ncc = pickle.load(f)
    ncc.to_files(os.path.join("data", "output"))
ax = ncc.plot(zero_line=True)
ax.set_xlabel("Redshift")
ax.figure.savefig(os.path.join("data", "checkplot.png"))
