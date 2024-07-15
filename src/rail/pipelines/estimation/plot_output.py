#!/usr/bin/env python3
import pickle
import os


with open(os.path.join("data", "output_summarize.pkl"), "rb") as f:
    ncc = pickle.load(f)
ncc.plot(zero_line=True).figure.savefig(os.path.join("data", "checkplot.png"))
