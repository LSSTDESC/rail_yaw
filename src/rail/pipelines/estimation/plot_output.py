#!/usr/bin/env python3
import pickle
import os


with open(os.path.join("data", "output_summarize.pkl"), "rb") as f:
    ncc = pickle.load(f)
ax = ncc.plot(zero_line=True)
ax.figure.savefig(os.path.join("data", "checkplot.png"))
