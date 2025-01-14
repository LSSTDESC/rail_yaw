![yet_another_wizz](https://raw.githubusercontent.com/jlvdb/yet_another_wizz/main/docs/source/_static/logo-dark.png)

[![PyPI](https://img.shields.io/pypi/v/pz-rail-yaw?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/pz-rail-yaw/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/LSSTDESC/rail_yaw/smoke-test.yml)](https://github.com/LSSTDESC/rail_yaw/actions/workflows/smoke-test.yml)
[![codecov](https://codecov.io/gh/LSSTDESC/rail_yaw/graph/badge.svg?token=BsmWz2v0qL)](https://codecov.io/gh/LSSTDESC/rail_yaw)
[![Template](https://img.shields.io/badge/Template-LINCC%20Frameworks%20Python%20Project%20Template-brightgreen)](https://lincc-ppt.readthedocs.io/en/latest/)

# pz-rail-yaw

This is a wrapper to integrate the clustering redshift code *yet_another_wizz*
(YAW) into [RAIL](https://github.com/LSSTDESC/RAIL):

- code: https://github.com/jlvdb/yet_another_wizz.git
- docs: https://yet-another-wizz.readthedocs.io/
- PyPI: https://pypi.org/project/yet_another_wizz/
- Docker: https://hub.docker.com/r/jlvdb/yet_another_wizz/

**Original publication:** https://arxiv.org/abs/2007.01846


## Installation

This package can be either installed with the RAIL base package *(recommended)*
or explicitly with

    pip install pz-rail-yaw


## About this wrapper

The wrapper closely resembles the structure and functionality of YAW by
implementing four different RAIL stages:

- *YawCacheCreate*, which implements the spatial patches of YAW data catalogues,
- *YawAutoCorrelate*/*YawCrossCorrelate*, which implement the expensive pair
  counting of the correlation measurements, and
- *YawSummarize*, which transforms the pair counts to a redshift estimate with
  an optional mitigation for galaxy sample bias.

The repository includes an extensive example notebook

    examples/full_example.ipynb

with further documentation and an example `ceci` pipeline

    src/rail/pipelines/estimation/yaw_pipeline.yml

for procesing large and/or more complex data sets.

![rail_yaw_network](https://raw.githubusercontent.com/LSSTDESC/rail/main/examples/estimation_examples/rail_yaw_network.png)

## RAIL: Redshift Assessment Infrastructure Layers

This package is part of the larger ecosystem of Photometric Redshifts
in [RAIL](https://github.com/LSSTDESC/RAIL).

### Citing RAIL

This code, while public on GitHub, has not yet been released by DESC and is
still under active development. Our release of v1.0 will be accompanied by a
journal paper describing the development and validation of RAIL.

If you make use of the ideas or software in RAIL, please cite the repository 
<https://github.com/LSSTDESC/RAIL>. You are welcome to re-use the code, which
is open source and available under terms consistent with the MIT license.

External contributors and DESC members wishing to use RAIL for non-DESC projects
should consult with the Photometric Redshifts (PZ) Working Group conveners,
ideally before the work has started, but definitely before any publication or 
posting of the work to the arXiv.

### Citing this package

If you use this package, you should also cite the appropriate papers for each
code used.  A list of such codes is included in the 
[Citing RAIL](https://rail-hub.readthedocs.io/en/latest/source/citing.html)
section of the main RAIL Read The Docs page.
