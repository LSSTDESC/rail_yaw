![yet_another_wizz](https://raw.githubusercontent.com/jlvdb/yet_another_wizz/main/docs/source/_static/logo-dark.png)

[![PyPI](https://img.shields.io/pypi/v/yaw_rail?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/yaw_rail/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/LSSTDESC/rail_yaw/smoke-test.yml)](https://github.com/LSSTDESC/rail_yaw/actions/workflows/smoke-test.yml)
[![codecov](https://codecov.io/gh/LSSTDESC/rail_yaw/graph/badge.svg?token=BsmWz2v0qL)](https://codecov.io/gh/LSSTDESC/rail_yaw)
[![Template](https://img.shields.io/badge/Template-LINCC%20Frameworks%20Python%20Project%20Template-brightgreen)](https://lincc-ppt.readthedocs.io/en/latest/)

# pz-rail-yaw

This is a wrapper for [RAIL](https://github.com/LSSTDESC/RAIL) (see below) to
integrate the clustering redshift code *yet_another_wizz*:

- code: https://github.com/jlvdb/yet_another_wizz.git
- docs: https://yet-another-wizz.readthedocs.io/
- PyPI: https://pypi.org/project/yet_another_wizz/
- Docker: https://hub.docker.com/r/jlvdb/yet_another_wizz/


## About this wrapper

The current wrapper implements most of the functionality of *yet_another_wizz*,
which is an external dependency for this package. The wrapper currently
implements five different stages and three custom data handles:

- A cache directory, which stores a data set and its corresponding random
  points. Both catalogs are split into spatial patches which are used for the
  covariance estimation. The cache directory is created and destroyed with two
  dedicated stages.
- A handle for *yet_another_wizz* pair count data (stored as HDF5 file), which
  are created as outputs of the cross- and autocorrelation stages.
- A handle for *yet_another_wizz* clustering redshift estimates (stored as
  python pickle file), which is created by the final estimator summary stage.

A jupyter notebook containing a full example with more detailed descriptions is
included in

    examples/full_example.ipynb

### Note

The summary stage produces a `qp.Ensemble`, but does so by simply setting all
negative correlation amplitudes in all generated (spatial) samples to zero.
This needs refinement in a future release. For now it is advised to use the
second output of the summary stage, which is the raw clutering redshift estimate
from *yet_another_wizz* (`yaw.RedshiftData`).

![rail_yaw_network](https://raw.githubusercontent.com/LSSTDESC/rail_yaw/main/examples/rail_yaw_network.png)

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
