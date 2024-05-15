"""
The current wrapper implements the basic functionality of yet_another_wizz,
which is an external dependency for this package. Additional (unit) tests are
required to verify full functionality.

The wrapper currently implements five different stages and three custom data
handles:

- A cache directory, of which each stores a data set and its corresponding
  random points. Both catalogs are split into spatial patches which are used for
  covariance estimation. The cache directory is created and destroyed with two
  dedicated stages.
- A handle for yet_another_wizz pair count data (stored as HDF5 file), which are
  created as outputs of the cross- and autocorrelation stages.
- A handle for yet_another_wizz clustering redshift estimates (stored as python
  pickle), which is created by the final estimator stage.

The final stage does produce a qp ensemble as expected, but does so by setting
all negative correlation amplitudes in all generated (spatial) samples to zero.
This needs refinement in a future release, for now it is advised to use the raw
clutering redshift estimate from yet_another_wizz.
"""

from .cache import YawCacheCreate, YawCacheDrop
from .correlation import YawAutoCorrelate, YawCrossCorrelate
from .summarize import YawSummarize
from .utils import get_dc2_test_data

__all__ = [
    "YawAutoCorrelate",
    "YawCacheCreate",
    "YawCacheDrop",
    "YawCrossCorrelate",
    "YawSummarize",
]
