#!/usr/bin/env python
#
# This script produces a pipeline file for the yet_another_wizz example notebook
#

# pylint: skip-file
import os

from rail.core.stage import RailStage, RailPipeline
import rail.stages

rail.stages.import_and_attach_all()
from rail.stages import *

try:  # TODO: remove when integrated in RAIL
    YawCacheCreate
except NameError:
    from rail.estimation.algos.cc_yaw import *


ROOT = "run"
VERBOSE = "debug"  # verbosity level of built-in logger, disable with "error"

# configuration for the correlation measurements
corr_config = dict(
    rmin=100,
    rmax=1000,
    zmin=0.0,
    zmax=3.0,
    zbin_num=8,
    verbose=VERBOSE,
)


class YawPipeline(RailPipeline):

    def __init__(self):
        RailPipeline.__init__(self)

        DS = RailStage.data_store
        DS.__class__.allow_overwrite = True

        self.cache_ref = YawCacheCreate.build(
            aliases=create_yaw_cache_alias("ref"),
            path=os.path.join(ROOT, "test_ref"),
            overwrite=True,
            ra_name="ra",
            dec_name="dec",
            redshift_name="z",
            n_patches=5,
            verbose=VERBOSE,
        )

        self.cache_unk = YawCacheCreate.build(
            aliases=create_yaw_cache_alias("unk"),
            path=os.path.join(ROOT, "test_unk"),
            overwrite=True,
            ra_name="ra",
            dec_name="dec",
            patches=os.path.join(ROOT, "test_ref"),
            verbose=VERBOSE,
        )

        self.autocorr = YawAutoCorrelate.build(
            connections=dict(
                sample=self.cache_ref.io.cache,
            ),
            **corr_config,
        )

        self.crosscorr = YawCrossCorrelate.build(
            connections=dict(
                reference=self.cache_ref.io.cache,
                unknown=self.cache_unk.io.cache,
            ),
            **corr_config,
        )

        self.estimate = YawSummarize.build(
            connections=dict(
                cross_corr=self.crosscorr.io.crosscorr,
                auto_corr_ref=self.autocorr.io.autocorr,
            ),
            verbose=VERBOSE,
        )


if __name__ == "__main__":
    pipe = YawPipeline()
    pipe.initialize(
        dict(
            data_ref="dummy.in",
            rand_ref="dummy.in",
            data_unk="dummy.in",
            rand_unk="/dev/null",
            auto_corr_unk="/dev/null",
        ),
        dict(output_dir=ROOT, log_dir=ROOT, resume=False),
        None,
    )
    pipe.save(os.path.join(ROOT, "yaw_pipeline.yml"))
