#!/usr/bin/env python
#
# This script produces a pipeline file akin to the yet_another_wizz example notebook
#

# pylint: skip-file
import os

from yaw import UniformRandoms

from rail.core.stage import RailStage
import rail.stages

rail.stages.import_and_attach_all()
from rail.stages import *
from rail.yaw_rail.hotfixes import FixedRailPipeline as RailPipeline
from rail.yaw_rail.utils import get_dc2_test_data

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


def create_datasets(root):
    test_data = get_dc2_test_data()
    redshifts = test_data["z"].to_numpy()
    n_data = len(test_data)

    data_name = "input_data.parquet"
    data_path = os.path.join(root, data_name)
    test_data.to_parquet(data_path)

    angular_rng = UniformRandoms(
        test_data["ra"].min(),
        test_data["ra"].max(),
        test_data["dec"].min(),
        test_data["dec"].max(),
        seed=12345,
    )
    test_rand = angular_rng.generate(n_data * 10, draw_from=dict(z=redshifts))

    rand_name = "input_rand.parquet"
    rand_path = os.path.join(root, rand_name)
    test_rand.to_parquet(rand_path)

    return (data_path, rand_path)


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
            connections=dict(
                patch_source=self.cache_ref.io.cache,
            ),
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
                cross_corr=self.crosscorr.io.cross_corr,
                auto_corr_ref=self.autocorr.io.auto_corr,
            ),
            verbose=VERBOSE,
        )


if __name__ == "__main__":
    if not os.path.exists(ROOT):
        os.makedirs(ROOT)
    data_path, rand_path = create_datasets(ROOT)

    pipe = YawPipeline()
    pipe.initialize(
        overall_inputs=dict(
            data_ref=data_path,
            rand_ref=rand_path,
            data_unk=data_path,
            rand_unk="/dev/null",
            patch_source_ref="/dev/null",
            auto_corr_unk="/dev/null",
        ),
        run_config=dict(output_dir=ROOT, log_dir=ROOT, resume=False),
        stages_config=None,
    )
    pipe.save(os.path.join(ROOT, "yaw_pipeline.yml"), site_name="local")
