#!/usr/bin/env python3
#
# This script produces a pipeline file akin to the yet_another_wizz example
# notebook as well as the input test data required to run the pipeline.
#

# coverage is excluded since the code is run in an external interpreter
# pylint: skip-file
import argparse
import os
from shutil import rmtree

import pandas as pd
from yaw.randoms import BoxRandoms

import rail.stages
from rail.core.stage import RailPipeline, RailStage

rail.stages.import_and_attach_all()
from rail.stages import *

from rail.yaw_rail.utils import get_dc2_test_data

try:  # TODO: remove when integrated in RAIL
    YawCacheCreate
except NameError:
    from rail.estimation.algos.cc_yaw import *


VERBOSE = "debug"  # verbosity level of built-in logger, disable with "error"

parser = argparse.ArgumentParser(
    description="Generate test data and build the rail_yaw ceci example pipeline."
)
parser.add_argument("--root", default=".")

# configuration for the correlation measurements
corr_config = dict(
    rmin=100,
    rmax=1000,
    zmin=0.2,
    zmax=1.8,
    num_bins=8,
    verbose=VERBOSE,
)


def create_datasets(root):  # pragma: no cover
    test_data = get_dc2_test_data()
    redshifts = test_data["z"].to_numpy()
    n_data = len(test_data)

    data_name = "input_data.parquet"
    data_path = os.path.join(root, data_name)
    test_data.to_parquet(data_path)

    generator = BoxRandoms(
        test_data["ra"].min(),
        test_data["ra"].max(),
        test_data["dec"].min(),
        test_data["dec"].max(),
        redshifts=redshifts,
        seed=12345,
    )
    test_rand = generator.generate_dataframe(n_data * 10)
    test_rand.rename(columns=dict(redshifts="z"), inplace=True)

    rand_name = "input_rand.parquet"
    rand_path = os.path.join(root, rand_name)
    test_rand.to_parquet(rand_path)

    return (data_path, rand_path)


class YawPipeline(RailPipeline):  # pragma: no cover

    def __init__(self, data_dir):
        super().__init__()

        DS = RailStage.data_store
        DS.__class__.allow_overwrite = True

        self.cache_ref = YawCacheCreate.build(
            aliases=create_yaw_cache_alias("ref"),
            path=os.path.join(data_dir, "test_ref"),
            overwrite=True,
            ra_name="ra",
            dec_name="dec",
            redshift_name="z",
            patch_num=5,
            verbose=VERBOSE,
        )

        self.cache_unk = YawCacheCreate.build(
            connections=dict(
                patch_source=self.cache_ref.io.output,
            ),
            aliases=create_yaw_cache_alias("unk"),
            path=os.path.join(data_dir, "test_unk"),
            overwrite=True,
            ra_name="ra",
            dec_name="dec",
            verbose=VERBOSE,
        )

        self.auto_corr = YawAutoCorrelate.build(
            connections=dict(
                sample=self.cache_ref.io.output,
            ),
            **corr_config,
        )

        self.cross_corr = YawCrossCorrelate.build(
            connections=dict(
                reference=self.cache_ref.io.output,
                unknown=self.cache_unk.io.output,
            ),
            **corr_config,
        )

        self.summarize = YawSummarize.build(
            connections=dict(
                cross_corr=self.cross_corr.io.output,
                auto_corr_ref=self.auto_corr.io.output,
            ),
            verbose=VERBOSE,
        )


if __name__ == "__main__":  # pragma: no cover
    root = parser.parse_args().root
    print(f"setting working directory: {root}")
    if not os.path.exists(root):
        os.mkdir(root)

    data_dir = os.path.join(root, "data")
    log_dir = os.path.join(root, "logs")
    for folder in (data_dir, log_dir):
        if os.path.exists(folder):
            rmtree(folder)
        os.mkdir(folder)

    data_path, rand_path = create_datasets(data_dir)

    pipe = YawPipeline(data_dir)
    pipe.initialize(
        overall_inputs=dict(
            data_ref=data_path,
            rand_ref=rand_path,
            data_unk=data_path,
            rand_unk="none",
            patch_source_ref="none",
            auto_corr_unk="none",
        ),
        run_config=dict(output_dir=data_dir, log_dir=log_dir, resume=False),
        stages_config=None,
    )
    pipe.save(os.path.join(root, "yaw_pipeline.yml"), site_name="local")
