#!/usr/bin/env python3
#
# This script produces a pipeline file that demonstrates the clustering redshift
# estimate obtained from the correlation amplitudes measured in multiple radial
# bins, fitted against a model for the angular matter correlation function.
#
# It measures the correlations of two reference tracers that cover different,
# partially overlapping redshift ranges, fits their amplitudes and combines them
# into a single redshift estimate.
#

# coverage is excluded since the code is run in an external interpreter
# pylint: skip-file
import argparse
import os
import sys
from shutil import rmtree

import numpy as np
import rail.stages
from rail.core.stage import RailPipeline
from yaw.randoms import BoxRandoms

rail.stages.import_and_attach_all()
from rail.stages import *

# `import_and_attach_all` only attaches the stage classes, not the helpers that
# build the alias mappings or the tracer-specific summarizer
from rail.estimation.algos.cc_yaw import *

from rail.yaw_rail.utils import get_dc2_test_data

VERBOSE = "debug"  # verbosity level of built-in logger, disable with "error"

parser = argparse.ArgumentParser(
    description="Build the rail_yaw clustering redshift example pipeline."
)
parser.add_argument("--root", default=".")

# The reference tracers cover different redshift ranges, but share their binning
# where the ranges overlap, which the inverse-variance combination requires.
TRACERS = ("lowz", "highz")
TRACER_ZLIM = dict(lowz=(0.2, 1.2), highz=(0.8, 1.8))
ZBIN_WIDTH = 0.2

# the correlation amplitude is measured in five logarithmic radial bins
SCALE_EDGES = np.logspace(np.log10(100.0), np.log10(3000.0), 6)

corr_config = dict(
    rmin=SCALE_EDGES[:-1].tolist(),
    rmax=SCALE_EDGES[1:].tolist(),
    unit="kpc",
    verbose=VERBOSE,
)

# with only five patches the jackknife covariance across five radial bins cannot
# be inverted, so the amplitude is fitted with diagonal weighted least squares
fit_config = dict(
    # plain floats, since the stage config is serialised to YAML
    fit_rmin=float(SCALE_EDGES[0]),
    fit_rmax=float(SCALE_EDGES[-1]),
    auto_fit_rmin=float(SCALE_EDGES[0]),
    auto_fit_rmax=float(SCALE_EDGES[-1]),
    use_jk_cov=False,
    verbose=VERBOSE,
)

# The summarizer declares one input per tracer, so its class depends on the
# tracers being combined. It must be created at the top level of this module:
# `ceci` runs every stage in a fresh process, where it recreates the class by
# importing the module that defines it.
#
# Executing this file as a script puts it in the "__main__" module, which `ceci`
# cannot import, so it is registered under its importable name as well. When the
# module is imported normally this is a no-op.
MODULE = "rail.pipelines.estimation.build_pipeline_nz"
sys.modules.setdefault(MODULE, sys.modules[__name__])

YawNzCombineTracers = create_yaw_nz_combine(TRACERS, module=MODULE)


def zbins(tracer):
    zmin, zmax = TRACER_ZLIM[tracer]
    return dict(zmin=zmin, zmax=zmax, num_bins=round((zmax - zmin) / ZBIN_WIDTH))


def create_datasets(root):  # pragma: no cover
    test_data = get_dc2_test_data()
    redshifts = test_data["z"].to_numpy()

    data_path = os.path.join(root, "input_data.parquet")
    test_data.to_parquet(data_path)

    generator = BoxRandoms(
        test_data["ra"].min(),
        test_data["ra"].max(),
        test_data["dec"].min(),
        test_data["dec"].max(),
        redshifts=redshifts,
        seed=12345,
    )
    test_rand = generator.generate_dataframe(len(test_data) * 10)
    test_rand.rename(columns=dict(redshifts="z"), inplace=True)

    rand_path = os.path.join(root, "input_rand.parquet")
    test_rand.to_parquet(rand_path)

    return data_path, rand_path


class YawNzPipeline(RailPipeline):  # pragma: no cover

    def __init__(self, data_dir):
        super().__init__()

        # the reference cache defines the patch centers that all other caches
        # must adopt, so that the jackknife samples are consistent
        self.cache_ref = YawCacheCreate.build(
            aliases=create_yaw_cache_alias("ref"),
            path=os.path.join(data_dir, "cache_ref"),
            overwrite=True,
            ra_name="ra",
            dec_name="dec",
            redshift_name="z",
            patch_num=5,
            verbose=VERBOSE,
        )

        # `patch_source` is wired to the reference cache, so it must not be
        # aliased: `RailStage.set_data` gives an existing alias precedence over
        # the tag of the connected stage's output
        unk_aliases = create_yaw_cache_alias("unk")
        del unk_aliases["patch_source"]

        self.cache_unk = YawCacheCreate.build(
            connections=dict(patch_source=self.cache_ref.io.output),
            aliases=unk_aliases,
            path=os.path.join(data_dir, "cache_unk"),
            overwrite=True,
            ra_name="ra",
            dec_name="dec",
            verbose=VERBOSE,
        )

        for tracer in TRACERS:
            auto = YawAutoCorrelate.build(
                connections=dict(sample=self.cache_ref.io.output),
                **corr_config,
                **zbins(tracer),
            )
            setattr(self, f"auto_corr_{tracer}", auto)

            cross = YawCrossCorrelate.build(
                connections=dict(
                    reference=self.cache_ref.io.output,
                    unknown=self.cache_unk.io.output,
                ),
                **corr_config,
                **zbins(tracer),
            )
            setattr(self, f"cross_corr_{tracer}", cross)

            fit = YawFitAmplitude.build(
                connections=dict(
                    cross_corr=getattr(self, f"cross_corr_{tracer}").io.output,
                    auto_corr_ref=getattr(self, f"auto_corr_{tracer}").io.output,
                ),
                **fit_config,
            )
            setattr(self, f"fit_{tracer}", fit)

        self.combine = YawNzCombineTracers.build(
            connections={
                f"fit_{tracer}": getattr(self, f"fit_{tracer}").io.output
                for tracer in TRACERS
            },
            bias_mode="growth_factor",
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

    pipe = YawNzPipeline(data_dir)
    pipe.initialize(
        overall_inputs=dict(
            data_ref=data_path,
            rand_ref=rand_path,
            data_unk=data_path,
            rand_unk="none",
            patch_source_ref="none",
        ),
        run_config=dict(output_dir=data_dir, log_dir=log_dir, resume=False),
        stages_config=None,
    )
    pipe.save(os.path.join(root, "yaw_nz_pipeline.yml"), site_name="local")
