#!/usr/bin/env python
# coding: utf-8

# Prerquisites, os, and numpy
import os
import numpy as np

# Various rail modules
import rail.stages
rail.stages.import_and_attach_all()
from rail.stages import *

from rail.core.stage import RailStage, RailPipeline
import ceci
from rail.yaw_rail.cache import stage_helper  # utility for YawCacheCreate



VERBOSE = "debug"  # verbosity level of built-in logger, disable with "error"

corr_config = dict(
    rmin=100,   # in kpc
    rmax=1000,  # in kpc
    # rweight=None,
    # rbin_num=50,
    zmin=0.,
    zmax=3.,
    zbin_num=8,  # default: 30
    # method="linear",
    # zbins=np.linspace(zmin, zmax, zbin_num+1)
    # crosspatch=True,
    # thread_num=None,
    verbose=VERBOSE,  # default: "info"
)


class YawPipeline(RailPipeline):

    def __init__(self):
        RailPipeline.__init__(self)

        DS = RailStage.data_store
        DS.__class__.allow_overwrite = True

        self.ref = YawCacheCreate.build(
            aliases=stage_helper("ref"),
            path="./test_ref",
            overwrite=True,  # default: False
            ra_name="ra",
            dec_name="dec",
            redshift_name="z",
            n_patches=5,
            verbose=VERBOSE,  # default: "info"
        )
        
        self.unk = YawCacheCreate.build(
            aliases=stage_helper("unk"),
            path="./test_unk",
            overwrite=True,  # default: False
            ra_name="ra",
            dec_name="dec",
            # redshift_name=,
            # weight_name=,
            patches="./test_ref",
            # patch_name=,
            # n_patches=,
            verbose=VERBOSE,  # default: "info"
        )

        self.corr_ss = YawAutoCorrelate.build(
            connections=dict(
                sample=self.ref.io.cache,
            ),
            **corr_config
        )
        
        self.corr_sp = YawCrossCorrelate.build(
            connections=dict(
                reference=self.ref.io.cache,
                unknown=self.unk.io.cache,
            ),
            **corr_config
        )

        self.estimate = YawSummarize.build(
            connections=dict(
                cross_corr=self.corr_sp.io.crosscorr,
                ref_corr=self.corr_ss.io.autocorr,
            ),
            verbose=VERBOSE,
        )
        
        self.drop_ref = YawCacheDrop.build(
            connections=dict(
                cache=self.ref.io.cache,
            ),
            verbose=VERBOSE,           
        )

        self.drop_unk = YawCacheDrop.build(
            connections=dict(
                cache=self.unk.io.cache,                
            ),
            verbose=VERBOSE,           
        )

        

if __name__ == '__main__':    
    pipe = YawPipeline()
    pipe.initialize(
        dict(
            data_ref="dummy.in",
            rand_ref="dummy.in",
            data_unk="dummy.in",
            rand_unk="/dev/null",
            unk_corr="/dev/null",
        ),
        dict(
            output_dir='.',
            log_dir='.',
            resume=False
        ),
        None,
    )
    pipe.save('tmp_yaw_pipeline.yml')
