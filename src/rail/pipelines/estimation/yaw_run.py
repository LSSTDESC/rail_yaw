#!/usr/bin/env python
#
# This script runs the pipeline file produced by `yaw_pipeline.py`
#
import os

from yaw import UniformRandoms

import ceci
from rail.core.data import TableHandle
from rail.core.stage import RailStage
from rail.yaw_rail.utils import get_dc2_test_data


ROOT = "run"


if __name__ == "__main__":
    if not os.path.exists(ROOT):
        os.mkdir(ROOT)

    DS = RailStage.data_store
    DS.__class__.allow_overwrite = True

    test_data = get_dc2_test_data()
    redshifts = test_data["z"].to_numpy()
    zmin = redshifts.min()
    zmax = redshifts.max()
    n_data = len(test_data)

    path = os.path.join(ROOT, "input_data.parquet")
    test_data.to_parquet(path)
    test_data_handle = DS.add_data(
        "input_data",
        data=test_data,
        handle_class=TableHandle,
        path=path,
    )

    angular_rng = UniformRandoms(
        test_data["ra"].min(),
        test_data["ra"].max(),
        test_data["dec"].min(),
        test_data["dec"].max(),
        seed=12345,
    )
    test_rand = angular_rng.generate(n_data * 10, draw_from=dict(z=redshifts))

    path = os.path.join(ROOT, "input_rand.parquet")
    test_rand.to_parquet(path)
    test_rand_handle = DS.add_data(
        "input_rand",
        data=test_rand,
        handle_class=TableHandle,
        path=path,
    )

    pipe = ceci.Pipeline.read(os.path.join(ROOT, "yaw_pipeline.yml"))
    pipe.run()
