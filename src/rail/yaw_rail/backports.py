from __future__ import annotations

import sys
import yaml

from ceci import __version__ as ceci_ver_str
from rail.core.stage import RailPipeline


ceci_ver_main, ceci_ver_sub = [int(v) for v in ceci_ver_str.split(".")[:2]]

if ceci_ver_main >= 2:
    FixedRailPipeline = RailPipeline

else:

    class FixedRailPipeline(RailPipeline):
        def save(
            self,
            pipefile: str,
            stagefile: str | None = None,
            reduce_config: bool = False,
            # added keyword arguments
            site_name: str = "local",
            **kwargs,  # pylint: disable=W0613
        ) -> None:
            super().save(
                pipefile=pipefile,
                stagefile=stagefile,
                reduce_config=reduce_config,
            )

            # need to rewrite the configuration and add site/name key
            with open(pipefile, "r") as f:
                pipe_dict = yaml.load(f, yaml.SafeLoader)
                pipe_dict["site"]["name"] = site_name
            with open(pipefile, "w") as f:
                yaml.dump(pipe_dict, f)
            sys.stderr.write(f"INFO: set site/name config to '{site_name}'\n")
