from __future__ import annotations

from ceci import __version__ as ceci_version
from rail.core.stage import RailPipeline


class FixedRailPipeline(RailPipeline):
    def save(
        self,
        pipefile: str,
        stagefile: str | None = None,
        reduce_config: bool = False,
        site_name: str | None = None,
        **kwargs,
    ) -> None:
        need_sitename_fix = ceci_version[0] in "01"  # this will be fixed in ceci v2

        allargs = dict(
            pipefile=pipefile,
            stagefile=stagefile,
            reduce_config=reduce_config,
            **kwargs,
        )
        if not need_sitename_fix:
            allargs["site_name"] = site_name
        super().save(**allargs)

        if need_sitename_fix:
            import sys  # pylint: disable=C0415
            import yaml  # pylint: disable=C0415

            with open(pipefile, "r") as f:
                pipe_dict = yaml.load(f, yaml.SafeLoader)

            site_name = kwargs.get("site_name", "local")
            sys.stderr.write(f"INFO: fixing site as '{site_name}'\n")
            pipe_dict["site"]["name"] = site_name

            with open(pipefile, "w") as f:
                yaml.dump(pipe_dict, f)
