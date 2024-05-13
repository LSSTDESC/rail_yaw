"""
This file implements a utility functions that automatically create
`ceci.config.StageParameter` instances from their corresponding parameter in
*yet_another_wizz* by hooking into its integrated help system. Additionally,
it provides a decorator for RAIL stages that automatically populates the
stage configuration and building class doc-string.
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Container
from copy import copy
from dataclasses import fields
from typing import TYPE_CHECKING, Any, Literal

from yaw import config

from ceci.config import StageParameter
from rail.core.stage import RailStage
from rail.yaw_rail.logging import config_verbose

if TYPE_CHECKING:  # pragma: no cover
    from rail.core.data import DataHandle

__all__ = [
    "YawRailStage",
    "create_param",
]


def get_yaw_config_meta(config_cls: Any, parname: str) -> dict[str, Any]:
    """Convert parameter metadata, embedded into the yet_another_wizz
    configuration dataclasses, to a python dictionary."""
    for field in fields(config_cls):
        if field.name == parname:
            return {k[4:]: v for k, v in field.metadata.items()}
    raise AttributeError(f"{config_cls} has no attribute '{parname}'")


def create_param(
    category: Literal["backend", "binning", "scales", "resampling"],
    parname: str,
) -> StageParameter:
    """Hook into the yet_another_wizz parameter defaults and help system to
    construct a StageParameter."""

    config_cls_name = category.capitalize() + "Config"
    metadata = get_yaw_config_meta(
        config_cls=getattr(config, config_cls_name),
        parname=parname,
    )

    config_default = getattr(config.DEFAULT, category.capitalize())
    default = getattr(config_default, parname, None)

    return StageParameter(
        dtype=metadata.get("type"),
        default=default,
        required=metadata.get("required", False),
        msg=metadata.get("help"),
    )


class YawRailStage(ABC, RailStage):
    algo_parameters: set[str]

    def __init_subclass__(cls, config_items: dict[str, StageParameter] | None = None):
        cls.name = cls.__name__
        super().__init_subclass__()

        cls.config_options = super().config_options.copy()

        if config_items is None:
            config_items = {}
        else:
            config_items = copy(config_items)
        cls.config_options.update(config_items)
        cls.algo_parameters = set(config_items.keys())

        cls.config_options["verbose"] = config_verbose  # used for yaw logger

        param_str = "Parameters\n    ----------\n"
        for name, param in config_items.items():
            msg = param._help  # pylint: disable=W0212; PR filed in ceci
            param_str += f"    {name}: {param.dtype.__name__} \n"
            param_str += f"        {msg}\n"
        cls.__doc__ = cls.__doc__.replace("@YawParameters", param_str)

    def get_algo_config_dict(
        self, exclude: Container[str] | None = None
    ) -> dict[str, Any]:
        if exclude is None:
            exclude = []
        return {
            key: param
            for key, param in self.get_config_dict(reduce_config=True).items()
            if key in self.algo_parameters and key not in exclude
        }

    def get_optional_handle(self, tag: str, **kwarg) -> DataHandle | None:
        try:
            return self.get_handle(tag, allow_missing=False, **kwarg)
        except KeyError:
            return None

    def get_optional_data(self, tag: str, **kwarg) -> Any | None:
        try:
            return self.get_data(tag, allow_missing=False, **kwarg)
        except KeyError:
            return None

    def set_optional_data(self, tag: str, value: Any | None, **kwarg) -> None:
        if value is not None:
            self.set_data(tag, value, **kwarg)
