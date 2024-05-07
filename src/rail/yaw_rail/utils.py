"""
This file implements a utility functions that automatically create
`ceci.config.StageParameter` instances from their corresponding parameter in
*yet_another_wizz* by hooking into its integrated help system. Additionally,
it provides a decorator for RAIL stages that automatically populates the
stage configuration and building class doc-string.
"""
from __future__ import annotations

from abc import ABC
from dataclasses import fields
from typing import TYPE_CHECKING, Any, Literal, Type

from yaw import config

from ceci.config import StageParameter
from rail.core.stage import RailStage

if TYPE_CHECKING:
    from rail.core.data import DataHandle


def handle_has_path(handle: DataHandle) -> bool:
    """This is a workaround for a potential bug in RAIL."""
    if handle.path is None:
        return False
    return handle.path != "None"


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


class ParsedRailStage(ABC, RailStage):
    def __init__(self, args, comm=None):
        super().__init__(args, comm=comm)
        for name, param in self.config_options.items():
            if name in args:
                param.set(args[name])


def railstage_add_params_and_docs(**kwargs: StageParameter):
    def decorator(cls: Type[ParsedRailStage]):
        cls.config_options.update(kwargs)
        cls._method_parameters = set(kwargs.keys())  # pylint: disable=W0212

        param_str = "Parameters\n    ----------\n"
        for name, param in kwargs.items():
            param_str += f"    {name}: {param.dtype.__name__} \n"
            param_str += f"        {param._help}\n"  # pylint: disable=W0212
        cls.__doc__ = cls.__doc__.replace("@Parameters", param_str)

        return cls
    return decorator


def unpack_stageparam_dict(stage: ParsedRailStage) -> dict[str, Any]:
    return {
        key: param for key, param in stage.get_config_dict().items()
        if key in stage._method_parameters  # pylint: disable=W0212
    }
