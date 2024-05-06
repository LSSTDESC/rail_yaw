"""
This file implements a utility functions that automatically create
`ceci.config.StageParameter` instances from their corresponding parameter in
*yet_another_wizz* by hooking into its integrated help system. Additionally,
it provides a decorator for RAIL stages that automatically populates the
stage configuration and building class doc-string.
"""
from __future__ import annotations

from dataclasses import fields
from typing import TYPE_CHECKING, Any, Literal, Type

from yaw import config

from ceci.config import StageParameter
if TYPE_CHECKING:
    from rail.core.stage import RailStage


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


def railstage_add_params_and_docs(**kwargs: StageParameter):
    def decorator(cls: Type[RailStage]):
        cls.config_options.update(kwargs)

        param_str = "Parameters\n    ----------\n"
        for name, param in kwargs.items():
            param_str += f"    {name}: {param.dtype.__name__} \n"
            param_str += f"        {param._help}\n"  # pylint: disable=W0212
        cls.__doc__ = cls.__doc__.replace("@Parameters", param_str)

        return cls
    return decorator


def unpack_stageparam_dict(params: dict[str, StageParameter]) -> dict[str, Any]:
    return {key: param.value for key, param in params.items()}
