"""
This file implements utility functions that help generating stage parameters
from the *yet_another_wizz* configuration classes. Furthermore, it implements an
extension to the default `RailStage` that simplfies safe access to optional
stage inputs (handles/data) that is commonly used in the wrapper stages.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Container
from dataclasses import fields
from typing import TYPE_CHECKING, Any, Literal

from yaw import config

from ceci.config import StageParameter
from rail.core.stage import RailStage
from rail.yaw_rail.logging import config_yaw_verbose

if TYPE_CHECKING:  # pragma: no cover
    from rail.core.data import DataHandle

__all__ = [
    "YawRailStage",
    "create_param",
    "handle_has_path",
]


def get_yaw_config_meta(config_cls: Any, parname: str) -> dict[str, Any]:
    """Convert the parameter metadata, embedded in the *yet_another_wizz*
    configuration dataclasses, to a python dictionary."""
    for field in fields(config_cls):
        if field.name == parname:
            return {k[4:]: v for k, v in field.metadata.items()}
    raise AttributeError(f"{config_cls} has no attribute '{parname}'")


def create_param(
    category: Literal["backend", "binning", "scales", "resampling"],
    parname: str,
) -> StageParameter:
    """
    Hook into *yet_another_wizz* configuration and defaults to construct a
    `StageParameter` from a *yet_another_wizz* configuration class.

    Parameters
    ----------
    category : str
        Prefix of one of the *yet_another_wizz* configuration classes that
        defines the parameter of interest, e.g. `"scales"` for
        `yaw.config.ScalesConfig`.
    parname : str
        The name of the parameter of interest, e.g. `"rmin"` for
        `yaw.config.ScalesConfig.rmin`.

    Returns
    -------
    StageParameter
        Parameter metadata including `dtype`, `default`, `required` and `msg`
        values set.
    """
    category = category.lower().capitalize()

    metadata = get_yaw_config_meta(
        config_cls=getattr(config, f"{category}Config"),
        parname=parname,
    )

    config_default = getattr(config.DEFAULT, category)
    default = getattr(config_default, parname, None)

    return StageParameter(
        dtype=metadata.get("type"),
        default=default,
        required=metadata.get("required", False),
        msg=metadata.get("help"),
    )


def handle_has_path(handle: DataHandle) -> bool:
    """This is a workaround for a peculiarity of `ceci`."""
    return handle.path is not None and handle.path.lower() != "none"


class YawRailStage(ABC, RailStage):
    """
    Base class for any `RailStage` used in this wrapper package.

    It introduces a few quality-of-life improvements compared to the base
    `RailStage` when creating a sub-class. These include:

    - adding a methods to safely access optional stage inputs (handles/data),
    - setting the `name` attribute automatically to the class name,
    - copying the default `RailStage.config_options`,
    - providing an interface to directly register a dictionary of algorithm-
      specific stage parameters, and
    - automatically adding the `"verbose"` parameter to the stage, which
      controlls the log-level filtering for the *yet_another_wizz* logger.

    The names of all algorithm-specific parameters are tracked in the special
    attribute `algo_parameters`. There is a special method to get a dictionary
    of just those parameters.

    Examples
    --------
    Create a new stage with one algorithm specific parameter called `"param"`.
    The resulting subclass will have the standard `RailStage` parameters and an
    additional parameter `"verbose"`.

    >>> class MyStage(
    ...     YawRailStage,
    ...     config_items=dict(
    ...         param=StageParameter(dtype=int)
    ...     ),
    ... ):
    """

    algo_parameters: set[str]
    """Lists the names of all algorithm-specific parameters that were added when
    subclassing."""

    def __init_subclass__(
        cls, config_items: dict[str, StageParameter] | None = None, **kwargs
    ):
        cls.name = cls.__name__

        if config_items is None:
            config_items = {}  # pragma: no cover
        else:
            config_items = config_items.copy()
        cls.algo_parameters = set(config_items.keys())

        cls.config_options = super().config_options.copy()
        cls.config_options.update(config_items)
        cls.config_options["verbose"] = config_yaw_verbose  # used for yaw logger

        super().__init_subclass__(**kwargs)  # delegate back to rail/ceci

    def get_algo_config_dict(
        self, exclude: Container[str] | None = None
    ) -> dict[str, Any]:
        """
        Return the algorithm-specific configuration.

        Same as `get_config_dict`, but only returns those parameters that are
        listed in `algo_parameters`, i.e. been added as stage parameters when
        creating the subclass.

        Parameters
        ----------
        exclude : Container of str, optional
            Listing of parameters not to include in the output.

        Returns
        -------
        dict
            Dictionary containing pairs of parameter names and (default) values.
        """
        if exclude is None:
            exclude = []
        return {
            key: param
            for key, param in self.get_config_dict(reduce_config=True).items()
            if (key in self.algo_parameters) and (key not in exclude)
        }

    def get_optional_handle(self, tag: str, **kwargs) -> DataHandle | None:
        """
        Access an optional handle an return `None` if it is not set.

        Parameters
        ----------
        tag : str
            The requested tag.
        **kwargs : dict, optional
            Parameters passed on to `get_handle`.

        Returns
        -------
        DataHandle or None
            The handle or nothing if not set.
        """
        kwargs = kwargs.copy()
        kwargs.update(allow_missing=True)
        handle = self.get_handle(tag, **kwargs)
        if handle_has_path(handle) or handle.data is not None:
            return handle
        return None

    def get_optional_data(self, tag: str, **kwargs) -> Any | None:
        """
        Access the data of an optional handle and return `None` if it is not set.

        Parameters
        ----------
        tag : str
            The requested tag.
        **kwargs : dict, optional
            Parameters passed on to `get_data`.

        Returns
        -------
        Any or None
            The handle's data or nothing if not set.
        """
        kwargs = kwargs.copy()
        kwargs.update(allow_missing=True)
        handle: DataHandle = self.get_handle(tag, **kwargs)
        if handle.data is not None:
            return handle.data
        if handle_has_path(handle):
            return handle.read()
        return None

    def set_optional_data(self, tag: str, value: Any | None, **kwarg) -> None:
        """
        Set a handle's data if the provided value is not None.

        Parameters
        ----------
        tag : str
            The requested tag.
        value : Any or None
            The data to assing to the handle unless `None` is provided.
        **kwargs : dict, optional
            Parameters passed on to `set_data`.
        """
        if value is not None:
            self.set_data(tag, value, **kwarg)

    @abstractmethod
    def run(self) -> None:
        pass  # pragma: no cover
