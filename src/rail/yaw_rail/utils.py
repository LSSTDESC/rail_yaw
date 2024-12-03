"""
This module implements some simple utility functions and logging facilites.
Furthermore, it implements an extension to the default `RailStage` that
simplfies safe access to optional stage inputs (handles/data) that is commonly
used in the wrapper stages.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from functools import lru_cache, wraps
from typing import TYPE_CHECKING

from pandas import read_parquet
from yaw.utils import get_logger

from rail.core.stage import RailStage
from rail.yaw_rail import stage_config

if TYPE_CHECKING:
    from collections.abc import Container
    from typing import Any

    from ceci.config import StageParameter
    from pandas import DataFrame

    from rail.core.data import DataHandle

__all__ = [
    "YawRailStage",
    "get_dc2_test_data",
    "yaw_logged",
]


@lru_cache
def get_dc2_test_data() -> DataFrame:
    """
    Download a small dataset with positions and redshifts, derived from DC2.

    Taken from 25 sqdeg, limited to 100k objects with redshifts `0.2 <= z < 1.8`.

    Returns
    -------
    DataFrame
        Table containing right ascension (`ra`), declination (`dec`) and
        redshift (`z`).
    """
    return read_parquet("https://portal.nersc.gov/cfs/lsst/PZ/test_dc2_rail_yaw.pqt")


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
        cls.name = cls.__name__  # standard RAIL practice

        if config_items is None:
            config_items = {}  # pragma: no cover
        else:
            config_items = config_items.copy()
        cls.algo_parameters = set(config_items.keys())  # track all parameters

        cls.config_options = super().config_options.copy()
        cls.config_options.update(config_items)  # standard RAIL practice
        cls.config_options["verbose"] = stage_config.yaw_verbose  # used for yaw logger

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
        kwargs.update(allow_missing=True)  # this is required
        handle = self.get_handle(tag, **kwargs)
        # the handle is only set if there is either a path or data
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
        kwargs.update(allow_missing=True)  # this is required
        handle: DataHandle = self.get_handle(tag, **kwargs)
        # test if handle has any data referenced, otherwise handle is not set
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


def init_logger(level: str = "info") -> logging.Logger:
    """Init a logger that writes *yet_another_wizz* messages to stdout in a
    custom format."""
    return get_logger(level=level)


def yaw_logged(method):
    """
    Decorator that creates a temporary logger for a method of a `YawRailStage`
    that redirects messages emitted by *yet_another_wizz* to stdout.
    """

    @wraps(method)
    def impl(self: YawRailStage, *args, **kwargs):
        logger = init_logger(level=self.get_config_dict()["verbose"])
        try:
            return method(self, *args, **kwargs)
        finally:
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)

    return impl
