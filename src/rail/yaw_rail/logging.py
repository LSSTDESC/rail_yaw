"""
This file implements a context wrapper that allows displaying *yet_another_wizz*
logging messages on stdout, which is used in RAIL stages that call
*yet_another_wizz* code.
"""

from __future__ import annotations

import logging
from functools import wraps
from sys import stdout
from typing import TYPE_CHECKING

from ceci.stage import StageParameter

if TYPE_CHECKING:  # pragma: no cover
    from rail.yaw_rail.stage import YawRailStage

__all__ = [
    "yaw_logged",
]


config_yaw_verbose = StageParameter(
    str,
    required=False,
    default="info",
    msg="lowest log level emitted by *yet_another_wizz*",
)


class OnlyYawFilter(logging.Filter):
    """A logging filter that rejects all messages not emitted by
    *yet_another_wizz*."""

    def filter(self, record):
        record.exc_info = None
        record.exc_text = None
        return "yaw" in record.name


def init_logger(level: str = "info") -> logging.Logger:
    """Init a logger that writes *yet_another_wizz* messages to stdout in a
    custom format."""
    level = getattr(logging, level.upper())
    formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")

    handler = logging.StreamHandler(stdout)
    handler.setFormatter(formatter)
    handler.setLevel(level)
    handler.addFilter(OnlyYawFilter())

    logging.basicConfig(level=level, handlers=[handler])
    return logging.getLogger()


def yaw_logged(method):
    """
    Decorator that creates a temporary logger for a method of a `YawRailStage`
    that redirects messages emitted by *yet_another_wizz* to stdout.
    """

    @wraps(method)
    def impl(self: YawRailStage, *args, **kwargs):
        config = self.get_config_dict()
        logger = init_logger(level=config["verbose"])
        try:
            return method(self, *args, **kwargs)
        finally:
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)

    return impl
