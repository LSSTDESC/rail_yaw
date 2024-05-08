"""
This file implements a context wrapper that allows displaying *yet_another_wizz*
logging messages on stderr, which is used in RAIL stages that call
*yet_another_wizz* code.
"""

from __future__ import annotations

import logging
import sys
from contextlib import contextmanager

from ceci.stage import StageParameter

__all__ = [
    "yaw_logged",
]


config_verbose = StageParameter(
    str,
    required=False,
    default="info",
    msg="lowest log level emitted by *yet_another_wizz*",
)


class OnlyYawFilter(logging.Filter):
    def filter(self, record):
        record.exc_info = None
        record.exc_text = None
        return "yaw" in record.name


def init_logger(level: str = "info") -> logging.Logger:
    level = getattr(logging, level.upper())
    formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.setLevel(level)
    handler.addFilter(OnlyYawFilter())

    logging.basicConfig(level=level, handlers=[handler])
    return logging.getLogger()


@contextmanager
def yaw_logged(level: str = "debug"):
    logger = init_logger(level=level)
    try:
        yield logger
    finally:
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
