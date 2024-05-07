from __future__ import annotations

import logging
import sys


class OnlyYawFilter(logging.Filter):
    def filter(self, record):
        record.exc_info = None
        record.exc_text = None
        return "yaw" in record.name


def init_logger(level: str = "info") -> logging.Logger:
    level = getattr(logging, level.upper())
    handler = logging.StreamHandler(sys.stdout)
    format_str = "%(levelname)s:%(name)s:%(message)s"
    handler.setFormatter(logging.Formatter(format_str))
    handler.setLevel(level)
    handler.addFilter(OnlyYawFilter())
    logging.basicConfig(level=level, handlers=[handler])
    return logging.getLogger()


class YawLogged:
    def __init__(self, level: str = "debug") -> None:
        self.logger = init_logger(level=level)

    def __enter__(self) -> YawLogged:
        return self

    def __exit__(self, *args, **kwargs) -> None:
        for handler in self.logger.handlers[:]:
            handler.close()
            self.logger.removeHandler(handler)
