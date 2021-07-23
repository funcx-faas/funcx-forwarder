""" funcX : Fast function serving for clouds, clusters and supercomputers.

"""
import logging
from .version import VERSION
from pythonjsonlogger import jsonlogger

__author__ = "The funcX team"
__version__ = VERSION


def set_stream_logger(name='funcx_forwarder', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Args:
         - name (string) : Set the logger name.
         - level (logging.LEVEL) : Set to logging.DEBUG by default.
         - format_string (string) : Set to None by default.

    Returns:
         - Logger
    """
    if format_string is None:
        format_string = "%(asctime)s %(name)s %(levelname)s %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = jsonlogger.JsonFormatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


logging.getLogger('funcx_forwarder').addHandler(logging.NullHandler())
