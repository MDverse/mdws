"""Define logger."""

import sys

import loguru
from loguru import logger


def create_logger(logpath: str | None = None, level: str = "INFO") -> "loguru.Logger":
    """Create the logger with optional file logging.

    Parameters
    ----------
    logpath : str | None, optional
        Path to the log file. If None, no file logging is done.
    level : str, optional
        Logging level. Default is "INFO".

    Returns
    -------
    loguru.Logger
        Configured logger instance.
    """
    # Define log format.
    logger_format = (
        "{time:YYYY-MM-DD HH:mm:ss} "
        "| <level>{level:<8}</level> "  # noqa: RUF027
        "| <level>{message}</level>"
    )
    # Remove default logger.
    logger.remove()
    # Add logger to path (if path is provided).
    if logpath:
        logger.add(logpath, format=logger_format, level="DEBUG", mode="w")
    # Add logger to stdout.
    logger.add(sys.stdout, format=logger_format, level=level)
    return logger
