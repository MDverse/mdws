import sys
import loguru
from loguru import logger

logger_format = (
    "{time:YYYY-MM-DD HH:mm:ss}"
    "| {level:<8} "
    "| {message}"
)

def create_logger(logpath: str | None = None) -> "loguru.Logger":
    """Create the logger with optional file logging."""
    logger.remove()
    if logpath:
        logger.add(logpath, format=logger_format, level="DEBUG")
    logger.add(sys.stderr, format=logger_format, level="INFO")
    return logger
