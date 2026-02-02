import logging.config
import os
from pathlib import Path

LOG_DIRECTORY = Path("logs")
LOG_FILE_NAME = "fpl_agent.log"
DEFAULT_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


def setup_logging(default_level: str = DEFAULT_LOG_LEVEL):
    """
    Initializes a centralized logging system.

    Justification: Using dictConfig over basicConfig allows for a declarative 
    structure that is easier to maintain and scale (Clean Code: Data-Driven Systems).
    """

    if not LOG_DIRECTORY.exists():
        LOG_DIRECTORY.mkdir(parents=True)

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "detailed": {
                "format": "%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": default_level,
                "formatter": "standard",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",  # Always log details to file for post-mortem analysis
                "formatter": "detailed",
                "filename": LOG_DIRECTORY / LOG_FILE_NAME,
                "maxBytes": 10485760,  # 10MB - Performance choice: prevent disk bloat
                "backupCount": 5,      # Keep 5 historical logs
                "encoding": "utf8",
            },
        },
        "loggers": {
            # Specific logger for our package to avoid noise from dependencies
            "fpl_agent": {
                "handlers": ["console", "file"],
                "level": default_level,
                "propagate": False,
            },
        },
        "root": {
            "handlers": ["console"],
            "level": "WARNING",
        },
    }

    logging.config.dictConfig(config)
