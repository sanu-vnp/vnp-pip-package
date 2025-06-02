"""
A module providing configurable logger setup for applications and libraries.

This module configures both the root logger and common third-party library loggers
with consistent settings to ensure uniform logging behavior.
"""

import logging
from logging import Logger


class LoggerConfigurator:
    """A class to manage logger configuration state and operations."""

    _library_loggers_configured: bool = False

    @classmethod
    def _configure_library_loggers(cls, level: int) -> None:
        """
        Configure third-party library loggers with consistent settings.

        Sets the specified logging level and removes existing handlers for known
        third-party libraries to ensure they inherit the root logger's configuration.

        Args:
            level: The logging level to set for all third-party library loggers.

        """
        libraries = [
            "requests",
            "gunicorn",
            "uwsgi",
            "celery",
            "urllib3",
            "starlette",
            "uvicorn",
        ]
        for lib in libraries:
            lib_logger = logging.getLogger(lib)
            # Remove existing handlers to prevent duplicates
            for handler in lib_logger.handlers[:]:
                lib_logger.removeHandler(handler)
            lib_logger.setLevel(level)

            console_handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(formatter)
            lib_logger.addHandler(console_handler)
            # Prevent propagation to avoid root logger handling if configured elsewhere
            lib_logger.propagate = False

    @classmethod
    def get_logger(cls, name: str, level: int = logging.DEBUG) -> Logger:
        """
        Retrieve a configured logger instance.

        Initializes logging configuration on first call, including root logger setup
        and third-party library configuration. Subsequent calls return existing loggers.

        Args:
            name: The name of the logger to retrieve/create.
            level: The logging level to configure (only used on first call).

        Returns:
            A configured Logger instance with the specified name.

        """
        if not cls._library_loggers_configured:
            # Configure root logger first to ensure base configuration
            root_logger = logging.getLogger()
            root_logger.setLevel(level)

            # Clear root logger handlers if any to avoid duplicates
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)

            # Setup console handler for root logger
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)

            # Configure third-party loggers
            cls._configure_library_loggers(level)
            cls._library_loggers_configured = True

        # Get the requested logger
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Prevent adding multiple handlers if logger is reused
        if not logger.handlers:
            # Use the same handler as root logger to maintain consistency
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # Prevent propagation to avoid double logging from ancestor handlers
        logger.propagate = False

        return logger


# Public interface remains the same
get_logger = LoggerConfigurator.get_logger
