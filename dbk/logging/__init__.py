import logging.config


def setup_logging():
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "[{asctime}] {levelname:<7} {name:<30} {message}",
                    "style": "{",
                },
            },
            "handlers": {
                "file": {
                    "class": "logging.FileHandler",
                    "filename": "dbk.log",
                    "formatter": "default",
                },
            },
            "loggers": {
                "root": {
                    "handlers": ["file"],
                    "level": "DEBUG",
                    "propagate": False,
                },
            },
        }
    )
