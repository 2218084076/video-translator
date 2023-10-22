"""
Log util
"""
import os
from logging.config import dictConfig
from pathlib import Path

from crawlers_eyp_shunqi.config import settings


def mkdir(log_path: Path):
    """
    mkdir
    :param log_path:
    :return:
    """
    if not log_path.exists():
        os.makedirs(log_path)


def init_log():
    """Init log"""
    mkdir(Path(settings.LOG_PATH))
    default_logging = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            'verbose': {
                'format': '%(asctime)s %(levelname)s %(name)s %(filename)s(%(lineno)d) %(message)s'
            },
            'simple': {
                'format': '%(asctime)s %(levelname)s %(message)s'
            },
        },
        "handlers": {
            "console": {
                "formatter": 'verbose',
                'level': 'DEBUG',
                "class": "logging.StreamHandler",
            },
            'all_file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': 'DEBUG',
                'formatter': 'verbose',
                'filename': os.path.join(settings.LOG_PATH, 'all.log'),
                'maxBytes': 1024 * 1024 * 1024 * 200,  # 200M
                'backupCount': '5',
                'encoding': 'utf-8'
            },
        },
        "loggers": {
            '': {'level': settings.LOG_LEVEL, 'handlers': ['console', 'all_file']},
        }
    }
    dictConfig(default_logging)
