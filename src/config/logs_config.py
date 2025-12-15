import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from src.config.config import settings

def setup_logging(level: str = settings.logger_level, log_file: str = settings.logger_file):

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    log_level = getattr(logging, level.upper(), logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('aiogram').setLevel(logging.INFO)

    return root_logger

