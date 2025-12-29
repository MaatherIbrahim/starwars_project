import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "mission.log")

def setup_logger() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger("mission_logger")
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if import many times
    if logger.handlers:
        return logger

    # 1MB max per file, keep 7 backups
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=1_000_000,    # ✅ 1MB
        backupCount=7,         # ✅ keep 7 old files
        encoding="utf-8"
    )

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler.setFormatter(formatter)

    #  print to console while developing
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
