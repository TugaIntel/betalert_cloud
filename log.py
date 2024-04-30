import logging
import os
from logging.handlers import TimedRotatingFileHandler


def setup_logger(entity_name, level=logging.INFO):
    """Sets up a logger for a specific entity with 7-day retention in a single file."""

    logger = logging.getLogger(entity_name)
    logger.setLevel(level)

    log_dir = '/apps/betalert/logs'
    os.makedirs(log_dir, exist_ok=True)  # Create directory if it doesn't exist

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = TimedRotatingFileHandler(
        os.path.join(log_dir, f"{entity_name}.log"),
        when="midnight",
        interval=1,
        backupCount=7
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Stream handler for errors only
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.ERROR)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger

