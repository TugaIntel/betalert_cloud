import logging
import os
from logging.handlers import TimedRotatingFileHandler


def setup_logger(entity_name, level=logging.INFO, cloud_env=False):
    """Sets up a logger with options for local or cloud environment."""

    logger = logging.getLogger(entity_name)
    logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if cloud_env:
        from google.cloud import logging as cloud_logging
        client = cloud_logging.Client()
        handler = cloud_logging.handlers.CloudLoggingHandler(client, name=entity_name)
    else:
        log_dir = 'D:\TugaIntel\Bet_Alert\logs'
        os.makedirs(log_dir, exist_ok=True)
        handler = TimedRotatingFileHandler(
            os.path.join(log_dir, f"{entity_name}.log"),
            when="midnight",
            interval=1,
            backupCount=7
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Stream handler for errors only
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.ERROR)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
