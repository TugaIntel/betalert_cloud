import logging


def setup_logger(entity_name, level=logging.INFO):
    """Sets up a logger with options for local or cloud environment."""

    logger = logging.getLogger(entity_name)
    logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    from google.cloud import logging as cloud_logging
    client = cloud_logging.Client()
    handler = cloud_logging.handlers.CloudLoggingHandler(client, name=entity_name)

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Stream handler for errors only
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.ERROR)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
