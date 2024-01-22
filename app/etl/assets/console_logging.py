import logging


class ConsoleLogging:
    def __init__(self, pipeline_name: str):
        logger = logging.getLogger(pipeline_name)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        self.logger = logger
