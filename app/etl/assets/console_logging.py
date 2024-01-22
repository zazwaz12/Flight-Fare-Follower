import logging


class ConsoleLogging:
    def __init__(self, pipeline_name: str):
        self.logger = logging.getLogger(pipeline_name)
        if not self.logger.hasHandlers():  # Check if handlers are already added
            self.logger.setLevel(logging.INFO)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(logging.INFO)
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)
