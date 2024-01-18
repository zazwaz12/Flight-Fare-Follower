import logging
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import URL
import time
from datetime import datetime
import os
from etl.connectors.postgresql import PostgreSqlClient

# This is default properties for SQLAlchemy inheritance
Base = declarative_base()

# logging_url = URL.create(
""" drivername="postgresql+pg8000",
    username=os.environ.get("DB_USERNAME"),
    password=os.environ.get("DB_PASSWORD"),
    host=os.environ.get("SERVER_NAME"),
    port=os.environ.get("PORT"),
    database=os.environ.get("DB_LOG_FLIGHT_NAME")
    )
"""


class LogEntry(Base):
    __tablename__ = "pipeline_processes"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    pipeline_name = Column(String)
    process = Column(String)
    log_level = Column(String)
    message = Column(String)
    output = Column(String)


class PipelineLogging:
    def __init__(
        self,
        pipeline_name: str,
        postgresql_client: PostgreSqlClient,
    ):
        print("creating pipeline logging")
        self.pipeline_name = pipeline_name
        self.postgresql_client = postgresql_client
        self.engine = self.postgresql_client.get_engine()
        self.logger = self.initialize_logger()
        if self.logger:
            self.configure_database()

    def initialize_logger(self):
        logger = logging.getLogger(self.pipeline_name)
        if logger.handlers:
            # Logger already initialized, return None
            return None

        logger.setLevel(logging.INFO)
        # Configure database connection and log relevant information
        return logger

    def configure_database(self):
        try:
            Base.metadata.create_all(self.engine)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            self.logger.info("Connected to the logging database successfully.")
        except SQLAlchemyError as e:
            self.logger.error(f"Error connecting to the logging database: {str(e)}")

    def log_message(self, log_level, message, process, output):
        try:
            with self.session.begin():
                # Perform database operations (add, update, delete)
                log_entry = LogEntry(
                    timestamp=datetime.utcnow(),
                    pipeline_name=self.pipeline_name,
                    log_level=log_level,
                    process=process,
                    message=message,
                    output=output,
                )
                self.session.add(log_entry)
        except Exception as e:
            self.logger.error(f"Error logging message: {str(e)}")
