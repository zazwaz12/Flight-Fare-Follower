import logging
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import URL
import time
from datetime import datetime
import os

#This is default properties for SQLAlchemy inheritance
Base = declarative_base()

#logging_url = URL.create(
''' drivername="postgresql+pg8000",
    username=os.environ.get("DB_USERNAME"),
    password=os.environ.get("DB_PASSWORD"),
    host=os.environ.get("SERVER_NAME"),
    port=os.environ.get("PORT"),
    database=os.environ.get("DB_LOG_FLIGHT_NAME")
    )
'''
class LogEntry(Base):
    __tablename__ = "pipeline_processes"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    pipeline_name = Column(String)
    process = Column(String)
    log_level = Column(String)
    message = Column(String)



class PipelineLogging:
    def __init__(self, pipeline_name: str, db_uri):
        self.pipeline_name = pipeline_name
        #This is not good practice
        self.db_uri = db_uri
        self.logger = self.initialize_logger()

    def initialize_logger(self):
        logger = logging.getLogger(self.pipeline_name)
        logger.setLevel(logging.INFO)
        # Configure database connection and log relevant information
        self.configure_database()
        return logger

    def configure_database(self):
        try:
            engine = create_engine(self.db_uri)
            Base.metadata.create_all(engine)
            Session = sessionmaker(bind=engine)
            self.session = Session()
            self.logger.info("Connected to the logging database successfully.")
        except SQLAlchemyError as e:
            self.logger.error(f"Error connecting to the logging database: {str(e)}")

    def log_message(self, log_level, message, process, output):
        try:
            timestamp = datetime.utcnow()  # Obtain the current timestamp
            timestamp_str = timestamp.strftime("%y-%m-%d-%H-%M-%S")  # Format as string
            with self.session.begin():
                # Perform database operations (add, update, delete)
                log_entry = LogEntry(
                    timestamp=timestamp_str,  # Add the formatted timestamp string
                    pipeline_name=self.pipeline_name,
                    log_level=log_level,
                    process=process,
                    message=message,
                    output=output
                )
                self.session.add(log_entry)
            self.logger.info("Log entry added successfully.")
        except SQLAlchemyError as e:
            # Handle exceptions
            self.session.rollback()
            self.logger.error(f"Error adding log entry: {str(e)}")

