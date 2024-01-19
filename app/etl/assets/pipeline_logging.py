import logging
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from etl.connectors.postgresql import PostgreSqlClient

# This is default properties for SQLAlchemy inheritance
Base = declarative_base()


class LogEntry(Base):
    """
    A SQLAlchemy ORM model representing a log entry in the 'pipeline_processes' table.

    This class defines the structure of the 'pipeline_processes' table in the database, where
    log entries from various pipeline processes are stored. Each log entry contains information
    such as the timestamp, pipeline name, process name, log level, message, and any output.

    Attributes:
        __tablename__ (str): The name of the table in the database.
        id (Column): The primary key column, an integer uniquely identifying each log entry.
        timestamp (Column): The date and time of the log entry, stored as a DateTime.
        pipeline_name (Column): The name of the pipeline generating the log, stored as a String.
        process (Column): The name of the process within the pipeline, stored as a String.
        log_level (Column): The log level (e.g., INFO, ERROR), stored as a String.
        message (Column): The log message, stored as a String.
        output (Column): Any additional output associated with the log entry, stored as a String.
    """

    __tablename__ = "pipeline_processes"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    pipeline_name = Column(String)
    process = Column(String)
    log_level = Column(String)
    message = Column(String)
    output = Column(String)


class PipelineLogging:
    """
    A class for handling logging of pipeline processes in a PostgreSQL database.

    This class provides functionality for initializing a logger for a specific pipeline,
    configuring the database for storing log entries, and logging messages with various
    levels (INFO, ERROR, etc.) to the database.

    Attributes:
        pipeline_name (str): The name of the pipeline for which logging is being performed.
        postgresql_client (PostgreSqlClient): An instance of the PostgreSqlClient to interact with the PostgreSQL database.
        logger (Logger): A logging.Logger instance for logging messages.

    Methods:
        initialize_logger: Initializes the logger for the pipeline.
        configure_database: Configures the database for storing log entries.
        log_message: Logs a message to the database with the specified log level, process, and output.
    """

    def __init__(
        self,
        pipeline_name: str,
        postgresql_client: PostgreSqlClient,
    ):
        self.pipeline_name = pipeline_name
        self.postgresql_client = postgresql_client
        self.logger = self.initialize_logger()
        if self.logger:
            self.configure_database()

    def initialize_logger(self):
        """
        Initializes a logger for the specified pipeline.

        Checks if a logger with the same name already exists to avoid duplicate handlers.
        Sets the logging level to INFO.

        Returns:
            Logger: A logging.Logger instance for the pipeline, or None if already initialized.
        """
        logger = logging.getLogger(self.pipeline_name)
        if logger.handlers:
            # Logger already initialized, return None
            return None

        logger.setLevel(logging.INFO)
        # Configure database connection and log relevant information
        return logger

    def configure_database(self) -> None:
        """
        Configures the database for storing log entries.

        Creates the necessary table in the database if it doesn't exist and initializes a session for database transactions.
        Logs an info message upon successful connection or an error message in case of failure.
        """
        try:
            Base.metadata.create_all(self.postgresql_client.engine)
            Session = sessionmaker(bind=self.postgresql_client.engine)
            self.session = Session()
            self.logger.info("Connected to the logging database successfully.")
        except SQLAlchemyError as e:
            self.logger.error(f"Error connecting to the logging database: {str(e)}")

    def log_message(self, log_level, message, process, output) -> None:
        """
        Logs a message to the database with the specified log level, process, and output.

        Args:
            log_level (str): The level of the log (e.g., INFO, ERROR).
            message (str): The log message to be stored.
            process (str): The name of the process generating the log.
            output (str): Any additional output related to the log entry.

        This method creates a new log entry in the database and commits the transaction.
        In case of an error during logging, it logs an error message using the logger.
        """
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
