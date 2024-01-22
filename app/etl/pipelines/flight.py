from etl.connectors.postgresql import PostgreSqlClient
from etl.connectors.flight_api import FlightApiClient
from etl.assets.flight import extract_load_flights
from dotenv import load_dotenv
import os
from sqlalchemy import Table, MetaData, Column, Integer, String
import yaml
from pathlib import Path
from etl.assets.pipeline_logging import PipelineLogging
from etl.assets.console_logging import ConsoleLogging


def run_pipeline(pipeline_config: dict):
    """
    Executes the data extraction and loading pipeline.

    It starts by loading environment variables, then initializes clients for the database and the Amadeus API.
    The function also reads the pipeline configuration from a YAML file, sets up a logger, and defines the
    database table schema. Finally, it runs the extraction and loading process and handles any exceptions.

    Args:
        pipeline_config (dict): A dictionary containing pipeline configuration details.

    Raises:
        Exception: If the YAML configuration file is missing.
        BaseException: For any exceptions raised during the pipeline execution.
    """
    load_dotenv()

    API_KEY = os.environ.get("API_KEY")
    API_SECRET_KEY = os.environ.get("API_SECRET_KEY")

    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    PORT = os.environ.get("PORT")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            PIPELINE_NAME = pipeline_config.get("name")

            logger = PipelineLogging(
                pipeline_name=PIPELINE_NAME, postgresql_client=postgresql_client
            )
            console_logger = ConsoleLogging(pipeline_name=PIPELINE_NAME)
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    try:
        logger.log_message(
            print,
            message="Accessing Amadeus API client",
            process="[Amadeus] Amadeus API Setup",
            output="START",
        )
        console_logger.logger.info("Accessing Amadeus API client starting...")

        flight_api_client = FlightApiClient(
            client_id=API_KEY, client_secret=API_SECRET_KEY
        )

        logger.log_message(
            print,
            message="Accessing Amadeus API client",
            process="[Amadeus] Amadeus API Setup",
            output="SUCCESS",
        )
        console_logger.logger.info("Accessing Amadeus API client done!")

        metadata = MetaData()
        table = Table(
            "flight_price",
            metadata,
            Column("viewedAt", String, primary_key=True),
            Column("origin", String, primary_key=True),
            Column("destination", String, primary_key=True),
            Column("duration", Integer, primary_key=True),
            Column("departureDate", String, primary_key=True),
            Column("returnDate", String, primary_key=True),
            Column("cheapestPrice", String),
        )
        logger.log_message(
            print,
            message="Extracting and loading data from Amadeus API",
            process="[Amadeus] Extract and Load",
            output="START",
        )
        console_logger.logger.info(
            "Extracting and loading data from Amadeus API starting..."
        )
        extract_load_flights(
            flight_api_client=flight_api_client,
            postgresql_client=postgresql_client,
            airport_codes_reference_path=pipeline_config.get("config").get(
                "airport_code_reference_path"
            ),
            table=table,
            metadata=metadata,
        )
        logger.log_message(
            print,
            message="Extracting and loading data from Amadeus API",
            process="[Amadeus] Extract and Load",
            output="START",
        )
        console_logger.logger.info("Extracting and loading data from Amadeus API done!")
    except BaseException as e:
        logger.log_message(
            print,
            message=f"Amadeus API EL pipeline run failed. See detailed logs: {e}",
            process="[Amadeus] EL Pipeline Failed",
            output="FAIL",
        )
