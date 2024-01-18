from etl.connectors.postgresql import PostgreSqlClient
from etl.connectors.flight_api import FlightApiClient
from etl.assets.flight import extract_load_flights
from dotenv import load_dotenv
import os
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
import yaml
from pathlib import Path
import schedule
import time
import logging
import pdb
from etl.assets.pipeline_logging import PipelineLogging

# logging.basicConfig(level=print, format="%(asctime)s - %(levelname)s - %(message)s")


def run_pipeline(pipeline_config: dict):
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

            flightLogger = PipelineLogging(
                pipeline_name=PIPELINE_NAME, postgresql_client=postgresql_client
            )
            flightLogger.log_message(
                print,
                message="The logging is set up on flight.py",
                process="Logging Set-UP",
                output="SUCCESS.",
            )
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    try:
        DATABASE_NAME = os.environ.get("DB_LOG_FLIGHT_NAME")

        print("Creating Amadeus API client")

        flight_api_client = FlightApiClient(
            client_id=API_KEY, client_secret=API_SECRET_KEY
        )

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
        print("Extracting and loading data from Amadeus API")

        extract_load_flights(
            flight_api_client=flight_api_client,
            postgresql_client=postgresql_client,
            airport_codes_reference_path=pipeline_config.get("config").get(
                "airport_code_reference_path"
            ),
            table=table,
            metadata=metadata,
        )
        print("Pipeline run successful")
    except BaseException as e:
        print(f"Pipeline run failed. See detailed logs: {e}")
