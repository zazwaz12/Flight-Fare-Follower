from etl.connectors.postgresql import PostgreSqlClient
from etl.connectors.exchange_api import ExchangeApiClient
from etl.assets.exchange import extract_load_airport_currencies
from dotenv import load_dotenv
import os
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
import yaml
from pathlib import Path
import schedule
import time
from etl.assets.pipeline_logging import PipelineLogging


def run_pipeline(pipeline_config: dict):
    load_dotenv()
    EXCHANGE_KEY = os.environ.get("EXCHANGE_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME_EXCHANGE = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME_EXCHANGE,
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
        print("Creating Exchange API client")
        exchange_api_client = ExchangeApiClient(api_key=EXCHANGE_KEY)

        metadata = MetaData()
        table = Table(
            "exchange_price",
            metadata,
            Column("viewedAt", String, primary_key=True),
            Column("currencyCode", String, primary_key=True),
            Column("airportCode", String, primary_key=False),
            Column("value", Float, primary_key=False),
        )
        print("Extracting and loading data from ExchangeAPI ")

        extract_load_airport_currencies(
            exchange_api_client=exchange_api_client,
            postgresql_client=postgresql_client,
            airport_currency_reference_path=pipeline_config.get("config").get(
                "airport_currency_reference_path"
            ),
            table=table,
            metadata=metadata,
        )
        print("Pipeline run successful")
    except BaseException as e:
        print(f"Pipeline run failed. See detailed logs: {e}")
