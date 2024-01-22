from etl.connectors.postgresql import PostgreSqlClient
from etl.connectors.exchange_api import ExchangeApiClient
from etl.assets.exchange import extract_load_airport_currencies
from dotenv import load_dotenv
import os
from sqlalchemy import Table, MetaData, Column, String, Float
import yaml
from pathlib import Path
from etl.assets.pipeline_logging import PipelineLogging
from etl.assets.console_logging import ConsoleLogging


def run_pipeline(pipeline_config: dict):
    """
    Executes the data extraction and loading pipeline for airport exchange rates.

    The function starts by loading environment variables, then it sets up clients for the database and the Exchange API.
    It reads the pipeline configuration from a YAML file, sets up a logger, and defines the database table schema.
    The extraction and loading process is executed, with appropriate logging and exception handling.

    Args:
        pipeline_config (dict): A dictionary containing pipeline configuration details.

    Raises:
        Exception: If the YAML configuration file is missing.
        BaseException: For any exceptions raised during the pipeline execution.
    """
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
            message="Accessing Exchange API client",
            process="[Exchange] Exchange API Setup",
            output="START",
        )
        console_logger.logger.info("Accessing Exchange API client starting...")
        exchange_api_client = ExchangeApiClient(api_key=EXCHANGE_KEY)
        logger.log_message(
            print,
            message="Accessing Exchange API client",
            process="[Exchange] Exchange API Setup",
            output="SUCCESS",
        )
        console_logger.logger.info("Accessing Exchange API client done!")
        metadata = MetaData()
        table = Table(
            "exchange_price",
            metadata,
            Column("viewedAt", String, primary_key=True),
            Column("currencyCode", String, primary_key=True),
            Column("airportCode", String, primary_key=False),
            Column("value", Float, primary_key=False),
        )

        logger.log_message(
            print,
            message="Extracting and loading data from Exchange API",
            process="[Exchange] Extract and Load",
            output="START",
        )
        console_logger.logger.info(
            "Extracting and loading data from Exchange API starting..."
        )
        extract_load_airport_currencies(
            exchange_api_client=exchange_api_client,
            postgresql_client=postgresql_client,
            airport_currency_reference_path=pipeline_config.get("config").get(
                "airport_currency_reference_path"
            ),
            table=table,
            metadata=metadata,
        )
        logger.log_message(
            print,
            message="Extracting and loading data from Exchange API",
            process="[Exchange] Extract and Load",
            output="SUCCESS",
        )
        console_logger.logger.info(
            "Extracting and loading data from Exchange API done!"
        )
    except BaseException as e:
        logger.log_message(
            print,
            message=f"Exchange API EL pipeline run failed. See detailed logs: {e}",
            process="[Exchange] EL Pipeline Failed",
            output="FAIL",
        )
