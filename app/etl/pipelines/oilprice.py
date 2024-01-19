from etl.connectors.postgresql import PostgreSqlClient
from etl.connectors.oilprice_api import OilPriceApiClient
from etl.assets.oilprice import extract_load_oilprice
from dotenv import load_dotenv
import os
from sqlalchemy import Table, MetaData, Column, String
import yaml
from pathlib import Path
from etl.assets.pipeline_logging import PipelineLogging


def run_pipeline(pipeline_config: dict):
    """
    Executes the data extraction and loading pipeline for oil prices.

    The function initializes by loading environment variables, then sets up clients for the database and the Oil Price API.
    It reads the pipeline configuration from a YAML file, establishes a logger, and defines the database table schema.
    The extraction and loading process is then executed, with exception handling included.

    Args:
        pipeline_config (dict): A dictionary containing pipeline configuration details.

    Raises:
        Exception: If the YAML configuration file is missing.
        BaseException: For any exceptions raised during the pipeline execution.
    """
    load_dotenv()

    API_SECRET_KEY = os.environ.get("OIL_API_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

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
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    try:
        logger.log_message(
            print,
            message="Accessing Oil Price API client",
            process="[Oil] Oil Price API Setup",
            output="START",
        )
        oilprice_api_client = OilPriceApiClient(client_secret=API_SECRET_KEY)
        logger.log_message(
            print,
            message="Accessing Oil Price API client",
            process="[Oil] Oil Price API Setup",
            output="SUCCESS",
        )

        metadata = MetaData()
        table = Table(
            "oil_price",
            metadata,
            Column("viewedAt", String, primary_key=True),
            Column("price", String),
            Column("currency", String),
            Column("commodity", String),
        )

        logger.log_message(
            print,
            message="Extracting and loading data from Oil Price API",
            process="[Oil] Oil Price Extract and Load",
            output="START",
        )
        extract_load_oilprice(
            oilprice_api_client=oilprice_api_client,
            postgresql_client=postgresql_client,
            table=table,
            metadata=metadata,
        )
        logger.log_message(
            print,
            message="Extracting and loading data from Oil Price API",
            process="[Oil] Oil Price Extract and Load",
            output="SUCCESS",
        )
    except BaseException as e:
        logger.log_message(
            print,
            message=f"Oil Price API EL pipeline run failed. See detailed logs: {e}",
            process="[Oil] EL Pipeline Failed",
            output="FAIL",
        )
