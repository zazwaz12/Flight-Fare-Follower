from etl.connectors.postgresql import PostgreSqlClient
from etl.connectors.oilprice_api import OilPriceApiClient
from etl.assets.oilprice import extract_load_oilprice
from dotenv import load_dotenv
import os
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
import yaml
from pathlib import Path
import schedule
import time

def run_pipeline():
    API_SECRET_KEY = os.environ.get("OIL_API_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    try:
        print("Creating Oil API client")
        oilprice_api_client = OilPriceApiClient(
            client_secret=API_SECRET_KEY
        )
        postgresql_client = PostgreSqlClient(
            server_name=SERVER_NAME,
            database_name=DATABASE_NAME,
            username=DB_USERNAME,
            password=DB_PASSWORD,
            port=PORT,
        )
        metadata = MetaData()
        table = Table(
            "oil_price",
            metadata,
            Column("viewedAt", String, primary_key=True),
            Column("price", String),
            Column("currency", String),
            Column("commodity", String)
        )
        print("Extracting and loading data from Oil Price API")
        extract_load_oilprice(
            oilprice_api_client=oilprice_api_client,
            postgresql_client=postgresql_client,
            table=table,
            metadata=metadata,
        )
        print("Pipeline run successful")
    except BaseException as e:
        print(f"Pipeline run failed. See detailed logs: {e}")


if __name__ == "__main__":
    load_dotenv()

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            PIPELINE_NAME = pipeline_config.get("name")
            # CONFIG = pipeline_config.get("config")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    run_pipeline()