import pandas as pd
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl.connectors.postgresql import PostgreSqlClient
from etl.connectors.oilprice_api import OilPriceApiClient
from etl.assets.console_logging import ConsoleLogging
from datetime import datetime


def extract_load_oilprice(
    oilprice_api_client: OilPriceApiClient,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
) -> pd.DataFrame:
    """
    Extracts and loads oil price data from an Oil Price API and inserts it into a PostgreSQL database.

    This function retrieves the latest oil price data using the Oil Price API and normalizes this data
    into a structured format. It then upserts this data into a specified table within a PostgreSQL database.

    Parameters:
        oilprice_api_client (OilPriceApiClient): Client to interact with the Oil Price API.
        postgresql_client (PostgreSqlClient): Client for executing database operations.
        table (Table): SQLAlchemy table object where data will be inserted.
        metadata (MetaData): SQLAlchemy MetaData object associated with the database.

    Returns:
        pd.DataFrame: A DataFrame containing the normalized oil price data.

    """
    console_logger = ConsoleLogging(pipeline_name="oilprice")
    current_date = datetime.now()
    current_date_formatted = current_date.strftime("%Y-%m-%d")
    console_logger.logger.info(f"Getting oil prices...")
    response_data = oilprice_api_client.get_prices()
    console_logger.logger.info(f"Getting oil prices... done!")

    oil_data = {
        "viewedAt": current_date_formatted,  # Map created_at to viewedAt
        "price": response_data.get("price"),  # Map price to price
        "currency": response_data.get("currency"),  # Map currency to currency
        "commodity": response_data.get("code"),  # Map code to index
    }

    df_oilprice = pd.json_normalize(oil_data)
    console_logger.logger.info(f"Upserting oil prices data...")
    postgresql_client.upsert(
        data=df_oilprice.to_dict(orient="records"), table=table, metadata=metadata
    )
    console_logger.logger.info(f"Upserting oil prices data... done!")
    return df_oilprice
