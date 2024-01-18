import pandas as pd
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl.connectors.postgresql import PostgreSqlClient
from etl.connectors.exchange_api import ExchangeApiClient
from datetime import datetime, timedelta
import time


def extract_load_airport_currencies(
    exchange_api_client: ExchangeApiClient,
    postgresql_client: PostgreSqlClient,
    airport_currency_reference_path: Path,
    table: Table,
    metadata: MetaData,
) -> pd.DataFrame:
    """
    Perform extraction using a filepath which contains a list of cities.
    """
    df_airport_currencies = pd.read_csv(airport_currency_reference_path)
    current_date = datetime.now()
    current_date_formatted = current_date.strftime("%Y-%m-%d")
    exchange_currency_data = exchange_api_client.get_exchange_rates()
    df_combined = pd.merge(df_airport_currencies, exchange_currency_data, on='currencyCode')
    df_combined["viewedAt"] = current_date_formatted
    print(df_combined)

    postgresql_client.upsert(
        data=df_combined.to_dict(orient="records"), table=table, metadata=metadata
    )
    return df_combined
