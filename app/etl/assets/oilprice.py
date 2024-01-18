import pandas as pd
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl.connectors.postgresql import PostgreSqlClient
from etl.connectors.oilprice_api import OilPriceApiClient
from datetime import datetime, timedelta
import time

def extract_load_oilprice(
    oilprice_api_client: OilPriceApiClient,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
) -> pd.DataFrame:
    current_date = datetime.now()
    current_date_formatted = current_date.strftime("%Y-%m-%d")
    response_data = oilprice_api_client.get_prices()

    oil_data = {
    "viewedAt": current_date_formatted,   # Map created_at to viewedAt
    "price": response_data.get("price"),           # Map price to price
    "currency": response_data.get("currency"),     # Map currency to currency
    "commodity": response_data.get("code")             # Map code to index
    }


    df_oilprice = pd.json_normalize(oil_data)
    postgresql_client.upsert(
        data=df_oilprice.to_dict(orient="records"), table=table, metadata=metadata
    )
    return df_oilprice