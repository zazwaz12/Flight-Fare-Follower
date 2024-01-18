import pandas as pd
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl.connectors.postgresql import PostgreSqlClient
from etl.connectors.flight_api import FlightApiClient
from datetime import datetime, timedelta
import time


def extract_load_flights(
    flight_api_client: FlightApiClient,
    postgresql_client: PostgreSqlClient,
    airport_codes_reference_path: Path,
    table: Table,
    metadata: MetaData,
) -> pd.DataFrame:
    """
    Perform extraction using a filepath which contains a list of cities.
    """
    df_airport_codes = pd.read_csv(airport_codes_reference_path)
    current_date = datetime.now()
    current_date_formatted = current_date.strftime("%Y-%m-%d")
    next_day = current_date + timedelta(days=1)
    next_day_formatted = next_day.strftime("%Y-%m-%d")
    days_later_120 = current_date + timedelta(days=120)
    days_later_120_formatted = days_later_120.strftime("%Y-%m-%d")
    duration_from = 9
    duration_to = 9#15

    flight_data = []
    for code in df_airport_codes["airport_code"]:
        for duration in range(duration_from, duration_to + 1):
            flight_response_data = flight_api_client.get_prices(
                destination=code,
                departure_date_from=next_day_formatted,
                departure_date_to=days_later_120_formatted,
                duration=duration,
            )
            flight_data.extend(
                [
                    {
                        "viewedAt": current_date_formatted,
                        "origin": entry.get("origin"),
                        "destination": entry.get("destination"),
                        "duration": duration,
                        "departureDate": entry.get("departureDate"),
                        "returnDate": entry.get("returnDate"),
                        "cheapestPrice": entry.get("price", {}).get("total"),
                    }
                    for entry in flight_response_data
                ]
            )
            time.sleep(1)  # Pausing for a seconds between each API call

    df_flights = pd.json_normalize(flight_data)
    postgresql_client.upsert(
        data=df_flights.to_dict(orient="records"), table=table, metadata=metadata
    )
    return df_flights
