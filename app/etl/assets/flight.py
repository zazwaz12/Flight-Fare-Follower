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
    This function extracts and loads flight data using a Flight API client and a PostgreSQL client.
    It reads a list of airport codes from a CSV file, fetches flight prices for various durations
    using the Flight API, and then upserts this data into a specified PostgreSQL database table.

    Parameters:
        flight_api_client (FlightApiClient): An instance of FlightApiClient to fetch flight data.
        postgresql_client (PostgreSqlClient): An instance of PostgreSqlClient for database operations.
        airport_codes_reference_path (Path): The file path to the CSV containing airport codes.
        table (Table): The SQLAlchemy table object where data will be upserted.
        metadata (MetaData): The SQLAlchemy MetaData object associated with the database.

    Returns:
        pd.DataFrame: A DataFrame containing the compiled flight data, including origins, destinations,
                      departure and return dates, duration, and prices.
    """
    df_airport_codes = pd.read_csv(airport_codes_reference_path)
    current_date = datetime.now()
    current_date_formatted = current_date.strftime("%Y-%m-%d")
    next_day = current_date + timedelta(days=1)
    next_day_formatted = next_day.strftime("%Y-%m-%d")
    days_later_120 = current_date + timedelta(days=120)
    days_later_120_formatted = days_later_120.strftime("%Y-%m-%d")
    duration_from = 9
    duration_to = 9  # 15

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
