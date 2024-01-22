import requests
from time import sleep
from etl.assets.pipeline_logging import PipelineLogging

class FlightApiClient:
    def __init__(self, client_id, client_secret):
        self.base_url = "https://test.api.amadeus.com/v1/security/oauth2/token"
        self.prices_url = "https://test.api.amadeus.com/v1/shopping/flight-dates"

        if client_id is None or client_secret is None:
            raise Exception("API key cannot be set to None.")

        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None

    def retrieve_access_token(self):
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        params = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        max_retries = 3  # Set the maximum number of retries
        retry_delay = 5  # Delay between retries in seconds

        for attempt in range(max_retries):
            try:
                response = requests.post(self.base_url, data=params, headers=headers)
                response.raise_for_status()
                self.access_token = response.json()["access_token"]
                break  # Exit the loop if the request was successful
            except requests.exceptions.RequestException as e:
               # print(f"Request failed: {e}")
                if attempt < max_retries - 1:
                    #print(f"Retrying in {retry_delay} seconds...")
                    sleep(retry_delay)
                    print(e)
                else:
                    return str(e)


    def get_prices(
        self,
        destination: str,
        departure_date_from: str,
        departure_date_to: str,
        duration: str,
    ):
        self.retrieve_access_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}

        params = {
            "origin": "SYD",
            "destination": destination,
            "departureDate": f"{departure_date_from},{departure_date_to}",
            "duration": duration,
            "oneWay": False,
            "viewBy": "DATE",
            "nonStop": True,
        }

        response = requests.get(self.prices_url, params=params, headers=headers)

        if response.status_code == 200:
            return response.json().get("data")
        else:
            raise Exception(
                f"Failed to extract data from Amadeus API. Status Code: {response.status_code}. Response: {response.text}"
            )
