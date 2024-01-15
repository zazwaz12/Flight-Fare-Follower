import requests


class FlightApiClient:
    def __init__(self, api_key: str):
        self.base_url = "http://api.openweathermap.org/data/2.5"
        if api_key is None:
            raise Exception("API key cannot be set to None.")
        self.api_key = api_key

    def get_prices(self, destination: str, duration: str): 
        pass 