import requests
import json

class ExchangeApiClient:
    def __init__(self, api_key):
        if api_key is None:
            raise ValueError("API key cannot be set to None.")
        
        self.api_key = api_key
        self.base_currency = "AUD"
        self.target_currencies = ["INR", "HKD", "NZD", "USD"]
        self.url = f"https://exchange-rates.abstractapi.com/v1/live?api_key={api_key}&base={self.base_currency}&target={','.join(self.target_currencies)}"

    def get_exchange_rates(self):
        try:
            response = requests.get(self.url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            #to change it from a json formatted string to a json object
            data = json.loads(response.text)
            return data["exchange_rates"]
        except requests.exceptions.RequestException as e:
            print(f"Error fetching exchange rates: {e}")
            return None
