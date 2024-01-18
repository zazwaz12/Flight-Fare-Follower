import requests
from time import sleep


class OilPriceApiClient:
    def __init__(self, client_secret):
        self.url = "https://api.oilpriceapi.com/v1/prices/latest"

        if client_secret is None:
            raise Exception("API key cannot be set to None.")
        
        self.client_secret = client_secret

    def get_prices(
        self
    ):
        headers = {
            "Authorization": f"{self.client_secret}",
            "Content-Type": "application/json"
                   }

        response = requests.get(url = self.url, headers=headers)

        if response.status_code == 200:
            return response.json().get("data",{})
        else:
            raise Exception(
                f"Failed to extract data from oil price api API. Status Code: {response.status_code}. Response: {response.text}"
            )