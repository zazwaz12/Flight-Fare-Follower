import unittest
from etl.connectors.flight_api import FlightApiClient  # Make sure to replace 'your_module' with the actual module where get_engine is defined

class FlightApi(unittest.TestCase):
        def test_retrieve_access_token(self):
            # Creates an instance of PostgreSqlClient with the necessary parameters
            retriever = FlightApiClient(client_id = "Im5QoQRDvlKJ8tVPfaVeVVBo0W7qvB3z"
                ,client_secret = "tN6q7DWZXyl3AVyv")
            error = retriever.retrieve_access_token()
            self.assertIsNone(error)
        
if __name__ == '__main__':
    unittest.main()
