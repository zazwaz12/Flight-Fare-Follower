import unittest
import sqlalchemy
from postgresql import PostgreSqlClient  # Make sure to replace 'your_module' with the actual module where get_engine is defined

class PostgreSql(unittest.TestCase):
        def test_get_engine(self):
            # Create an instance of PostgreSqlClient with the necessary parameters
            postgres_client = PostgreSqlClient(
                server_name='flights-db-1.clys4ao8guak.ca-central-1.rds.amazonaws.com',
                database_name="flights",
                username="postgres",
                password="flights010",
                port=5432  # Update with your actual port
            )

            # Call the get_engine method and check the type
            engine = postgres_client.get_engine()
            self.assertIs(type(engine.connect()), sqlalchemy.engine.Connection)
        
if __name__ == '__main__':
    unittest.main()
