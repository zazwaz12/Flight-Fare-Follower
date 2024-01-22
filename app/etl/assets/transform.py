from jinja2 import Environment
from etl.connectors.postgresql import PostgreSqlClient
from graphlib import TopologicalSorter


class SqlTransform:
    """
    A class for SQL data transformation in a PostgreSQL database using Jinja2 templating.

    This class encapsulates the functionality required to create or replace a table in a PostgreSQL database
    based on SQL code defined in a Jinja2 template. The template is selected based on the name of the table
    to be created or replaced.

    Attributes:
        postgresql_client (PostgreSqlClient): An instance of the PostgreSqlClient, used to execute SQL commands.
        environment (Environment): An instance of Jinja2 Environment, used for SQL template rendering.
        table_name (str): The name of the table to be created or replaced in the PostgreSQL database.
        template: The Jinja2 template loaded based on the table_name, containing the SQL commands.

    Methods:
        create_table_as: Creates or replaces the table in the PostgreSQL database using the rendered SQL template.
    """

    def __init__(
        self,
        postgresql_client: PostgreSqlClient,
        environment: Environment,
        table_name: str,
    ):
        self.postgresql_client = postgresql_client
        self.table_name = table_name
        self.template = environment.get_template(f"{table_name}.sql")

    def create_table_as(self) -> None:
        """
        Executes the SQL commands to create or replace a table in the PostgreSQL database.

        This method constructs the SQL command using the rendered Jinja2 template and executes it
        via the PostgreSqlClient. It first drops the table if it already exists, then creates a new
        table based on the SQL defined in the template.
        """
        exec_sql = f"""
            drop table if exists {self.table_name};
            create table {self.table_name} as (
                {self.template.render()}
            )
        """
        self.postgresql_client.execute_sql(exec_sql)


def transform(dag: TopologicalSorter) -> None:
    """
    Executes a series of SQL transformations based on a topological order.

    This function takes a directed acyclic graph (DAG) of SqlTransform nodes and executes
    the SQL transformation for each node in a topologically sorted order. This ensures that
    dependencies between transformations are respected.

    Args:
        dag (TopologicalSorter): A directed acyclic graph (DAG) of SqlTransform nodes representing
                                 the dependencies between different SQL transformations.
    """

    dag_rendered = tuple(dag.static_order())
    for node in dag_rendered:
        node.create_table_as()
