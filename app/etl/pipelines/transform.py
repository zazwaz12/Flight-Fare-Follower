from etl.connectors.postgresql import PostgreSqlClient
from jinja2 import Environment, FileSystemLoader
from etl.assets.transform import SqlTransform, transform
from dotenv import load_dotenv
import os
import yaml
from pathlib import Path
from etl.assets.pipeline_logging import PipelineLogging
from graphlib import TopologicalSorter


def run_pipeline(pipeline_config: dict):
    load_dotenv()

    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            PIPELINE_NAME = pipeline_config.get("name")
            logger = PipelineLogging(
                pipeline_name=PIPELINE_NAME, postgresql_client=postgresql_client
            )
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    try:
        transform_template_environment = Environment(
            loader=FileSystemLoader("etl/assets/sql/transform")
        )

        # create nodes
        staging_flights = SqlTransform(
            table_name="staging_flights",
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
        )
        serving_avg_price_dow = SqlTransform(
            table_name="serving_avg_price_dow",
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
        )
        serving_avg_price_month = SqlTransform(
            table_name="serving_avg_price_month",
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
        )
        serving_flight_price_exchange_rate = SqlTransform(
            table_name="serving_flight_price_exchange_rate",
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
        )
        serving_flight_price_oil_price = SqlTransform(
            table_name="serving_flight_price_oil_price",
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
        )
        serving_min_max_flight_HKG = SqlTransform(
            table_name="serving_min_max_flight_HKG",
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
        )

        # create DAG
        dag = TopologicalSorter()
        dag.add(staging_flights)
        dag.add(serving_avg_price_dow, staging_flights)
        dag.add(serving_avg_price_month, staging_flights)
        dag.add(serving_flight_price_exchange_rate)
        dag.add(serving_flight_price_oil_price)
        dag.add(serving_min_max_flight_HKG)

        logger.log_message(
            print,
            message="Transform data from all APIs",
            process="[Transform] Transform data",
            output="START",
        )

        # run transform
        transform(dag=dag)

        logger.log_message(
            print,
            message="Transform data from all APIs",
            process="[Transform] Transform data",
            output="SUCCESS",
        )
    except BaseException as e:
        logger.log_message(
            print,
            message=f"Transform pipeline run failed. See detailed logs: {e}",
            process="[Transform] Transform Pipeline Failed",
            output="FAIL",
        )
