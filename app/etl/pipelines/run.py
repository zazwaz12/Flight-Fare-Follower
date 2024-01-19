import os
import yaml
from pathlib import Path
import schedule
import time
from etl.connectors.postgresql import PostgreSqlClient
from dotenv import load_dotenv
from importlib import import_module

if __name__ == "__main__":
    """
    This script is designed to continuously run multiple data extraction and loading pipelines.
    It reads the configurations for each pipeline from a YAML file and executes them in a loop.

    The script starts by loading environment variables, then reads the pipeline configurations from
    the YAML file. For each pipeline, it dynamically imports the corresponding module and executes
    its `run_pipeline` function. The process repeats in a continuous loop with a specified sleep
    interval between each iteration.

    Raises:
        Exception: If the YAML configuration file is missing or cannot be found.

    Example:
        To run this script, simply execute it in a Python environment where the required modules
        and packages are installed. Ensure that the YAML configuration file for the pipelines is
        present in the same directory as this script."""
    load_dotenv()
    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            multi_pipeline_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    # while True:
    for pipeline_config in multi_pipeline_config.get("pipelines"):
        pipeline_name = pipeline_config.get("name")
        module = import_module(name=f".{pipeline_name}", package="etl.pipelines")
        module.run_pipeline(pipeline_config=pipeline_config)
    # time.sleep(15)
