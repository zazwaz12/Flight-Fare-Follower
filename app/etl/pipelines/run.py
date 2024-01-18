import os
import yaml
from pathlib import Path
import schedule
import time
from etl.connectors.postgresql import PostgreSqlClient
from dotenv import load_dotenv
from importlib import import_module

if __name__ == "__main__":
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

    while True:
        for pipeline_config in multi_pipeline_config.get("pipelines"):
            print("run pipeline")
            pipeline_name = pipeline_config.get("name")
            module = import_module(name=f".{pipeline_name}", package="etl.pipelines")
            module.run_pipeline(pipeline_config=pipeline_config)
        time.sleep(15)
