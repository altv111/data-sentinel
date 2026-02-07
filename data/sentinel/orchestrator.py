# orchestrator.py

from pyspark.sql import SparkSession

from data.sentinel.data_loader import load_config
from data.sentinel.executor import (
    LoadExecutor,
    TransformExecutor,
    TesterExecutor,
)

EXECUTOR_REGISTRY = {
    "load": LoadExecutor,
    "transform": TransformExecutor,
    "test": TesterExecutor,
}


class Orchestrator:
    """
    Main class for comparing two datasets.
    """

    def __init__(self, spark: SparkSession, config_path: str):
        self.spark = spark
        self.config = load_config(config_path)

    def execute_steps(self):
        """
        Executes the steps defined in the configuration.
        """
        for step in self.config["steps"]:
            try:
                step_type = step["type"]
                executor_cls = EXECUTOR_REGISTRY.get(step_type)
                if executor_cls is None:
                    supported = ", ".join(sorted(EXECUTOR_REGISTRY))
                    raise ValueError(
                        f"Unknown step type '{step_type}' in step "
                        f"'{step.get('name', 'unnamed')}'. "
                        f"Supported types: {supported}"
                    )
                executor_cls(self.spark, step).execute()

            except Exception as e:
                print(f"Failed to execute step. See {e}")
                raise
