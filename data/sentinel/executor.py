# executor.py

from pyspark.sql import SparkSession, DataFrame
import logging

from data.sentinel.data_loader import load_data
from data.sentinel.strategy_factory import StrategyFactory


class Executor:
    """
    Base class for executors.
    """

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config

    def execute(self):
        """
        Executes the logic defined in the subclass.
        Should be implemented in each subclass.
        """
        raise NotImplementedError(
            "Execute method must be implemented in subclass"
        )


class LoadExecutor(Executor):
    """
    Executor for loading data.
    """

    def execute(self) -> DataFrame:
        """
        Loads data into a Spark DataFrame and registers it as a temporary view.
        """
        try:
            step_name = self.config["name"]
            file_path = self.config["path"]
            file_format = self.config["format"]

            df = load_data(file_path, file_format, self.spark)
            df.createOrReplaceTempView(step_name)

            # consider moving write logic to a separate executor
            if self.config.get("write", False):
                file_path = self.config["write_path"]
                file_format = self.config["write_format"]

                df.write.mode("overwrite").format(file_format).save(file_path)

            return df

        except Exception as e:
            logging.error(f"Error in LoadExecutor: {e}")
            raise


class TransformExecutor(Executor):
    """
    Executor for transforming data.
    """

    def execute(self) -> DataFrame:
        """
        Executes a SQL query on a temporary view and registers the result
        as a new temporary view.
        """
        try:
            step_name = self.config["name"]
            query = self.config["query"]

            df = self.spark.sql(query)
            df.createOrReplaceTempView(step_name)

            # consider moving write logic to a separate executor
            if self.config.get("write", False):
                file_path = self.config["write_path"]
                file_format = self.config["write_format"]

                df.write.mode("overwrite").format(file_format).save(file_path)

            return df

        except Exception as e:
            logging.error(f"Error in TransformExecutor: {e}")
            raise

class TesterExecutor(Executor):
    def __init__(self, spark: SparkSession,config: dict):
        super().__init__(spark,config)
        self.strategy = StrategyFactory.get_comparison_strategy(config)

    def execute(self) -> tuple[DataFrame, DataFrame, DataFrame]:
        df_a_name = self.config['dataset_a']
        df_b_name = self.config['dataset_b']
        df_a = self.spark.table(df_a_name)
        df_b = self.spark.table(df_b_name)
        mismatches, a_only, b_only = self.strategy.compare(df_a, df_b, self.config['join_columns'], self.config['compare_columns'])
        mismatches.show() #Need to improve this later
        a_only.show()
        b_only.show()

