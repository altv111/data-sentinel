import sys

from pyspark.sql import SparkSession
from data.sentinel.orchestrator import Orchestrator
from data.sentinel.strategy_factory import StrategyFactory


def main():
    """
    Main entry point for the data-assert CLI.
    """
    if len(sys.argv) != 2:
        print("Usage: assert_data <yaml_config_path>")
        sys.exit(1)

    config_path = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName("DataAssertion")
        .getOrCreate()
    )

    comparator = Orchestrator(spark, config_path)
    comparator.execute_steps()

    spark.stop()


if __name__ == "__main__":
    main()
