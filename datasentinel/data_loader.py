from pyspark.sql import SparkSession, DataFrame
from abc import ABC, abstractmethod
import os
import yaml


def load_data(file_path: str, file_format: str, spark: SparkSession) -> DataFrame:
    """
    Loads data into a Spark DataFrame.

    Args:
        file_path: Path to the data file.
        file_format: Format of the data file (e.g., "csv", "parquet", "json", "avro", "xls", "xlsx").
        spark: SparkSession.

    Returns:
        A Spark DataFrame containing the data.
    """
    loader = get_file_loader(file_format)
    return loader.load(file_path, spark)


def load_table_data(
    db_type: str,
    connection_string: str,
    table_name: str,
    spark: SparkSession
) -> DataFrame:
    """
    Loads data from a database table into a Spark DataFrame.

    Args:
        db_type: Type of the database (e.g., "oracle", "hive", "postgres").
        connection_string: JDBC connection string.
        table_name: Name of the table to load.
        spark: SparkSession.

    Returns:
        A Spark DataFrame containing the data from the database table.
    """
    if db_type in ("oracle", "hive", "postgres"):
        df = (
            spark.read.format("jdbc")
            .option("url", connection_string)
            .option("dbtable", table_name)
            .option("driver", get_driver_class(db_type))
            .load()
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    return df


def load_config(config_path: str) -> dict:
    """Loads the configuration from a YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_driver_class(db_type: str) -> str:
    """Returns the driver class name for a given database type."""
    driver_classes = {
        "oracle": "oracle.jdbc.driver.OracleDriver",
        "postgres": "org.postgresql.Driver",
        "hive": "org.apache.hive.jdbc.HiveDriver",  # Example, verify correct classname
    }
    return driver_classes.get(db_type)


class FileLoader(ABC):
    """
    Abstract base class for file loaders.
    """

    @abstractmethod
    def load(self, file_path: str, spark: SparkSession) -> DataFrame:
        """
        Loads data into a Spark DataFrame.

        Args:
            file_path: Path to the data file.
            spark: SparkSession.

        Returns:
            A Spark DataFrame containing the data.
        """
        pass


class CsvLoader(FileLoader):
    """
    Loads data from a CSV file.
    """

    def load(self, file_path: str, spark: SparkSession) -> DataFrame:
        return spark.read.csv(file_path, header=True, inferSchema=True)


class ParquetLoader(FileLoader):
    """
    Loads data from a Parquet file.
    """

    def load(self, file_path: str, spark: SparkSession) -> DataFrame:
        return spark.read.parquet(file_path)


class JsonLoader(FileLoader):
    """
    Loads data from a JSON file.
    """

    def load(self, file_path: str, spark: SparkSession) -> DataFrame:
        return spark.read.json(file_path)


class AvroLoader(FileLoader):
    """
    Loads data from an Avro file.
    """

    def load(self, file_path: str, spark: SparkSession) -> DataFrame:
        return spark.read.format("avro").load(file_path)


file_loaders = {
    "csv": CsvLoader(),
    "parquet": ParquetLoader(),
    "json": JsonLoader(),
    "avro": AvroLoader(),
}


def get_file_loader(file_format: str) -> FileLoader:
    """
    Returns the file loader for a given file format.
    """
    loader = file_loaders.get(file_format)
    if not loader:
        raise ValueError(f"Unsupported file format: {file_format}")
    return loader
