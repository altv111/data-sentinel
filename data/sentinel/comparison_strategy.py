# comparison_strategy.py

from abc import ABC, abstractmethod
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class ComparisonStrategy(ABC):
    """
    Abstract base class for comparison strategies.
    """

    @abstractmethod
    def compare(
        self,
        df_a: DataFrame,
        df_b: DataFrame,
        join_cols: list[str],
        compare_cols: list[str]
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Compares two Spark DataFrames.

        Args:
            df_a: The first Spark DataFrame.
            df_b: The second Spark DataFrame.
            join_cols: List of columns to join on.
            compare_cols: List of columns to compare.

        Returns:
            A tuple of three Spark DataFrames:
            (mismatches, a_only, b_only)
        """
        pass


class FullOuterJoinStrategy(ComparisonStrategy):
    """
    Implements a comparison strategy using a full outer join.
    """

    def compare(
        self,
        df_a: DataFrame,
        df_b: DataFrame,
        join_cols: list[str],
        compare_cols: list[str]
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:

        # Register DataFrames as temporary views
        df_a.createOrReplaceTempView("df_a")
        df_b.createOrReplaceTempView("df_b")

        spark = df_a.sparkSession  # or df_b.sparkSession since they both exist at this point

        # Construct the SQL query
        join_condition = " AND ".join(
            [f"a.{join_col} = b.{join_col}" for join_col in join_cols]
        )

        select_expressions = [
            f"CASE WHEN a.{col} != b.{col} OR a.{col} IS NULL OR b.{col} IS NULL "
            f"THEN 1 ELSE 0 END AS {col}_mismatch"
            for col in compare_cols
        ]

        select_clause = ", ".join(select_expressions)

        sql_query = f"""
        SELECT *, {select_clause}
        FROM df_a a FULL OUTER JOIN df_b b
        ON {join_condition}
        """

        joined_df = spark.sql(sql_query)

        mismatches = joined_df.filter(
            " OR ".join([f"{col}_mismatch = 1" for col in compare_cols])
        )

        # Identify A-only and B-only records
        # Assumes at least one join column
        a_only = joined_df.filter(col(f"b.{join_cols[0]}").isNull())
        b_only = joined_df.filter(col(f"a.{join_cols[0]}").isNull())

        return mismatches, a_only, b_only


# You can add other comparison strategies here, e.g.,
class CustomStrategy(ComparisonStrategy):
    def compare(
        self,
        df_a: DataFrame,
        df_b: DataFrame,
        join_cols: list[str],
        compare_cols: list[str]
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        # Implement your custom comparison logic here
        raise NotImplementedError("Custom logic comparison not implemented yet")
