# comparison_strategy.py

from abc import ABC, abstractmethod
from typing import Tuple
from functools import reduce
from operator import or_ as or_operator
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, when


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
        if not join_cols:
            raise ValueError("At least one join column is required.")

        a = df_a.alias("a")
        b = df_b.alias("b")

        join_condition = [
            col(f"a.{join_col}") == col(f"b.{join_col}") for join_col in join_cols
        ]

        joined_df = a.join(b, on=join_condition, how="fullouter")

        def select_columns(df: DataFrame) -> DataFrame:
            join_select = [
                coalesce(col(f"a.{join_col}"), col(f"b.{join_col}")).alias(join_col)
                for join_col in join_cols
            ]

            compare_select = [
                col(f"a.{col_name}").alias(f"a_{col_name}")
                for col_name in compare_cols
            ] + [
                col(f"b.{col_name}").alias(f"b_{col_name}")
                for col_name in compare_cols
            ]

            mismatch_select = [
                when(
                    (col(f"a.{col_name}") != col(f"b.{col_name}"))
                    | col(f"a.{col_name}").isNull()
                    | col(f"b.{col_name}").isNull(),
                    1,
                ).otherwise(0).alias(f"{col_name}_mismatch")
                for col_name in compare_cols
            ]

            return df.select(*join_select, *compare_select, *mismatch_select)

        selected_df = select_columns(joined_df)

        mismatch_filters = [
            col(f"{col_name}_mismatch") == 1 for col_name in compare_cols
        ]
        mismatches = selected_df.filter(reduce(or_operator, mismatch_filters))

        a_only = select_columns(
            joined_df.filter(
                col(f"b.{join_cols[0]}").isNull()
                & col(f"a.{join_cols[0]}").isNotNull()
            )
        )
        b_only = select_columns(
            joined_df.filter(
                col(f"a.{join_cols[0]}").isNull()
                & col(f"b.{join_cols[0]}").isNotNull()
            )
        )

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
