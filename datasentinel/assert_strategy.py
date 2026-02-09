# assert_strategy.py

from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, Optional
from functools import reduce
from operator import or_ as or_operator
from pyspark.sql import DataFrame

from datasentinel.conditions import load_conditions
from datasentinel.native_kernel import run_local_tolerance
from pyspark.sql.functions import col, coalesce, when, abs as sql_abs


class AssertStrategy(ABC):
    """
    Abstract base class for assert strategies.
    """

    required_attributes: Tuple[str, ...] = ()

    def validate(self, attributes: dict) -> dict:
        missing = [key for key in self.required_attributes if not attributes.get(key)]
        if missing:
            required = ", ".join(missing)
            name = self.__class__.__name__
            raise ValueError(f"{name} requires attributes: {required}.")
        return attributes

    @abstractmethod
    def assert_(
        self,
        df_a: DataFrame,
        df_b: Optional[DataFrame],
        attributes: dict
    ) -> Dict[str, Any]:
        """
        Runs an assertion between two Spark DataFrames.

        Args:
            df_a: The left-hand Spark DataFrame.
            df_b: The right-hand Spark DataFrame, if provided.
            attributes: Strategy-specific configuration.

        Returns:
            A dict with a PASS/FAIL status and a nested dict of dataframes.
        """
        pass


class ReconBaseStrategy(AssertStrategy):
    """
    Shared helpers for recon-style strategies.
    """

    required_attributes = ("join_columns", "compare_columns")

    def _parse_recon_attributes(self, attributes: dict):
        attributes = self.validate(attributes)
        join_cols = attributes.get("join_columns")
        compare_cols = attributes.get("compare_columns")
        if isinstance(compare_cols, dict):
            compare_defs = compare_cols
            compare_cols = list(compare_defs.keys())
        elif isinstance(compare_cols, (list, tuple)):
            compare_defs = {name: {} for name in compare_cols}
            compare_cols = list(compare_cols)
        else:
            raise ValueError("compare_columns must be a list or a dict.")
        return join_cols, compare_cols, compare_defs


class FullOuterJoinStrategy(ReconBaseStrategy):
    """
    Implements a full-recon assert using a full outer join.
    """

    def assert_(
        self,
        df_a: DataFrame,
        df_b: Optional[DataFrame],
        attributes: dict
    ) -> Dict[str, Any]:
        if df_b is None:
            raise ValueError("full_recon requires RHS dataset.")
        join_cols, compare_cols, compare_defs = self._parse_recon_attributes(attributes)

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

            mismatch_select = []
            for col_name in compare_cols:
                col_a = col(f"a.{col_name}")
                col_b = col(f"b.{col_name}")
                options = compare_defs.get(col_name) or {}
                tolerance = options.get("tolerance")
                null_mismatch = col_a.isNull() | col_b.isNull()
                if tolerance is not None:
                    col_a_num = col_a.cast("double")
                    col_b_num = col_b.cast("double")
                    non_numeric_mismatch = (
                        (col_a_num.isNull() & col_a.isNotNull())
                        | (col_b_num.isNull() & col_b.isNotNull())
                    )
                    mismatch_expr = (
                        null_mismatch
                        | non_numeric_mismatch
                        | (sql_abs(col_a_num - col_b_num) > float(tolerance))
                    )
                else:
                    mismatch_expr = null_mismatch | (col_a != col_b)
                mismatch_select.append(
                    when(mismatch_expr, 1).otherwise(0).alias(f"{col_name}_mismatch")
                )

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

        mismatches_empty = mismatches.rdd.isEmpty()
        a_only_empty = a_only.rdd.isEmpty()
        b_only_empty = b_only.rdd.isEmpty()
        status = "PASS" if mismatches_empty and a_only_empty and b_only_empty else "FAIL"

        return {
            "status": status,
            "dataframes": {
                "mismatches": mismatches,
                "a_only": a_only,
                "b_only": b_only,
            },
        }


# Alias strategy that supports per-column options (e.g., tolerances).
class LocalFastReconStrategy(ReconBaseStrategy):
    def assert_(self, df_left, df_right, config):
        if df_right is None:
            raise ValueError("localfast_recon requires RHS dataset.")
        join_cols, compare_cols, compare_defs = self._parse_recon_attributes(config)
        joined = self._join_local(df_left, df_right, join_cols, compare_cols)
        joined_pd = joined.toPandas()
        result_col = run_local_tolerance(joined_pd, compare_defs)
        joined_pd["assert_result"] = result_col
        left_present = None
        right_present = None
        for join_col in join_cols:
            left_col = f"{join_col}_left"
            right_col = f"{join_col}_right"
            left_mask = ~joined_pd[left_col].isna()
            right_mask = ~joined_pd[right_col].isna()
            left_present = left_mask if left_present is None else (left_present | left_mask)
            right_present = right_mask if right_present is None else (right_present | right_mask)

        mismatches_pd = joined_pd[left_present & right_present & (~joined_pd["assert_result"])]
        a_only_pd = joined_pd[left_present & (~right_present)]
        b_only_pd = joined_pd[right_present & (~left_present)]

        spark = df_left.sparkSession
        joined = spark.createDataFrame(joined_pd)
        schema = joined.schema
        mismatches = (
            spark.createDataFrame(mismatches_pd)
            if not mismatches_pd.empty
            else spark.createDataFrame([], schema)
        )
        a_only = (
            spark.createDataFrame(a_only_pd)
            if not a_only_pd.empty
            else spark.createDataFrame([], schema)
        )
        b_only = (
            spark.createDataFrame(b_only_pd)
            if not b_only_pd.empty
            else spark.createDataFrame([], schema)
        )
        status = (
            "PASS"
            if mismatches_pd.empty and a_only_pd.empty and b_only_pd.empty
            else "FAIL"
        )
        return {
            "status": status,
            "dataframes": {
                "joined": joined,
                "mismatches": mismatches,
                "a_only": a_only,
                "b_only": b_only,
            },
        }
    def _join_local(self, df_left, df_right, join_cols, compare_cols):
        a = df_left.alias("a")
        b = df_right.alias("b")
        join_condition = [
            col(f"a.{join_col}") == col(f"b.{join_col}") for join_col in join_cols
        ]
        joined = a.join(b, on=join_condition, how="fullouter")
        join_select = [
            coalesce(col(f"a.{join_col}"), col(f"b.{join_col}")).alias(join_col)
            for join_col in join_cols
        ]
        join_side_select = [
            col(f"a.{join_col}").alias(f"{join_col}_left") for join_col in join_cols
        ] + [
            col(f"b.{join_col}").alias(f"{join_col}_right") for join_col in join_cols
        ]
        compare_select = [
            col(f"a.{col_name}").alias(f"{col_name}_left")
            for col_name in compare_cols
        ] + [
            col(f"b.{col_name}").alias(f"{col_name}_right")
            for col_name in compare_cols
        ]
        return joined.select(*join_select, *join_side_select, *compare_select)


    


# You can add other comparison strategies here, e.g.,
class CustomStrategy(AssertStrategy):
    def assert_(
        self,
        df_a: DataFrame,
        df_b: Optional[DataFrame],
        attributes: dict
    ) -> Dict[str, Any]:
        # Implement your custom comparison logic here
        raise NotImplementedError("Custom logic comparison not implemented yet")


class SqlAssertStrategy(AssertStrategy):
    """
    Executes a SQL condition that returns a single boolean-like value.
    """

    def assert_(
        self,
        df_a: DataFrame,
        df_b: Optional[DataFrame],
        attributes: dict
    ) -> Dict[str, Any]:
        condition_name = attributes.get("condition_name")
        inline_sql = attributes.get("sql")
        if not condition_name and not inline_sql:
            raise ValueError("sql_assert requires condition_name or sql.")

        df_a.createOrReplaceTempView("__LHS__")
        if df_b is not None:
            df_b.createOrReplaceTempView("__RHS__")

        sql = inline_sql
        if not sql:
            conditions = load_conditions()
            sql = conditions.get(condition_name)
            if not sql:
                raise ValueError(f"Unknown condition_name: {condition_name}")

        spark = df_a.sparkSession
        result_df = spark.sql(sql)
        columns = result_df.columns
        if len(columns) != 1:
            raise ValueError("sql_assert must return exactly one column.")

        rows = result_df.limit(2).collect()
        if len(rows) != 1:
            raise ValueError("sql_assert must return exactly one row.")

        value = rows[0][0]
        if isinstance(value, bool):
            passed = value
        elif isinstance(value, (int, float)):
            passed = value != 0
        elif isinstance(value, str):
            passed = value.strip().lower() in ("true", "t", "1", "yes")
        else:
            raise ValueError("sql_assert returned non-boolean value.")

        return {
            "status": "PASS" if passed else "FAIL",
            "dataframes": {
                "result": result_df,
            },
        }
