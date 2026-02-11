from functools import reduce
from operator import or_ as or_operator
from typing import Dict, Any, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, when, abs as sql_abs, max as sql_max


def _spark_null_masks(left_col, right_col):
    both_null = left_col.isNull() & right_col.isNull()
    one_null = left_col.isNull() != right_col.isNull()
    return both_null, one_null


def _string_mismatch_expr(col_a, col_b):
    return ~col_a.eqNullSafe(col_b)


def _numeric_mismatch_expr(col_a, col_b, tolerance):
    col_a_num = col_a.cast("double")
    col_b_num = col_b.cast("double")
    both_null, one_null = _spark_null_masks(col_a, col_b)
    numeric_invalid = (
        (col_a_num.isNull() & col_a.isNotNull())
        | (col_b_num.isNull() & col_b.isNotNull())
    )
    numeric_mismatch = sql_abs(col_a_num - col_b_num) > float(tolerance)
    return when(both_null, False).otherwise(one_null | numeric_invalid | numeric_mismatch)


def _row_infer_mismatch_expr(col_a, col_b, tolerance):
    col_a_num = col_a.cast("double")
    col_b_num = col_b.cast("double")
    both_null, one_null = _spark_null_masks(col_a, col_b)
    numeric_invalid = (
        (col_a_num.isNull() & col_a.isNotNull())
        | (col_b_num.isNull() & col_b.isNotNull())
    )
    numeric_mismatch = sql_abs(col_a_num - col_b_num) > float(tolerance)
    return when(
        both_null,
        False,
    ).otherwise(
        one_null
        | (~numeric_invalid & numeric_mismatch)
        | (numeric_invalid & (col_a != col_b))
    )


def _build_mismatch_expr(col_a, col_b, options, *, non_numeric_flag_col=None):
    options = options or {}
    semantics = options.get("semantics", "column_infer")
    tolerance = options.get("tolerance", 0)
    if semantics == "string":
        return _string_mismatch_expr(col_a, col_b)
    if semantics == "numeric":
        return _numeric_mismatch_expr(col_a, col_b, tolerance)
    if semantics == "row_infer":
        return _row_infer_mismatch_expr(col_a, col_b, tolerance)
    if semantics == "column_infer":
        if non_numeric_flag_col is None:
            raise ValueError("column_infer requires non_numeric_flag_col")
        return when(
            non_numeric_flag_col,
            _string_mismatch_expr(col_a, col_b),
        ).otherwise(_numeric_mismatch_expr(col_a, col_b, tolerance))
    raise ValueError(f"Unknown semantics: {semantics}")


def run_full_outer_join_recon(
    df_a: DataFrame,
    df_b: Optional[DataFrame],
    join_cols,
    compare_cols,
    compare_defs,
) -> Dict[str, Any]:
    if df_b is None:
        raise ValueError("full_recon requires RHS dataset.")

    a = df_a.alias("a")
    b = df_b.alias("b")

    join_condition = [
        col(f"a.{join_col}") == col(f"b.{join_col}") for join_col in join_cols
    ]

    joined_df = a.join(b, on=join_condition, how="fullouter")
    needs_column_infer = any(
        (compare_defs.get(col_name) or {}).get("semantics") == "column_infer"
        for col_name in compare_cols
    )
    if needs_column_infer:
        non_numeric_aggs = []
        for col_name in compare_cols:
            options = compare_defs.get(col_name) or {}
            if options.get("semantics") != "column_infer":
                continue
            col_a = col(f"a.{col_name}")
            col_b = col(f"b.{col_name}")
            non_numeric_expr = (
                (col_a.cast("double").isNull() & col_a.isNotNull())
                | (col_b.cast("double").isNull() & col_b.isNotNull())
            )
            non_numeric_aggs.append(
                sql_max(when(non_numeric_expr, 1).otherwise(0)).alias(
                    f"{col_name}_non_numeric"
                )
            )
        if non_numeric_aggs:
            flags_df = joined_df.agg(*non_numeric_aggs)
            joined_df = joined_df.crossJoin(flags_df)

    def select_columns(df: DataFrame) -> DataFrame:
        join_select = [
            coalesce(col(f"a.{join_col}"), col(f"b.{join_col}")).alias(join_col)
            for join_col in join_cols
        ]

        compare_select = [
            col(f"a.{col_name}").alias(f"{col_name}_left")
            for col_name in compare_cols
        ] + [
            col(f"b.{col_name}").alias(f"{col_name}_right")
            for col_name in compare_cols
        ]

        mismatch_select = []
        for col_name in compare_cols:
            col_a = col(f"a.{col_name}")
            col_b = col(f"b.{col_name}")
            options = compare_defs.get(col_name) or {}
            non_numeric_flag_col = None
            if options.get("semantics") == "column_infer":
                non_numeric_flag_col = col(f"{col_name}_non_numeric") == 1
            mismatch_expr = _build_mismatch_expr(
                col_a,
                col_b,
                options,
                non_numeric_flag_col=non_numeric_flag_col,
            )
            mismatch_select.append(
                when(mismatch_expr, 1).otherwise(0).alias(f"{col_name}_mismatch")
            )

        return df.select(*join_select, *compare_select, *mismatch_select)

    selected_df = select_columns(joined_df)

    mismatch_filters = [
        col(f"{col_name}_mismatch") == 1 for col_name in compare_cols
    ]
    mismatches = selected_df.filter(reduce(or_operator, mismatch_filters))

    a_present = reduce(or_operator, [col(f"a.{c}").isNotNull() for c in join_cols])
    b_present = reduce(or_operator, [col(f"b.{c}").isNotNull() for c in join_cols])
    a_only = select_columns(joined_df.filter(a_present & ~b_present))
    b_only = select_columns(joined_df.filter(b_present & ~a_present))

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
