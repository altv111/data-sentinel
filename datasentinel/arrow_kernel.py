from __future__ import annotations

from typing import Iterable, List, Optional


SEMANTICS_MAP = {
    "column_infer": 0,
    "row_infer": 1,
    "numeric": 2,
    "string": 3,
}


def compare_columns_arrow(
    left_arrays: Iterable,
    right_arrays: Iterable,
    tolerances: List[float],
    semantics: List[str],
    has_non_numeric: Optional[List[bool]] = None,
):
    """
    Wrapper for the native Arrow kernel.

    Expects Arrow arrays and returns a list of Arrow BooleanArray objects,
    one per compare column.
    """
    try:
        import datasentinel_arrow as _ext  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "datasentinel_arrow extension not installed. "
            "Build and install the Arrow kernel to use ArrowReconStrategy."
        ) from exc

    sem_ids = [SEMANTICS_MAP[s] for s in semantics]
    if has_non_numeric is None:
        has_non_numeric = [False] * len(sem_ids)
    return _ext.compare_columns(
        list(left_arrays),
        list(right_arrays),
        tolerances,
        sem_ids,
        has_non_numeric,
    )


def build_arrow_compare_udf(compare_cols, compare_defs, *, has_non_numeric=None):
    """
    Build a Pandas UDF that returns per-column match flags.

    This is a skeleton for wiring the C++ Arrow kernel into Spark.
    """
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StructType, StructField, BooleanType
    import pandas as pd
    import pyarrow as pa

    tolerances = []
    semantics = []
    for name in compare_cols:
        options = compare_defs.get(name) or {}
        tolerances.append(float(options.get("tolerance", 0)))
        semantics.append(options.get("semantics", "row_infer"))

    fields = [StructField(f"{name}_match", BooleanType(), nullable=True) for name in compare_cols]
    schema = StructType(fields)

    @pandas_udf(schema)
    def _compare_udf(*cols):
        left_arrays = []
        right_arrays = []
        for i, name in enumerate(compare_cols):
            left_arrays.append(pa.Array.from_pandas(cols[2 * i]))
            right_arrays.append(pa.Array.from_pandas(cols[2 * i + 1]))

        out = compare_columns_arrow(
            left_arrays,
            right_arrays,
            tolerances,
            semantics,
            has_non_numeric=has_non_numeric,
        )
        data = {f"{name}_match": out[i].to_pandas() for i, name in enumerate(compare_cols)}
        return pd.DataFrame(data)

    return _compare_udf
