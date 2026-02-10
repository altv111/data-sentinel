# Arrow Recon Strategy (Design Sketch)

This document outlines a minimal Arrow-backed strategy that keeps comparisons on executors via
Spark Arrow/Pandas UDFs and a C++ Arrow kernel.

## Goals
- Avoid driver-side `toPandas()` while keeping row-level comparison semantics.
- Reuse the existing compare config (`tolerance`, `semantics`) and return per-column match flags.
- Keep the first implementation narrow and extensible.

## Scope
- Initial semantics: `row_infer` only.
- Per-column output: one boolean column per compare column.
- No column-level inference inside the kernel. If needed, compute `has_non_numeric` in Spark.

## Data Flow
Executor JVM -> Arrow batch -> Python Pandas UDF -> C++ Arrow kernel -> Arrow boolean arrays

## C++ Kernel API
Per-column output is required to preserve mismatch flags:

```cpp
arrow::Result<std::vector<std::shared_ptr<arrow::BooleanArray>>> CompareColumns(
    const std::vector<std::shared_ptr<arrow::Array>>& left,
    const std::vector<std::shared_ptr<arrow::Array>>& right,
    const std::vector<double>& tolerances,
    const std::vector<Semantics>& semantics,
    const std::vector<bool>& has_non_numeric
);
```

Semantics:
- `row_infer`: per row, numeric compare if both parse; else string compare.
- `numeric`: numeric compare; non-numeric -> mismatch.
- `string`: string compare; tolerance ignored.
- `column_infer`: requires `has_non_numeric` per column.

## Python Wrapper
Expose the kernel via pybind11 and a Python wrapper that accepts Arrow arrays and returns
Arrow boolean arrays. See `datasentinel/arrow_kernel.py` for a skeleton.

## Spark UDF Wiring
Use a Pandas UDF that receives `*_left`/`*_right` Series and returns a Struct of boolean
match flags:

```python
@pandas_udf("struct<price_match:boolean,qty_match:boolean>")
def compare_udf(price_left, price_right, qty_left, qty_right):
    left_arrays = [pa.Array.from_pandas(price_left), pa.Array.from_pandas(qty_left)]
    right_arrays = [pa.Array.from_pandas(price_right), pa.Array.from_pandas(qty_right)]
    out = compare_columns_arrow(left_arrays, right_arrays, tolerances, semantics, has_non_numeric)
    return pd.DataFrame({
        "price_match": out[0].to_pandas(),
        "qty_match": out[1].to_pandas(),
    })
```

## Spark Integration Plan
1. Join LHS/RHS as usual.
2. Project `*_left`/`*_right` compare columns.
3. Apply the Pandas UDF to get per-column match flags.
4. Convert match flags to mismatch flags (`~match`) and build outputs (`mismatches`, `a_only`, `b_only`).

## Build Outline
- A stub pybind11 module is provided in `cpp/arrow_kernel.cpp` and `setup_arrow.py`.
- Build it with:
  - `python setup_arrow.py build_ext --inplace`
- The build uses `pyarrow` to locate Arrow headers and libraries.

If import fails due to missing `libarrow.so.*`, set:
```bash
export LD_LIBRARY_PATH=/path/to/site-packages/pyarrow:$LD_LIBRARY_PATH
```

## Testing
- Reuse `datasetC/datasetD` for row-level inference validation.
- Add a numeric-only dataset for tolerance checks.
