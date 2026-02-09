import numpy as np
#from datasentinel.native_kernel import compare_with_tolerance


def _null_mask(arr):
    try:
        return np.isnan(arr)
    except TypeError:
        return arr == None


def _coerce_numeric(arr):
    try:
        return arr.astype(float)
    except (TypeError, ValueError):
        return None


def run_local_tolerance(
    joined_df, compare_defs, *, left_suffix="_left", right_suffix="_right"
):
    """
    Run tolerance based comparison on a joined pandas dataframe.
    Returns np.ndarray[bool]: one boolean per row indicating whether all comparisons passed.
    """
    if not compare_defs:
        raise ValueError("No compare columns specified")
    per_column_result = []
    for col, cfg in compare_defs.items():
        tol = cfg.get("tolerance")
        if tol is None:
            tol = 0
        left_col = f"{col}{left_suffix}"
        right_col = f"{col}{right_suffix}"
        if left_col not in joined_df.columns or right_col not in joined_df.columns:
            raise KeyError(f"Missing column {left_col} or {right_col}")
        left = joined_df[left_col].to_numpy()
        right = joined_df[right_col].to_numpy()
        # null handling
        left_is_null = _null_mask(left)
        right_is_null = _null_mask(right)
        both_null = left_is_null & right_is_null

        # initialize column result as False
        col_result = np.zeros(len(joined_df), dtype=bool)
        # both nulls -> match
        col_result[both_null] = True
        # both_present -> numeric compare when possible, else string equality
        both_present = ~(left_is_null | right_is_null)
        if np.any(both_present):
            left_num = _coerce_numeric(left[both_present])
            right_num = _coerce_numeric(right[both_present])
            if left_num is not None and right_num is not None:
                col_result[both_present] = np.abs(left_num - right_num) <= tol
            else:
                col_result[both_present] = left[both_present] == right[both_present]
        # one-null rows remain False
        per_column_result.append(col_result)
    return np.logical_and.reduce(per_column_result)

    
                         
