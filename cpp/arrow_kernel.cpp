#include <cmath>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/python/pyarrow.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

namespace {

std::shared_ptr<arrow::Array> ToArray(const py::object& obj) {
    auto arr = arrow::py::unwrap_array(obj.ptr()).ValueOrDie();
    return arr;
}

std::shared_ptr<arrow::Array> CastArray(
    const std::shared_ptr<arrow::Array>& arr,
    const std::shared_ptr<arrow::DataType>& type) {
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Unsafe(type);
    auto res = arrow::compute::Cast(arr, options);
    if (!res.ok()) {
        throw std::runtime_error(res.status().ToString());
    }
    return res->make_array();
}

}  // namespace

// Semantics enum mapping (must match Python):
// 0 = column_infer, 1 = row_infer, 2 = numeric, 3 = string
py::list compare_columns(
    const std::vector<py::object>& left_arrays,
    const std::vector<py::object>& right_arrays,
    const std::vector<double>& tolerances,
    const std::vector<int>& semantics,
    const std::vector<bool>& has_non_numeric) {
    if (left_arrays.size() != right_arrays.size()) {
        throw std::runtime_error("left_arrays/right_arrays size mismatch");
    }
    const size_t ncols = left_arrays.size();
    if (tolerances.size() != ncols || semantics.size() != ncols) {
        throw std::runtime_error("tolerances/semantics size mismatch");
    }
    if (!has_non_numeric.empty() && has_non_numeric.size() != ncols) {
        throw std::runtime_error("has_non_numeric size mismatch");
    }

    std::vector<std::shared_ptr<arrow::Array>> left_vec;
    std::vector<std::shared_ptr<arrow::Array>> right_vec;
    left_vec.reserve(ncols);
    right_vec.reserve(ncols);
    for (size_t i = 0; i < ncols; ++i) {
        left_vec.push_back(ToArray(left_arrays[i]));
        right_vec.push_back(ToArray(right_arrays[i]));
    }

    py::list out;
    out.attr("clear")();
    for (size_t col_idx = 0; col_idx < ncols; ++col_idx) {
        auto left = left_vec[col_idx];
        auto right = right_vec[col_idx];
        if (left->length() != right->length()) {
            throw std::runtime_error("column length mismatch");
        }
        const int64_t n = left->length();
        const double tol = tolerances[col_idx];
        const int sem = semantics[col_idx];
        const bool col_has_non_numeric =
            has_non_numeric.empty() ? false : has_non_numeric[col_idx];

        auto left_num = CastArray(left, arrow::float64());
        auto right_num = CastArray(right, arrow::float64());
        auto left_str = CastArray(left, arrow::utf8());
        auto right_str = CastArray(right, arrow::utf8());

        auto left_num_arr = std::static_pointer_cast<arrow::DoubleArray>(left_num);
        auto right_num_arr = std::static_pointer_cast<arrow::DoubleArray>(right_num);
        auto left_str_arr = std::static_pointer_cast<arrow::StringArray>(left_str);
        auto right_str_arr = std::static_pointer_cast<arrow::StringArray>(right_str);

        arrow::BooleanBuilder builder;
        auto reserve_status = builder.Reserve(n);
        if (!reserve_status.ok()) {
            throw std::runtime_error(reserve_status.ToString());
        }
        for (int64_t i = 0; i < n; ++i) {
            const bool left_null = left->IsNull(i);
            const bool right_null = right->IsNull(i);
            if (left_null && right_null) {
                builder.UnsafeAppend(true);
                continue;
            }
            if (left_null != right_null) {
                builder.UnsafeAppend(false);
                continue;
            }

            const bool left_num_valid = !left_num_arr->IsNull(i);
            const bool right_num_valid = !right_num_arr->IsNull(i);
            const double left_val = left_num_arr->Value(i);
            const double right_val = right_num_arr->Value(i);

            bool match = false;
            if (sem == 3) {  // string
                match = left_str_arr->GetView(i) == right_str_arr->GetView(i);
            } else if (sem == 2) {  // numeric
                if (left_num_valid && right_num_valid) {
                    match = std::fabs(left_val - right_val) <= tol;
                } else {
                    match = false;
                }
            } else if (sem == 0) {  // column_infer
                if (col_has_non_numeric) {
                    match = left_str_arr->GetView(i) == right_str_arr->GetView(i);
                } else if (left_num_valid && right_num_valid) {
                    match = std::fabs(left_val - right_val) <= tol;
                } else {
                    match = false;
                }
            } else {  // row_infer (default)
                if (left_num_valid && right_num_valid) {
                    match = std::fabs(left_val - right_val) <= tol;
                } else {
                    match = left_str_arr->GetView(i) == right_str_arr->GetView(i);
                }
            }
            builder.UnsafeAppend(match);
        }

        std::shared_ptr<arrow::BooleanArray> out_arr;
        if (!builder.Finish(&out_arr).ok()) {
            throw std::runtime_error("Failed to build BooleanArray");
        }
        py::object wrapped = py::reinterpret_steal<py::object>(
            arrow::py::wrap_array(out_arr));
        out.append(wrapped);
    }
    return out;
}

PYBIND11_MODULE(datasentinel_arrow, m) {
    if (arrow::py::import_pyarrow() != 0) {
        throw std::runtime_error("Failed to import pyarrow C++ bindings");
    }
    m.doc() = "DataSentinel Arrow/C++ comparison kernel";
    m.def(
        "compare_columns",
        &compare_columns,
        "Compare Arrow columns and return per-column boolean match arrays."
    );
}
