from __future__ import annotations

from setuptools import setup
from pybind11.setup_helpers import Pybind11Extension, build_ext


try:
    import pyarrow
except ImportError as exc:
    raise SystemExit(
        "pyarrow is required to build the datasentinel_arrow extension."
    ) from exc


def _as_list(value):
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _arrow_lib_dirs():
    dirs = _as_list(pyarrow.get_library_dirs())
    try:
        import os
        base = pyarrow.__path__[0]
        libs = os.path.join(base, ".libs")
        if os.path.isdir(libs) and libs not in dirs:
            dirs.append(libs)
    except Exception:
        pass
    return dirs


def _arrow_extra_objects(library_dirs, libraries):
    extra = []
    if "arrow" in libraries:
        import glob
        libarrow = []
        for d in library_dirs:
            libarrow.extend(glob.glob(f"{d}/libarrow.so"))
            libarrow.extend(glob.glob(f"{d}/libarrow.so.*"))
        if not libarrow:
            return extra, libraries
        if not any(path.endswith("libarrow.so") for path in libarrow):
            libraries = [lib for lib in libraries if lib != "arrow"]
            extra.append(sorted(libarrow)[-1])
    return extra, libraries


ext_modules = [
    Pybind11Extension(
        "datasentinel_arrow",
        ["cpp/arrow_kernel.cpp"],
        cxx_std=17,
        include_dirs=_as_list(pyarrow.get_include()),
        library_dirs=_arrow_lib_dirs(),
        libraries=_as_list(pyarrow.get_libraries()),
    )
]

lib_dirs = _arrow_lib_dirs()
extra, libs = _arrow_extra_objects(lib_dirs, _as_list(pyarrow.get_libraries()))
ext_modules[0].library_dirs = lib_dirs
ext_modules[0].runtime_library_dirs = lib_dirs
ext_modules[0].extra_link_args = [f"-Wl,-rpath,{d}" for d in lib_dirs]
ext_modules[0].libraries = libs
if extra:
    ext_modules[0].extra_objects = extra


setup(
    name="datasentinel-arrow",
    version="0.0.0",
    description="Optional Arrow/C++ kernel for DataSentinel (stub)",
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
)
