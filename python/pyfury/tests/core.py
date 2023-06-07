import pytest

try:
    import pyarrow as pa
except ImportError:
    pa = None


def require_pyarrow(func):
    func = pytest.mark.fury_format(func)
    arrow_installed = False
    if pa is not None and hasattr(pa, "get_library_dirs"):
        arrow_installed = True
    func = pytest.mark.skipif(not arrow_installed, reason="pyarrow not installed")(func)
    return func
