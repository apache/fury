import os
import sys
import warnings

try:
    import pyarrow as pa

    # Ensure fury can link to arrow shared library
    sys.path.extend(pa.get_library_dirs() + [os.path.dirname(__file__)])

    from pyfury.format._format import (  # noqa: F401 # pylint: disable=unused-import
        create_row_encoder,
        RowData,
        ArrowWriter,
    )  # noqa: E402
    from pyfury.format.infer import (  # noqa: F401 # pylint: disable=unused-import
        get_cls_by_schema,
        remove_schema,
        reset,
    )
    from pyfury.format.encoder import (  # noqa: F401 # pylint: disable=unused-import
        encoder,
        Encoder,
    )
except (ImportError, AttributeError) as e:
    warnings.warn(
        f"Fury format initialization failed, please ensure pyarrow is installed "
        f"with version which fury is compiled with: {e}",
        RuntimeWarning,
        stacklevel=2,
    )
