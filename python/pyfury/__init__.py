from pyfury import lib  # noqa: F401 # pylint: disable=unused-import
from pyfury._fury import (  # noqa: F401 # pylint: disable=unused-import
    Fury,
    Language,
    OpaqueObject,
)

try:
    from pyfury._serialization import ENABLE_FURY_CYTHON_SERIALIZATION
except ImportError:
    ENABLE_FURY_CYTHON_SERIALIZATION = False

if ENABLE_FURY_CYTHON_SERIALIZATION:
    from pyfury._serialization import (  # noqa: F401,F811
        Fury,
        Language,
        ClassInfo,
        OpaqueObject,
        ComplexObjectSerializer,
    )
else:
    from pyfury._fury import (  # noqa: F401,F403,F811 # pylint: disable=unused-import
        Fury,
        Language,
        ClassInfo,
        OpaqueObject,
    )
    from pyfury._struct import (  # noqa: F401,F403,F811 # pylint: disable=unused-import
        ComplexObjectSerializer,
    )
from pyfury.serializer import *  # noqa: F401,F403 # pylint: disable=unused-import
from pyfury.type import (  # noqa: F401 # pylint: disable=unused-import
    record_class_factory,
    get_qualified_classname,
    FuryType,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
    # Int8ArrayType,
    Int16ArrayType,
    Int32ArrayType,
    Int64ArrayType,
    Float32ArrayType,
    Float64ArrayType,
    dataslots,
)
from pyfury._util import Buffer  # noqa: F401 # pylint: disable=unused-import

try:
    from pyfury.format import *  # noqa: F401,F403 # pylint: disable=unused-import
except (AttributeError, ImportError):
    pass

__version__ = "0.1.0.a5"
