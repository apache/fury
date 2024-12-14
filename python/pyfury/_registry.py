import array
import dataclasses
import datetime
import enum
import functools
import logging
import sys
from typing import Dict, Tuple, TypeVar

from pyfury.lib import mmh3
from pyfury.serializer import (
    Serializer,
    NOT_SUPPORT_CROSS_LANGUAGE,
    EnumSerializer,
    PickleSerializer,
    Numpy1DArraySerializer,
    NDArraySerializer,
    PyArraySerializer,
    DynamicPyArraySerializer,
    PYINT_CLASS_ID,
    PYFLOAT_CLASS_ID,
    PYBOOL_CLASS_ID,
    STRING_CLASS_ID,
    PICKLE_CLASS_ID,
    NO_CLASS_ID,
    NoneSerializer,
    PickleStrongCacheStub,
    PICKLE_STRONG_CACHE_CLASS_ID,
    PICKLE_CACHE_CLASS_ID,
    PickleCacheStub,
    SMALL_STRING_THRESHOLD,
)
from pyfury._struct import ComplexObjectSerializer
from pyfury.buffer import Buffer
from pyfury.meta.metastring import Encoding, MetaStringEncoder, MetaStringDecoder
from pyfury.type import (
    TypeId,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
    load_class,
)

try:
    import numpy as np
except ImportError:
    np = None


logger = logging.getLogger(__name__)


DEFAULT_DYNAMIC_WRITE_STRING_ID = -1
DYNAMIC_TYPE_ID = -1
USE_CLASSNAME = 0
USE_CLASS_ID = 1
# preserve 0 as flag for class id not set in ClassInfo`
NO_CLASS_ID = 0
PYINT_CLASS_ID = 1
PYFLOAT_CLASS_ID = 2
PYBOOL_CLASS_ID = 3
STRING_CLASS_ID = 4
PICKLE_CLASS_ID = 5
PICKLE_STRONG_CACHE_CLASS_ID = 6
PICKLE_CACHE_CLASS_ID = 7
# `NOT_NULL_VALUE_FLAG` + `CLASS_ID << 1` in little-endian order
NOT_NULL_PYINT_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | (PYINT_CLASS_ID << 9)
NOT_NULL_PYFLOAT_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | (PYFLOAT_CLASS_ID << 9)
NOT_NULL_PYBOOL_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | (PYBOOL_CLASS_ID << 9)
NOT_NULL_STRING_FLAG = NOT_NULL_VALUE_FLANOT_NULL_STRING_FLAGG & 0b11111111 | (
    STRING_CLASS_ID << 9
)
SMALL_STRING_THRESHOLD = 16


class ClassInfo:
    __slots__ = (
        "cls",
        "class_id",
        "serializer",
        "namespace_bytes",
        "typename_bytes",
        "dynamic_type",
    )

    def __init__(
        self,
        cls: type = None,
        class_id: int = NO_CLASS_ID,
        serializer: Serializer = None,
        namespace_bytes=None,
        typename_bytes=None,
        dynamic_type: bool = False,
    ):
        self.cls = cls
        self.class_id = class_id
        self.serializer = serializer
        self.namespace_bytes = namespace_bytes
        self.typename_bytes = typename_bytes
        self.dynamic_type = dynamic_type

    def __repr__(self):
        return (
            f"ClassInfo(cls={self.cls}, class_id={self.class_id}, "
            f"serializer={self.serializer})"
        )


class ClassResolver:
    __slots__ = (
        "fury",
        "_type_tag_to_class_x_lang_map",
        "_metastr_to_str",
        "_type_id_counter",
        "_classes_info",
        "_hash_to_metastring",
        "_metastr_to_class",
        "_hash_to_classinfo",
        "_dynamic_id_to_classinfo_list",
        "_dynamic_id_to_metastr_list",
        "_dynamic_write_string_id",
        "_dynamic_written_metastr",
        "_ns_type_to_classinfo",
        "_namespace_encoder",
        "_namespace_decoder",
        "_typename_encoder",
        "_typename_decoder",
        "require_registration",
    )

    def __init__(self, fury):
        self.fury = fury
        self.metastring_resolver = fury.metastring_resolver
        self.language = fury.language
        self.require_registration = fury.require_class_registration
        self._metastr_to_str = dict()
        self._metastr_to_class = dict()
        self._hash_to_metastring = dict()
        self._hash_to_classinfo = dict()
        self._dynamic_written_metastr = []
        self._type_id_to_classinfo = dict()
        self._type_id_counter = PICKLE_CACHE_CLASS_ID + 1
        self._dynamic_write_string_id = 0
        # hold objects to avoid gc, since `flat_hash_map/vector` doesn't
        # hold python reference.
        self._classes_info = dict()
        self._ns_type_to_classinfo = dict()
        self._namespace_encoder = MetaStringEncoder(".", "_")
        self._namespace_decoder = MetaStringDecoder(".", "_")
        self._typename_encoder = MetaStringEncoder("$", "_")
        self._typename_decoder = MetaStringDecoder("$", "_")

        from pyfury import MetaStringResolver

        self._meta_string_resolver = MetaStringResolver()

    def initialize(self):
        if self.fury.language == Language.PYTHON:
            self._initialize_py()
        else:
            self._initialize_xlang()

    def _initialize_py(self):
        register = functools.partial(self._register_type, internal=True)
        register(int, type_id=PYINT_CLASS_ID, serializer=Int64Serializer)
        register(float, type_id=PYFLOAT_CLASS_ID, serializer=DoubleSerializer)
        register(bool, type_id=PYBOOL_CLASS_ID, serializer=BooleanSerializer)
        register(str, type_id=STRING_CLASS_ID, serializer=StringSerializer)
        from pyfury import (
            PickleCacheSerializer,
            PickleStrongCacheSerializer,
            PickleSerializer,
        )

        register(_PickleStub, type_id=PICKLE_CLASS_ID, serializer=PickleSerializer)
        register(
            PickleStrongCacheStub,
            type_id=PICKLE_STRONG_CACHE_CLASS_ID,
            serializer=PickleStrongCacheSerializer(self.fury),
        )
        register(
            PickleCacheStub,
            type_id=PICKLE_CACHE_CLASS_ID,
            serializer=PickleCacheSerializer(self.fury),
        )
        register(type(None), serializer=NoneSerializer)
        register(Int8Type, serializer=ByteSerializer)
        register(Int16Type, serializer=Int16Serializer)
        register(Int32Type, serializer=Int32Serializer)
        register(Int64Type, serializer=Int64Serializer)
        register(Float32Type, serializer=FloatSerializer)
        register(Float64Type, serializer=DoubleSerializer)
        register(datetime.date, serializer=DateSerializer)
        register(datetime.datetime, serializer=TimestampSerializer)
        register(bytes, serializer=BytesSerializer)
        register(list, serializer=ListSerializer)
        register(tuple, serializer=TupleSerializer)
        register(dict, serializer=MapSerializer)
        register(set, serializer=SetSerializer)
        register(enum.Enum, serializer=EnumSerializer)
        register(slice, serializer=SliceSerializer)
        try:
            import pyarrow as pa
            from pyfury.format.serializer import (
                ArrowRecordBatchSerializer,
                ArrowTableSerializer,
            )

            register(pa.RecordBatch, serializer=ArrowRecordBatchSerializer)
            register(pa.Table, serializer=ArrowTableSerializer)
        except Exception:
            pass
        for size, ftype, type_id in PyArraySerializer.typecode_dict.keys():
            register(ftype, serializer=PyArraySerializer(self.fury, ftype, typecode))
        register(
            array.array, type_id=DYNAMIC_TYPE_ID, serializer=DynamicPyArraySerializer
        )
        if np:
            register(np.ndarray, serializer=NDArraySerializer)

    def _initialize_xlang(self):
        register = functools.partial(self._register_type, internal=True)
        register(bool, type_id=TypeId.BOOL, serializer=BooleanSerializer)
        register(Int8Type, type_id=TypeId.INT8, serializer=ByteSerializer)
        register(Int16Type, type_id=TypeId.INT16, serializer=Int16Serializer)
        register(Int32Type, type_id=TypeId.INT32, serializer=Int32Serializer)
        register(Int64Type, type_id=TypeId.INT64, serializer=Int64Serializer)
        register(int, type_id=DYNAMIC_TYPE_ID, serializer=DynamicIntSerializer)
        register(
            Float32Type,
            type_id=TypeId.FLOAT32,
            serializer=FloatSerializer,
        )
        register(
            Float64Type,
            type_id=TypeId.FLOAT64,
            serializer=FloatSerializer,
        )
        register(float, type_id=DYNAMIC_TYPE_ID, serializer=DynamicFloatSerializer)
        register(str, type_id=TypeId.STRING, serializer=StringSerializer)
        # TODO(chaokunyang) DURATION DECIMAL
        register(
            datetime.datetime, type_id=TypeId.TIMESTAMP, serializer=TimestampSerializer
        )
        register(datetime.date, type_id=TypeId.LOCAL_DATE, serializer=DateSerializer)
        register(bytes, type_id=TypeId.BINARY, serializer=BytesSerializer)
        for itemsize, ftype, typeid in PyArraySerializer.typecode_dict.values():
            register(
                ftype,
                type_id=typeid,
                serializer=PyArraySerializer(self.fury, ftype, typecode),
            )
        register(
            array.array, type_id=DYNAMIC_TYPE_ID, serializer=DynamicPyArraySerializer
        )
        if np:
            # overwrite pyarray  with same type id.
            # if pyarray are needed, one must annotate that value with XXXArrayType
            # as a field of a struct.
            for (
                itemsize,
                format,
                ftype,
                typeid,
            ) in Numpy1DArraySerializer.dtypes_dict.values():
                register(
                    ftype,
                    type_id=typeid,
                    serializer=Numpy1DArraySerializer(self.fury, np.ndarray, dtype),
                )
            register(np.ndarray, type_id=DYNAMIC_TYPE_ID, serializer=NDArraySerializer)
        register(list, type_id=TypeId.LIST, serializer=ListSerializer)
        register(set, type_id=TypeId.SET, serializer=SetSerializer)
        register(dict, type_id=TypeId.MAP, serializer=MapSerializer)

    def register_type(
        self,
        cls: Union[type, TypeVar],
        *,
        type_id: int = None,
        namespace: str = None,
        typename: str = None,
        serializer=None,
    ):
        self._register_type(
            cls,
            type_id=type_id,
            namespace=namespace,
            typename=typename,
            serializer=serializer,
        )

    def _register_type(
        self,
        cls: Union[type, TypeVar],
        *,
        type_id: int = None,
        namespace: str = None,
        typename: str = None,
        serializer=None,
        internal=False,
    ):
        """Register class with given type id or typename. If typename is not None, it will be used for
        cross-language serialization."""
        if serializer is not None and not isinstance(serializer, Serializer):
            serializer = Serializer(self.fury, cls)
        n_params = len(set(typename, type_id))
        if n_params == 0:
            type_id = self._next_type_id()
        if n_params == 2:
            raise TypeError(
                f"type name {typename} and id {type_id} should not be set at the same time"
            )
        if type_id not in {0, None}:
            if type_id in self._type_id_to_classinfo:
                raise TypeError(f"{cls} registered already")
        elif cls in self._classes_info:
            raise TypeError(f"{cls} registered already")
        register_type = (
            self._register_xtype
            if self.fury.language == Language.XLANG
            else self._register_pytype
        )
        register_type(
            cls,
            type_id=type_id,
            namespace=namespace,
            typename=typename,
            serializer=serializer,
            internal=internal,
        )

    def _register_xtype(
        self,
        cls: Union[type, TypeVar],
        *,
        type_id: int = None,
        namespace: str = None,
        typename: str = None,
        serializer=None,
        internal=False,
    ):
        if serializer is None:
            if issubclass(cls, enum.Enum):
                serializer = EnumSerializer(self.fury, cls)
                type_id = (
                    TypeId.NS_ENUM if type_id is None else (type_id << 8 + TypeId.ENUM)
                )
            else:
                serializer = ComplexObjectSerializer(self.fury, cls)
                type_id = (
                    TypeId.NS_STRUCT
                    if type_id is None
                    else (type_id << 8 + TypeId.STRUCT)
                )
        elif not internal:
            type_id = TypeId.NS_EXT if type_id is None else (type_id << 8 + TypeId.EXT)
        self.__register_type(
            cls,
            type_id=type_id,
            serializer=serializer,
            namespace=namespace,
            typename=typename,
            internal=internal,
        )

    def _register_pytype(
        self,
        cls: Union[type, TypeVar],
        *,
        type_id: int = None,
        namespace: str = None,
        typename: str = None,
        serializer: Serializer = None,
        internal: bool = False,
    ):
        self.__register_type(
            cls,
            type_id=type_id,
            namespace=namespace,
            typename=typename,
            serializer=serializer,
            internal=internal,
        )

    def __register_type(
        self,
        cls: Union[type, TypeVar],
        *,
        type_id: int = None,
        namespace: str = None,
        typename: str = None,
        serializer: Serializer = None,
        internal: bool = False,
    ):
        if serializer is None:
            serializer = self._create_serializer(cls)
        if typename is None:
            classinfo = ClassInfo(cls, type_id, serializer, None, None)
        else:
            ns_metastr = self._namespace_encoder.encode(namespace)
            ns_meta_bytes = _create_metastr_bytes(ns_metastr)
            type_metastr = self._typename_encoder.encode(typename)
            type_meta_bytes = _create_metastr_bytes(type_metastr)
            classinfo = ClassInfo(
                cls, type_id, serializer, ns_meta_bytes, type_meta_bytes
            )
            self._ns_type_to_classinfo[(ns_meta_bytes, type_meta_bytes)] = classinfo
        self._classes_info[cls] = classinfo
        if type_id > 0:
            self._type_id_to_classinfo[type_id] = classinfo
        self._classes_info[cls] = classinfo
        return classinfo

    def _next_type_id(self):
        type_id = self._type_id_counter = self._type_id_counter + 1
        while type_id in self._type_id_to_classinfo:
            type_id = self._type_id_counter = self._type_id_counter + 1
        return type_id

    def register_serializer(self, cls: Union[type, TypeVar], serializer):
        assert isinstance(cls, (type, TypeVar)), cls
        if cls not in self._classes_info:
            raise TypeUnregisteredError(f"{cls} not registered")
        classinfo = self._classes_info[cls]
        if self.fury.language == Language.PYTHON:
            classinfo.serializer = serializer
            return
        type_id = prev_type_id = classinfo.type_id
        self._type_id_to_classinfo.pop(prev_type_id)
        if classinfo.serializer is not serializer:
            if classinfo.typename_bytes is not None:
                type_id = classinfo.type_id & 0xFFFFFF00 | TypeId.NS_EXT
            else:
                type_id = classinfo.type_id & 0xFFFFFF00 | TypeId.EXT
        self._type_id_to_classinfo[type_id] = classinfo

    def get_serializer(self, cls: type):
        """
        Returns
        -------
            Returns or create serializer for the provided class
        """
        return self.get_classinfo(cls).serializer

    def get_classinfo(self, cls):
        class_info = self._classes_info.get(cls)
        if class_info is not None:
            if class_info.serializer is None:
                class_info.serializer = self._create_serializer(cls)
            return class_info
        if self.language != Language.PYTHON or self.require_registration:
            raise TypeError(f"{cls} not registered")
        logger.info("Class %s not registered", cls)
        serializer = self._create_serializer(cls)
        type_id = (
            NO_CLASS_ID if type(serializer) is not PickleSerializer else PICKLE_CLASS_ID
        )
        return self.__register_type(
            cls,
            type_id=type_id,
            namespace=cls.__module__,
            typename=cls.__qualname__,
            serializer=serializer,
        )

    def _create_serializer(self, cls):
        classinfo_ = self._classes_info.get(cls)
        for clz in cls.__mro__:
            class_info = self._classes_info.get(clz)
            if (
                class_info
                and class_info.serializer
                and class_info.serializer.support_subclass()
            ):
                serializer = type(class_info.serializer)(self.fury, cls)
                break
        else:
            if dataclasses.is_dataclass(cls):
                from pyfury import DataClassSerializer

                serializer = DataClassSerializer(self.fury, cls)
            elif issubclass(cls, enum.Enum):
                serializer = EnumSerializer(self.fury, cls)
            else:
                serializer = PickleSerializer(self.fury, cls)
        return serializer

    def write_classinfo(self, buffer: Buffer, classinfo):
        class_id = classinfo.class_id
        if class_id != NO_CLASS_ID:
            buffer.write_varuint32(class_id << 1)
            return
        buffer.write_varuint32(1)
        self._meta_string_resolver.write_meta_string_bytes(
            buffer, classinfo.namespace_bytes
        )
        self._meta_string_resolver.write_meta_string_bytes(
            buffer, classinfo.typename_bytes
        )

    def read_classinfo(self, buffer):
        header = buffer.read_varuint32()
        if header & 0b1 == 0:
            class_id = header >> 1
            classinfo = self._type_id_to_classinfo[class_id]
            if classinfo.serializer is None:
                classinfo.serializer = self._create_serializer(classinfo.cls)
            return classinfo
        ns_metabytes = self._meta_string_resolver.read_meta_string_bytes(buffer)
        type_metabytes = self._meta_string_resolver.read_meta_string_bytes(buffer)
        typeinfo = self._ns_type_to_classinfo.get((ns_metabytes, type_metabytes))
        if typeinfo is None:
            typeinfo = self._load_metabytes_to_classinfo(ns_metabytes, type_metabytes)
        return classinfo

    def _load_metabytes_to_classinfo(self, ns_metabytes, typename_metabytes):
        if typeinfo is None:
            ns = ns_metabytes.decode(self._namespace_decoder)
            typename = type_metabytes.decode(self._typename_decoder)
            cls = load_class(ns + "#" + typename)
            classinfo = self.get_classinfo(cls)
            self._ns_type_to_classinfo[(ns_metabytes, type_metabytes)] = classinfo
        return typeinfo

    def xwrite_typeinfo(self, buffer, classinfo):
        type_id = classinfo.type_id
        internal_type_id = type_id & 0xFF
        buffer.write_varuint32(type_id)
        if TypeId.is_namespaced_type(internal_type_id):
            self._meta_string_resolver.write_meta_string_bytes(
                buffer, classinfo.namespace_bytes
            )
            self._meta_string_resolver.write_meta_string_bytes(
                buffer, classinfo.typename_bytes
            )

    def xread_typeinfo(self, buffer):
        type_id = buffer.read_varuint32()
        internal_type_id = type_id & 0xFF
        if TypeId.is_namespaced_type(internal_type_id):
            ns_metabytes = self._meta_string_resolver.read_meta_string_bytes(buffer)
            type_metabytes = self._meta_string_resolver.read_meta_string_bytes(buffer)
            typeinfo = self._ns_type_to_classinfo.get((ns_metabytes, type_metabytes))
            if typeinfo is None:
                ns = ns_metabytes.decode(self._namespace_decoder)
                typename = type_metabytes.decode(self._typename_decoder)
                # TODO(chaokunyang) generate a dynamic class and serializer
                #  when meta share is enabled.
                raise TypeUnregisteredError(f"{ns}.{typename} not registered")
            return typeinfo
        else:
            return self._type_id_to_classinfo[type_id]

    def reset(self):
        pass

    def reset_read(self):
        pass

    def reset_write(self):
        pass


def _create_metastr_bytes(metastr):
    value_hash = mmh3.hash_buffer(metastr.encoded_data, seed=47)[0]
    value_hash &= 0xFFFFFFFFFFFFFF00
    header = metastr.encoding.value & 0xFF
    value_hash |= header
    return MetaStringBytes(metastr.encoded_data, value_hash)
