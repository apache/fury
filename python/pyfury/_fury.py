import array
import dataclasses
import datetime
import enum
import logging
import os
import sys
import warnings
from dataclasses import dataclass
from typing import Dict, Tuple, TypeVar, Optional, Union, Iterable

from pyfury.lib import mmh3

from pyfury.buffer import Buffer
from pyfury.resolver import (
    MapRefResolver,
    NoRefResolver,
    NULL_FLAG,
    NOT_NULL_VALUE_FLAG,
)
from pyfury._serializer import (
    Serializer,
    SerializationContext,
    NOT_SUPPORT_CROSS_LANGUAGE,
    BufferObject,
    PickleSerializer,
    Numpy1DArraySerializer,
    PyArraySerializer,
    PYINT_CLASS_ID,
    PYFLOAT_CLASS_ID,
    PYBOOL_CLASS_ID,
    STRING_CLASS_ID,
    PICKLE_CLASS_ID,
    USE_CLASSNAME,
    USE_CLASS_ID,
    NOT_NULL_STRING_FLAG,
    NOT_NULL_PYINT_FLAG,
    NOT_NULL_PYBOOL_FLAG,
    NO_CLASS_ID,
    NoneSerializer,
    _PickleStub,
    PickleStrongCacheStub,
    PICKLE_STRONG_CACHE_CLASS_ID,
    PICKLE_CACHE_CLASS_ID,
    PickleCacheStub,
)
from pyfury.type import (
    FuryType,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
    load_class,
)
from pyfury.util import is_little_endian, set_bit, get_bit, clear_bit

try:
    import numpy as np
except ImportError:
    np = None

if sys.version_info[:2] < (3, 8):  # pragma: no cover
    import pickle5 as pickle  # nosec  # pylint: disable=import_pickle
else:
    import pickle  # nosec  # pylint: disable=import_pickle

logger = logging.getLogger(__name__)


DEFAULT_DYNAMIC_WRITE_STRING_ID = -1


class EnumStringBytes:
    __slots__ = (
        "data",
        "length",
        "hashcode",
        "dynamic_write_string_id",
    )

    def __init__(self, data, hashcode=None):
        self.data = data
        self.length = len(data)
        self.hashcode = hashcode or mmh3.hash_buffer(data, 47)[0]
        self.dynamic_write_string_id = DEFAULT_DYNAMIC_WRITE_STRING_ID

    def __eq__(self, other):
        return type(other) is EnumStringBytes and other.hashcode == self.hashcode

    def __hash__(self):
        return self.hashcode


class ClassInfo:
    __slots__ = (
        "cls",
        "class_id",
        "serializer",
        "class_name_bytes",
        "type_tag_bytes",
    )

    def __init__(
        self,
        cls: type = None,
        class_id: int = NO_CLASS_ID,
        serializer: Serializer = None,
        class_name_bytes: bytes = None,
        type_tag_bytes: bytes = None,
    ):
        self.cls = cls
        self.class_id = class_id
        self.serializer = serializer
        self.class_name_bytes = EnumStringBytes(class_name_bytes)
        self.type_tag_bytes = (
            EnumStringBytes(type_tag_bytes) if type_tag_bytes else None
        )

    def __repr__(self):
        return (
            f"ClassInfo(cls={self.cls}, class_id={self.class_id}, "
            f"serializer={self.serializer})"
        )


class ClassResolver:
    __slots__ = (
        "fury",
        "_type_id_to_class",
        "_type_id_to_serializer",
        "_type_id_and_cls_to_serializer",
        "_type_tag_to_class_x_lang_map",
        "_enum_str_to_str",
        "_class_id_counter",
        "_used_classes_id",
        "_classes_info",
        "_registered_id2_class_info",
        "_hash_to_enum_string",
        "_enum_str_to_class",
        "_hash_to_classinfo",
        "_dynamic_id_to_classinfo_list",
        "_dynamic_id_to_enum_str_list",
        "_serializer",
        "_dynamic_write_string_id",
        "_dynamic_written_enum_string",
    )

    _type_id_to_class: Dict[int, type]
    _type_id_to_serializer: Dict[int, Serializer]
    _type_id_and_cls_to_serializer: Dict[Tuple[int, type], Serializer]
    _classes_info: Dict[type, "ClassInfo"]

    def __init__(self, fury):
        self.fury = fury
        self._type_id_to_class = dict()
        self._type_id_to_serializer = dict()
        self._type_id_and_cls_to_serializer = dict()
        self._type_tag_to_class_x_lang_map = dict()
        self._class_id_counter = PICKLE_CACHE_CLASS_ID + 1
        self._used_classes_id = set()

        self._classes_info = dict()
        self._registered_id2_class_info = []

        self._enum_str_to_str = dict()
        self._enum_str_to_class = dict()
        self._hash_to_enum_string = dict()
        self._hash_to_classinfo = dict()
        self._dynamic_id_to_classinfo_list = list()
        self._dynamic_id_to_enum_str_list = list()

        self._serializer = None
        self._dynamic_write_string_id = 0
        self._dynamic_written_enum_string = []

    def initialize(self):
        self.register_class(int, PYINT_CLASS_ID)
        self.register_class(float, PYFLOAT_CLASS_ID)
        self.register_class(bool, PYBOOL_CLASS_ID)
        self.register_class(str, STRING_CLASS_ID)
        self.register_class(_PickleStub, PICKLE_CLASS_ID)
        self.register_class(PickleStrongCacheStub, PICKLE_STRONG_CACHE_CLASS_ID)
        self.register_class(PickleCacheStub, PICKLE_CACHE_CLASS_ID)
        self._add_default_serializers()

    # `Union[type, TypeVar]` is not supported in py3.6
    def register_serializer(self, cls, serializer):
        assert isinstance(cls, (type, TypeVar)), cls
        type_id = serializer.get_xtype_id()
        if type_id != NOT_SUPPORT_CROSS_LANGUAGE:
            self._add_x_lang_serializer(cls, serializer=serializer)
        else:
            self.register_class(cls)
            self._classes_info[cls].serializer = serializer

    # `Union[type, TypeVar]` is not supported in py3.6
    def register_class(self, cls, class_id: int = None):
        classinfo = self._classes_info.get(cls)
        if classinfo is None:
            if isinstance(cls, TypeVar):
                class_name_bytes = (cls.__module__ + "#" + cls.__name__).encode("utf-8")
            else:
                class_name_bytes = (cls.__module__ + "#" + cls.__qualname__).encode(
                    "utf-8"
                )
            class_id = class_id if class_id is not None else self._next_class_id()
            assert class_id not in self._used_classes_id, (
                self._used_classes_id,
                self._classes_info,
            )
            classinfo = ClassInfo(
                cls=cls, class_name_bytes=class_name_bytes, class_id=class_id
            )
            self._classes_info[cls] = classinfo
            if len(self._registered_id2_class_info) <= class_id:
                self._registered_id2_class_info.extend(
                    [None] * (class_id - len(self._registered_id2_class_info) + 1)
                )
            self._registered_id2_class_info[class_id] = classinfo
        else:
            if classinfo.class_id == NO_CLASS_ID:
                class_id = class_id if class_id is not None else self._next_class_id()
                assert class_id not in self._used_classes_id, (
                    self._used_classes_id,
                    self._classes_info,
                )
                classinfo.class_id = class_id
                if len(self._registered_id2_class_info) <= class_id:
                    self._registered_id2_class_info.extend(
                        [None] * (class_id - len(self._registered_id2_class_info) + 1)
                    )
                self._registered_id2_class_info[class_id] = classinfo
            else:
                if class_id is not None and classinfo.class_id != class_id:
                    raise ValueError(
                        f"Inconsistent class id {class_id} vs {classinfo.class_id} "
                        f"for class {cls}"
                    )

    def _next_class_id(self):
        class_id = self._class_id_counter = self._class_id_counter + 1
        while class_id in self._used_classes_id:
            class_id = self._class_id_counter = self._class_id_counter + 1
        return class_id

    def register_class_tag(self, cls: type, type_tag: str = None):
        """Register class with given type tag which will be used for cross-language
        serialization."""
        from pyfury._struct import ComplexObjectSerializer

        self.register_serializer(cls, ComplexObjectSerializer(self.fury, cls, type_tag))

    def _add_serializer(self, cls: type, serializer=None, serializer_cls=None):
        if serializer_cls:
            serializer = serializer_cls(self.fury, cls)
        self.register_serializer(cls, serializer)

    def _add_x_lang_serializer(self, cls: type, serializer=None, serializer_cls=None):
        if serializer_cls:
            serializer = serializer_cls(self.fury, cls)
        type_id = serializer.get_xtype_id()
        from pyfury._serializer import NOT_SUPPORT_CROSS_LANGUAGE

        assert type_id != NOT_SUPPORT_CROSS_LANGUAGE
        self._type_id_and_cls_to_serializer[(type_id, cls)] = serializer
        self.register_class(cls)
        classinfo = self._classes_info[cls]
        classinfo.serializer = serializer
        if type_id == FuryType.FURY_TYPE_TAG.value:
            type_tag = serializer.get_xtype_tag()
            assert type(type_tag) is str
            assert type_tag not in self._type_tag_to_class_x_lang_map
            classinfo.type_tag_bytes = EnumStringBytes(type_tag.encode("utf-8"))
            self._type_tag_to_class_x_lang_map[type_tag] = cls
        else:
            self._type_id_to_serializer[type_id] = serializer
            if type_id > NOT_SUPPORT_CROSS_LANGUAGE:
                self._type_id_to_class[type_id] = cls

    def _add_default_serializers(self):
        import pyfury.serializer as serializers
        from pyfury._serializer import PyArraySerializer, Numpy1DArraySerializer

        self._add_x_lang_serializer(int, serializer_cls=serializers.ByteSerializer)
        self._add_x_lang_serializer(int, serializer_cls=serializers.Int16Serializer)
        self._add_x_lang_serializer(int, serializer_cls=serializers.Int32Serializer)
        self._add_x_lang_serializer(int, serializer_cls=serializers.Int64Serializer)
        self._add_x_lang_serializer(float, serializer_cls=serializers.FloatSerializer)
        self._add_x_lang_serializer(float, serializer_cls=serializers.DoubleSerializer)
        self._add_serializer(type(None), serializer_cls=NoneSerializer)
        self._add_serializer(bool, serializer_cls=serializers.BooleanSerializer)
        self._add_serializer(Int8Type, serializer_cls=serializers.ByteSerializer)
        self._add_serializer(Int16Type, serializer_cls=serializers.Int16Serializer)
        self._add_serializer(Int32Type, serializer_cls=serializers.Int32Serializer)
        self._add_serializer(Int64Type, serializer_cls=serializers.Int64Serializer)
        self._add_serializer(Float32Type, serializer_cls=serializers.FloatSerializer)
        self._add_serializer(Float64Type, serializer_cls=serializers.DoubleSerializer)
        self._add_serializer(str, serializer_cls=serializers.StringSerializer)
        self._add_serializer(datetime.date, serializer_cls=serializers.DateSerializer)
        self._add_serializer(
            datetime.datetime, serializer_cls=serializers.TimestampSerializer
        )
        self._add_serializer(bytes, serializer_cls=serializers.BytesSerializer)
        self._add_serializer(list, serializer_cls=serializers.ListSerializer)
        self._add_serializer(tuple, serializer_cls=serializers.TupleSerializer)
        self._add_serializer(dict, serializer_cls=serializers.MapSerializer)
        self._add_serializer(set, serializer_cls=serializers.SetSerializer)
        self._add_serializer(enum.Enum, serializer_cls=serializers.EnumSerializer)
        self._add_serializer(slice, serializer_cls=serializers.SliceSerializer)
        from pyfury import PickleCacheSerializer, PickleStrongCacheSerializer

        self._add_serializer(
            PickleStrongCacheStub, serializer=PickleStrongCacheSerializer(self.fury)
        )
        self._add_serializer(
            PickleCacheStub, serializer=PickleCacheSerializer(self.fury)
        )
        try:
            import pyarrow as pa
            from pyfury.format.serializer import (
                ArrowRecordBatchSerializer,
                ArrowTableSerializer,
            )

            self._add_serializer(
                pa.RecordBatch, serializer_cls=ArrowRecordBatchSerializer
            )
            self._add_serializer(pa.Table, serializer_cls=ArrowTableSerializer)
        except Exception:
            pass
        for typecode in PyArraySerializer.typecode_dict.keys():
            self._add_serializer(
                array.array,
                serializer=PyArraySerializer(self.fury, array.array, typecode),
            )
            self._add_serializer(
                PyArraySerializer.typecodearray_type[typecode],
                serializer=PyArraySerializer(self.fury, array.array, typecode),
            )
        if np:
            for dtype in Numpy1DArraySerializer.dtypes_dict.keys():
                self._add_serializer(
                    np.ndarray,
                    serializer=Numpy1DArraySerializer(self.fury, array.array, dtype),
                )

    def get_serializer(self, cls: type = None, type_id: int = None, obj=None):
        """
        Returns
        -------
            Returns or create serializer for the provided class
        """
        assert cls is not None or type_id is not None or obj is not None
        if obj is not None:
            cls = type(obj)
            if cls is int and 2**63 - 1 >= obj >= -(2**63):
                type_id = FuryType.INT64.value
            elif cls is float:
                type_id = FuryType.DOUBLE.value
            elif cls is array.array:
                info = PyArraySerializer.typecode_dict.get(obj.typecode)
                if info is not None:
                    type_id = info[1]
            elif np and cls is np.ndarray and obj.ndim == 1:
                info = Numpy1DArraySerializer.dtypes_dict.get(obj.dtype)
                if info:
                    type_id = info[2]
        if type_id is not None:
            if cls is not None:
                serializer_ = self._type_id_and_cls_to_serializer[(type_id, cls)]
            else:
                serializer_ = self._type_id_to_serializer[type_id]
        else:
            class_info = self._classes_info.get(cls)
            if class_info is not None:
                serializer_ = class_info.serializer
            else:
                self._add_serializer(cls, serializer=self.get_or_create_serializer(cls))
                serializer_ = self._classes_info.get(cls).serializer
        self._serializer = serializer_
        return serializer_

    def get_or_create_serializer(self, cls):
        return self.get_or_create_classinfo(cls).serializer

    def get_or_create_classinfo(self, cls):
        class_info = self._classes_info.get(cls)
        if class_info is not None:
            if class_info.serializer is not None:
                return class_info
            else:
                class_info.serializer = self._create_serializer(cls)
                return class_info
        else:
            serializer = self._create_serializer(cls)
            class_id = (
                NO_CLASS_ID
                if type(serializer) is not PickleSerializer
                else PICKLE_CLASS_ID
            )
            class_name_bytes = (cls.__module__ + "#" + cls.__qualname__).encode("utf-8")
            class_info = ClassInfo(
                cls=cls,
                class_name_bytes=class_name_bytes,
                serializer=serializer,
                class_id=class_id,
            )
            self._classes_info[cls] = class_info
            return class_info

    def _create_serializer(self, cls):
        mro = cls.__mro__
        classinfo_ = self._classes_info.get(cls)
        for clz in mro:
            class_info = self._classes_info.get(clz)
            if (
                class_info
                and class_info.serializer
                and class_info.serializer.support_subclass()
            ):
                if classinfo_ is None or classinfo_.class_id == NO_CLASS_ID:
                    logger.info("Class %s not registered", cls)
                serializer = type(class_info.serializer)(self.fury, cls)
                break
        else:
            if dataclasses.is_dataclass(cls):
                if classinfo_ is None or classinfo_.class_id == NO_CLASS_ID:
                    logger.info("Class %s not registered", cls)
                logger.info("Class %s not registered", cls)
                from pyfury import DataClassSerializer

                serializer = DataClassSerializer(self.fury, cls)
            else:
                serializer = PickleSerializer(self.fury, cls)
        return serializer

    def write_classinfo(self, buffer: Buffer, classinfo: ClassInfo):
        class_id = classinfo.class_id
        if class_id != NO_CLASS_ID:
            buffer.write_int16(class_id)
            return
        buffer.write_int16(NO_CLASS_ID)
        self.write_enum_string_bytes(buffer, classinfo.class_name_bytes)

    def read_classinfo(self, buffer):
        class_id = buffer.read_int16()
        if (
            class_id > NO_CLASS_ID
        ):  # registered class id are greater than `NO_CLASS_ID`.
            classinfo = self._registered_id2_class_info[class_id]
            if classinfo.serializer is None:
                classinfo.serializer = self._create_serializer(classinfo.cls)
            return classinfo
        if buffer.read_int8() == USE_CLASS_ID:
            return self._dynamic_id_to_classinfo_list[buffer.read_int16()]
        class_name_bytes_hash = buffer.read_int64()
        class_name_bytes_length = buffer.read_int16()
        reader_index = buffer.reader_index
        buffer.check_bound(reader_index, class_name_bytes_length)
        buffer.reader_index = reader_index + class_name_bytes_length
        classinfo = self._hash_to_classinfo.get(class_name_bytes_hash)
        if classinfo is None:
            classname_bytes = buffer.get_bytes(reader_index, class_name_bytes_length)
            full_class_name = classname_bytes.decode(encoding="utf-8")
            cls = load_class(full_class_name)
            classinfo = self.get_or_create_classinfo(cls)
            self._hash_to_classinfo[class_name_bytes_hash] = classinfo
        self._dynamic_id_to_classinfo_list.append(classinfo)
        return classinfo

    def write_enum_string_bytes(
        self, buffer: Buffer, enum_string_bytes: EnumStringBytes
    ):
        dynamic_write_string_id = enum_string_bytes.dynamic_write_string_id
        if dynamic_write_string_id == DEFAULT_DYNAMIC_WRITE_STRING_ID:
            dynamic_write_string_id = self._dynamic_write_string_id
            enum_string_bytes.dynamic_write_string_id = dynamic_write_string_id
            self._dynamic_write_string_id += 1
            self._dynamic_written_enum_string.append(enum_string_bytes)
            buffer.write_int8(USE_CLASSNAME)
            buffer.write_int64(enum_string_bytes.hashcode)
            buffer.write_int16(enum_string_bytes.length)
            buffer.write_bytes(enum_string_bytes.data)
        else:
            buffer.write_int8(USE_CLASS_ID)
            buffer.write_int16(dynamic_write_string_id)

    def read_enum_string_bytes(self, buffer: Buffer) -> EnumStringBytes:
        if buffer.read_int8() != USE_CLASSNAME:
            return self._dynamic_id_to_enum_str_list[buffer.read_int16()]
        hashcode = buffer.read_int64()
        length = buffer.read_int16()
        reader_index = buffer.reader_index
        buffer.check_bound(reader_index, length)
        buffer.reader_index = reader_index + length
        enum_str = self._hash_to_enum_string.get(hashcode)
        if enum_str is None:
            str_bytes = buffer.get_bytes(reader_index, length)
            enum_str = EnumStringBytes(str_bytes, hashcode=hashcode)
            self._hash_to_enum_string[hashcode] = enum_str
        self._dynamic_id_to_enum_str_list.append(enum_str)
        return enum_str

    def xwrite_class(self, buffer, cls):
        class_name_bytes = self._classes_info[cls].class_name_bytes
        self.write_enum_string_bytes(buffer, class_name_bytes)

    def xwrite_type_tag(self, buffer, cls):
        type_tag_bytes = self._classes_info[cls].type_tag_bytes
        self.write_enum_string_bytes(buffer, type_tag_bytes)

    def read_class_by_type_tag(self, buffer):
        tag = self.xread_classname(buffer)
        return self._type_tag_to_class_x_lang_map[tag]

    def xread_class(self, buffer):
        class_name_bytes = self.read_enum_string_bytes(buffer)
        cls = self._enum_str_to_class.get(class_name_bytes)
        if cls is None:
            full_class_name = class_name_bytes.data.decode(encoding="utf-8")
            cls = load_class(full_class_name)
            self._enum_str_to_class[class_name_bytes] = cls
        return cls

    def xread_classname(self, buffer) -> str:
        str_bytes = self.read_enum_string_bytes(buffer)
        str_ = self._enum_str_to_str.get(str_bytes)
        if str_ is None:
            str_ = str_bytes.data.decode(encoding="utf-8")
            self._enum_str_to_str[str_bytes] = str_
        return str_

    def get_class_by_type_id(self, type_id: int):
        return self._type_id_to_class[type_id]

    def reset(self):
        self.reset_write()
        self.reset_read()

    def reset_read(self):
        self._dynamic_id_to_classinfo_list.clear()
        self._dynamic_id_to_enum_str_list.clear()

    def reset_write(self):
        if self._dynamic_write_string_id != 0:
            self._dynamic_write_string_id = 0
            for enum_str in self._dynamic_written_enum_string:
                enum_str.dynamic_write_string_id = DEFAULT_DYNAMIC_WRITE_STRING_ID
            self._dynamic_written_enum_string.clear()


class Language(enum.Enum):
    XLANG = 0
    JAVA = 1
    PYTHON = 2
    CPP = 3
    GO = 4


@dataclass
class OpaqueObject:
    language: Language
    classname: str
    ordinal: int


class Fury:
    __slots__ = (
        "language",
        "ref_tracking",
        "ref_resolver",
        "class_resolver",
        "serialization_context",
        "require_class_registration",
        "buffer",
        "pickler",
        "unpickler",
        "_buffer_callback",
        "_buffers",
        "_unsupported_callback",
        "_unsupported_objects",
        "_peer_language",
        "_native_objects",
    )
    serialization_context: "SerializationContext"
    unpickler: Optional[pickle.Unpickler]

    def __init__(
        self,
        language=Language.XLANG,
        ref_tracking: bool = False,
        require_class_registration: bool = True,
    ):
        """
        :param require_class_registration:
         Whether to require registering classes for serialization, enabled by default.
          If disabled, unknown insecure classes can be deserialized, which can be
          insecure and cause remote code execution attack if the classes
          `__new__`/`__init__`/`__eq__`/`__hash__` method contain malicious code.
          Do not disable class registration if you can't ensure your environment are
          *indeed secure*. We are not responsible for security risks if
          you disable this option.
        """
        self.language = language
        self.require_class_registration = (
            _ENABLE_CLASS_REGISTRATION_FORCIBLY or require_class_registration
        )
        self.ref_tracking = ref_tracking
        if self.ref_tracking:
            self.ref_resolver = MapRefResolver()
        else:
            self.ref_resolver = NoRefResolver()
        self.class_resolver = ClassResolver(self)
        self.class_resolver.initialize()
        self.serialization_context = SerializationContext()
        self.buffer = Buffer.allocate(32)
        if not require_class_registration:
            warnings.warn(
                "Class registration is disabled, unknown classes can be deserialized "
                "which may be insecure.",
                RuntimeWarning,
                stacklevel=2,
            )
            self.pickler = pickle.Pickler(self.buffer)
        else:
            self.pickler = _PicklerStub(self.buffer)
        self.unpickler = None
        self._buffer_callback = None
        self._buffers = None
        self._unsupported_callback = None
        self._unsupported_objects = None
        self._peer_language = None
        self._native_objects = []

    def register_serializer(self, cls: type, serializer):
        self.class_resolver.register_serializer(cls, serializer)

    # `Union[type, TypeVar]` is not supported in py3.6
    def register_class(self, cls, class_id: int = None):
        self.class_resolver.register_class(cls, class_id=class_id)

    def register_class_tag(self, cls: type, type_tag: str = None):
        self.class_resolver.register_class_tag(cls, type_tag)

    def serialize(
        self,
        obj,
        buffer: Buffer = None,
        buffer_callback=None,
        unsupported_callback=None,
    ) -> Union[Buffer, bytes]:
        try:
            return self._serialize(
                obj,
                buffer,
                buffer_callback=buffer_callback,
                unsupported_callback=unsupported_callback,
            )
        finally:
            self.reset_write()

    def _serialize(
        self,
        obj,
        buffer: Buffer = None,
        buffer_callback=None,
        unsupported_callback=None,
    ) -> Union[Buffer, bytes]:
        self._buffer_callback = buffer_callback
        self._unsupported_callback = unsupported_callback
        if buffer is not None:
            self.pickler = pickle.Pickler(buffer)
        else:
            self.buffer.writer_index = 0
            buffer = self.buffer
        mask_index = buffer.writer_index
        # 1byte used for bit mask
        buffer.grow(1)
        buffer.writer_index = mask_index + 1
        if obj is None:
            set_bit(buffer, mask_index, 0)
        else:
            clear_bit(buffer, mask_index, 0)
        # set endian
        if is_little_endian:
            set_bit(buffer, mask_index, 1)
        else:
            clear_bit(buffer, mask_index, 1)

        if self.language == Language.XLANG:
            # set reader as x_lang.
            set_bit(buffer, mask_index, 2)
            # set writer language.
            buffer.write_int8(Language.PYTHON.value)
        else:
            # set reader as native.
            clear_bit(buffer, mask_index, 2)
        if self._buffer_callback is not None:
            set_bit(buffer, mask_index, 3)
        else:
            clear_bit(buffer, mask_index, 3)
        if self.language == Language.PYTHON:
            self.serialize_ref(buffer, obj)
        else:
            start_offset = buffer.writer_index
            buffer.write_int32(-1)  # preserve 4-byte for nativeObjects start offsets.
            buffer.write_int32(-1)  # preserve 4-byte for nativeObjects size
            self.xserialize_ref(buffer, obj)
            buffer.put_int32(start_offset, buffer.writer_index)
            buffer.put_int32(start_offset + 4, len(self._native_objects))
            self.ref_resolver.reset_write()
            # fury write opaque object classname which cause later write of classname
            # only write an id.
            self.class_resolver.reset_write()
            for native_object in self._native_objects:
                self.serialize_ref(buffer, native_object)
        self.reset_write()
        if buffer is not self.buffer:
            return buffer
        else:
            return buffer.to_bytes(0, buffer.writer_index)

    def serialize_ref(self, buffer, obj, classinfo=None):
        cls = type(obj)
        if cls is str:
            buffer.write_int24(NOT_NULL_STRING_FLAG)
            buffer.write_string(obj)
            return
        elif cls is int:
            buffer.write_int24(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(obj)
            return
        elif cls is bool:
            buffer.write_int24(NOT_NULL_PYBOOL_FLAG)
            buffer.write_bool(obj)
            return
        if self.ref_resolver.write_ref_or_null(buffer, obj):
            return
        if classinfo is None:
            classinfo = self.class_resolver.get_or_create_classinfo(cls)
        self.class_resolver.write_classinfo(buffer, classinfo)
        classinfo.serializer.write(buffer, obj)

    def serialize_nonref(self, buffer, obj):
        cls = type(obj)
        if cls is str:
            buffer.write_int16(STRING_CLASS_ID)
            buffer.write_string(obj)
            return
        elif cls is int:
            buffer.write_int16(PYINT_CLASS_ID)
            buffer.write_varint64(obj)
            return
        elif cls is bool:
            buffer.write_int16(PYBOOL_CLASS_ID)
            buffer.write_bool(obj)
            return
        else:
            classinfo = self.class_resolver.get_or_create_classinfo(cls)
            self.class_resolver.write_classinfo(buffer, classinfo)
            classinfo.serializer.write(buffer, obj)

    def xserialize_ref(self, buffer, obj, serializer=None):
        if serializer is None or serializer.need_to_write_ref:
            if not self.ref_resolver.write_ref_or_null(buffer, obj):
                self.xserialize_nonref(buffer, obj, serializer=serializer)
        else:
            if obj is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                self.xserialize_nonref(buffer, obj, serializer=serializer)

    def xserialize_nonref(self, buffer, obj, serializer=None):
        cls = type(obj)
        serializer = serializer or self.class_resolver.get_serializer(obj=obj)
        type_id = serializer.get_xtype_id()
        buffer.write_int16(type_id)
        if type_id != NOT_SUPPORT_CROSS_LANGUAGE:
            if type_id == FuryType.FURY_TYPE_TAG.value:
                self.class_resolver.xwrite_type_tag(buffer, cls)
            if type_id < NOT_SUPPORT_CROSS_LANGUAGE:
                self.class_resolver.xwrite_class(buffer, cls)
            serializer.xwrite(buffer, obj)
        else:
            # Write classname so it can be used for debugging which object doesn't
            # support cross-language.
            # TODO add a config to disable this to reduce space cost.
            self.class_resolver.xwrite_class(buffer, cls)
            # serializer may increase reference id multi times internally, thus peer
            # cross-language later fields/objects deserialization will use wrong
            # reference id since we skip opaque objects deserialization.
            # So we stash native objects and serialize all those object at the last.
            buffer.write_varint32(len(self._native_objects))
            self._native_objects.append(obj)

    def deserialize(
        self,
        buffer: Union[Buffer, bytes],
        buffers: Iterable = None,
        unsupported_objects: Iterable = None,
    ):
        try:
            return self._deserialize(buffer, buffers, unsupported_objects)
        finally:
            self.reset_read()

    def _deserialize(
        self,
        buffer: Union[Buffer, bytes],
        buffers: Iterable = None,
        unsupported_objects: Iterable = None,
    ):
        if type(buffer) == bytes:
            buffer = Buffer(buffer)
        if self.require_class_registration:
            self.unpickler = _UnpicklerStub(buffer)
        else:
            self.unpickler = pickle.Unpickler(buffer)
        if unsupported_objects is not None:
            self._unsupported_objects = iter(unsupported_objects)
        reader_index = buffer.reader_index
        buffer.reader_index = reader_index + 1
        if get_bit(buffer, reader_index, 0):
            return None
        is_little_endian_ = get_bit(buffer, reader_index, 1)
        assert is_little_endian_, (
            "Big endian is not supported for now, "
            "please ensure peer machine is little endian."
        )
        is_target_x_lang = get_bit(buffer, reader_index, 2)
        if is_target_x_lang:
            self._peer_language = Language(buffer.read_int8())
        else:
            self._peer_language = Language.PYTHON
        is_out_of_band_serialization_enabled = get_bit(buffer, reader_index, 3)
        if is_out_of_band_serialization_enabled:
            assert buffers is not None, (
                "buffers shouldn't be null when the serialized stream is "
                "produced with buffer_callback not null."
            )
            self._buffers = iter(buffers)
        else:
            assert buffers is None, (
                "buffers should be null when the serialized stream is "
                "produced with buffer_callback null."
            )
        if is_target_x_lang:
            native_objects_start_offset = buffer.read_int32()
            native_objects_size = buffer.read_int32()
            if self._peer_language == Language.PYTHON:
                native_objects_buffer = buffer.slice(native_objects_start_offset)
                for i in range(native_objects_size):
                    self._native_objects.append(
                        self.deserialize_ref(native_objects_buffer)
                    )
                self.ref_resolver.reset_read()
                self.class_resolver.reset_read()
            obj = self.xdeserialize_ref(buffer)
        else:
            obj = self.deserialize_ref(buffer)
        return obj

    def deserialize_ref(self, buffer):
        ref_resolver = self.ref_resolver
        ref_id = ref_resolver.try_preserve_ref_id(buffer)
        # indicates that the object is first read.
        if ref_id >= NOT_NULL_VALUE_FLAG:
            classinfo = self.class_resolver.read_classinfo(buffer)
            o = classinfo.serializer.read(buffer)
            ref_resolver.set_read_object(ref_id, o)
            return o
        else:
            return ref_resolver.get_read_object()

    def deserialize_nonref(self, buffer):
        """Deserialize not-null and non-reference object from buffer."""
        classinfo = self.class_resolver.read_classinfo(buffer)
        return classinfo.serializer.read(buffer)

    def xdeserialize_ref(self, buffer, serializer=None):
        if serializer is None or serializer.need_to_write_ref:
            ref_resolver = self.ref_resolver
            red_id = ref_resolver.try_preserve_ref_id(buffer)

            # indicates that the object is first read.
            if red_id >= NOT_NULL_VALUE_FLAG:
                o = self.xdeserialize_nonref(buffer, serializer=serializer)
                ref_resolver.set_read_object(red_id, o)
                return o
            else:
                return ref_resolver.get_read_object()
        head_flag = buffer.read_int8()
        if head_flag == NULL_FLAG:
            return None
        return self.xdeserialize_nonref(buffer, serializer=serializer)

    def xdeserialize_nonref(self, buffer, serializer=None):
        type_id = buffer.read_int16()
        cls = None
        if type_id != NOT_SUPPORT_CROSS_LANGUAGE:
            if type_id == FuryType.FURY_TYPE_TAG.value:
                cls = self.class_resolver.read_class_by_type_tag(buffer)
            if type_id < NOT_SUPPORT_CROSS_LANGUAGE:
                if self._peer_language is not Language.PYTHON:
                    self.class_resolver.read_enum_string_bytes(buffer)
                    cls = self.class_resolver.get_class_by_type_id(-type_id)
                    serializer = serializer or self.class_resolver.get_serializer(
                        type_id=-type_id
                    )
                else:
                    cls = self.class_resolver.xread_class(buffer)
                    serializer = serializer or self.class_resolver.get_serializer(
                        cls=cls, type_id=type_id
                    )
            else:
                if type_id != FuryType.FURY_TYPE_TAG.value:
                    cls = self.class_resolver.get_class_by_type_id(type_id)
                serializer = serializer or self.class_resolver.get_serializer(
                    cls=cls, type_id=type_id
                )
            assert cls is not None
            return serializer.xread(buffer)
        else:
            class_name = self.class_resolver.xread_classname(buffer)
            ordinal = buffer.read_varint32()
            if self._peer_language != Language.PYTHON:
                return OpaqueObject(self._peer_language, class_name, ordinal)
            else:
                return self._native_objects[ordinal]

    def write_buffer_object(self, buffer, buffer_object: BufferObject):
        if self._buffer_callback is None or self._buffer_callback(buffer_object):
            buffer.write_bool(True)
            size = buffer_object.total_bytes()
            # writer length.
            buffer.write_varint32(size)
            writer_index = buffer.writer_index
            buffer.ensure(writer_index + size)
            buf = buffer.slice(buffer.writer_index, size)
            buffer_object.write_to(buf)
            buffer.writer_index += size
        else:
            buffer.write_bool(False)

    def read_buffer_object(self, buffer) -> Buffer:
        in_band = buffer.read_bool()
        if in_band:
            size = buffer.read_varint32()
            buf = buffer.slice(buffer.reader_index, size)
            buffer.reader_index += size
            return buf
        else:
            assert self._buffers is not None
            return next(self._buffers)

    def handle_unsupported_write(self, buffer, obj):
        if self._unsupported_callback is None or self._unsupported_callback(obj):
            buffer.write_bool(True)
            self.pickler.dump(obj)
        else:
            buffer.write_bool(False)

    def handle_unsupported_read(self, buffer):
        in_band = buffer.read_bool()
        if in_band:
            return self.unpickler.load()
        else:
            assert self._unsupported_objects is not None
            return next(self._unsupported_objects)

    def write_ref_pyobject(self, buffer, value, classinfo=None):
        if self.ref_resolver.write_ref_or_null(buffer, value):
            return
        if classinfo is None:
            classinfo = self.class_resolver.get_or_create_classinfo(type(value))
        self.class_resolver.write_classinfo(buffer, classinfo)
        classinfo.serializer.write(buffer, value)

    def read_ref_pyobject(self, buffer):
        return self.deserialize_ref(buffer)

    def reset_write(self):
        self.ref_resolver.reset_write()
        self.class_resolver.reset_write()
        self.serialization_context.reset()
        self._native_objects.clear()
        self.pickler.clear_memo()
        self._buffer_callback = None
        self._unsupported_callback = None

    def reset_read(self):
        self.ref_resolver.reset_read()
        self.class_resolver.reset_read()
        self.serialization_context.reset()
        self._native_objects.clear()
        self.unpickler = None
        self._buffers = None
        self._unsupported_objects = None

    def reset(self):
        self.reset_write()
        self.reset_read()


_ENABLE_CLASS_REGISTRATION_FORCIBLY = os.getenv(
    "ENABLE_CLASS_REGISTRATION_FORCIBLY", "0"
) in {
    "1",
    "true",
}


class _PicklerStub:
    def __init__(self, buf):
        self.buf = buf

    def dump(self, o):
        raise ValueError(
            f"Class {type(o)} is not registered, "
            f"pickle is not allowed when class registration enabled, Please register"
            f"the class or pass unsupported_callback"
        )

    def clear_memo(self):
        pass


class _UnpicklerStub:
    def __init__(self, buf):
        self.buf = buf

    def load(self):
        raise ValueError(
            "pickle is not allowed when class registration enabled, Please register"
            "the class or pass unsupported_callback"
        )
