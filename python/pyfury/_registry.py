import array
import dataclasses
import datetime
import enum
import logging
import sys
from typing import Dict, Tuple, TypeVar

from pyfury.serializer import (
    Serializer,
    NOT_SUPPORT_CROSS_LANGUAGE,
    PickleSerializer,
    Numpy1DArraySerializer,
    PyArraySerializer,
    PYINT_CLASS_ID,
    PYFLOAT_CLASS_ID,
    PYBOOL_CLASS_ID,
    STRING_CLASS_ID,
    PICKLE_CLASS_ID,
    NO_CLASS_ID,
    NoneSerializer,
    _PickleStub,
    PickleStrongCacheStub,
    PICKLE_STRONG_CACHE_CLASS_ID,
    PICKLE_CACHE_CLASS_ID,
    PickleCacheStub,
    SMALL_STRING_THRESHOLD,
)
from pyfury.buffer import Buffer
from pyfury.meta.metastring import Encoding
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

try:
    import numpy as np
except ImportError:
    np = None


logger = logging.getLogger(__name__)


DEFAULT_DYNAMIC_WRITE_STRING_ID = -1


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
        self.register_type(int, class_id=PYINT_CLASS_ID)
        self.register_type(float, class_id=PYFLOAT_CLASS_ID)
        self.register_type(bool, class_id=PYBOOL_CLASS_ID)
        self.register_type(str, class_id=STRING_CLASS_ID)
        self.register_type(_PickleStub, class_id=PICKLE_CLASS_ID)
        self.register_type(
            PickleStrongCacheStub, class_id=PICKLE_STRONG_CACHE_CLASS_ID
        )
        self.register_type(PickleCacheStub, class_id=PICKLE_CACHE_CLASS_ID)
        self._add_default_serializers()

    # `Union[type, TypeVar]` is not supported in py3.6
    def register_serializer(self, cls, serializer):
        assert isinstance(cls, (type, TypeVar)), cls
        type_id = serializer.get_xtype_id()
        if type_id != NOT_SUPPORT_CROSS_LANGUAGE:
            self._add_x_lang_serializer(cls, serializer=serializer)
        else:
            self.register_type(cls)
            self._classes_info[cls].serializer = serializer

    # `Union[type, TypeVar]` is not supported in py3.6
    def register_type(self, cls, *, class_id: int = None, type_tag: str = None):
        """Register class with given type id or tag, if tag is not None, it will be used for
        cross-language serialization."""
        if type_tag is not None:
            assert class_id is None, (
                f"Type tag {type_tag} has been set already, "
                f"set class id at the same time is not allowed."
            )
            from pyfury._struct import ComplexObjectSerializer

            self.register_serializer(
                cls, ComplexObjectSerializer(self.fury, cls, type_tag)
            )
            return
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
        self.register_type(cls)
        classinfo = self._classes_info[cls]
        classinfo.serializer = serializer
        if type_id == FuryType.FURY_TYPE_TAG.value:
            type_tag = serializer.get_xtype_tag()
            assert type(type_tag) is str
            assert type_tag not in self._type_tag_to_class_x_lang_map
            classinfo.type_tag_bytes = MetaStringBytes(type_tag.encode("utf-8"))
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
            buffer.write_varint32(class_id << 1)
            return
        buffer.write_varint32(1)
        self.write_enum_string_bytes(buffer, classinfo.class_name_bytes)

    def read_classinfo(self, buffer):
        header = buffer.read_varint32()
        if header & 0b1 == 0:
            class_id = header >> 1
            classinfo = self._registered_id2_class_info[class_id]
            if classinfo.serializer is None:
                classinfo.serializer = self._create_serializer(classinfo.cls)
            return classinfo
        meta_str_header = buffer.read_varint32()
        length = meta_str_header >> 1
        if meta_str_header & 0b1 != 0:
            return self._dynamic_id_to_classinfo_list[length - 1]
        class_name_bytes_hash = buffer.read_int64()
        reader_index = buffer.reader_index
        buffer.check_bound(reader_index, length)
        buffer.reader_index = reader_index + length
        classinfo = self._hash_to_classinfo.get(class_name_bytes_hash)
        if classinfo is None:
            classname_bytes = buffer.get_bytes(reader_index, length)
            full_class_name = classname_bytes.decode(encoding="utf-8")
            cls = load_class(full_class_name)
            classinfo = self.get_or_create_classinfo(cls)
            self._hash_to_classinfo[class_name_bytes_hash] = classinfo
        self._dynamic_id_to_classinfo_list.append(classinfo)
        return classinfo

    def write_enum_string_bytes(
            self, buffer: Buffer, enum_string_bytes: MetaStringBytes
    ):
        dynamic_write_string_id = enum_string_bytes.dynamic_write_string_id
        if dynamic_write_string_id == DEFAULT_DYNAMIC_WRITE_STRING_ID:
            dynamic_write_string_id = self._dynamic_write_string_id
            enum_string_bytes.dynamic_write_string_id = dynamic_write_string_id
            self._dynamic_write_string_id += 1
            self._dynamic_written_enum_string.append(enum_string_bytes)
            buffer.write_varint32(enum_string_bytes.length << 1)
            if enum_string_bytes.length <= SMALL_STRING_THRESHOLD:
                # TODO(chaokunyang) support meta string encoding
                buffer.write_int8(Encoding.UTF_8.value)
            else:
                buffer.write_int64(enum_string_bytes.hashcode)
            buffer.write_bytes(enum_string_bytes.data)
        else:
            buffer.write_varint32(((dynamic_write_string_id + 1) << 1) | 1)

    def read_enum_string_bytes(self, buffer: Buffer) -> MetaStringBytes:
        header = buffer.read_varint32()
        length = header >> 1
        if header & 0b1 != 0:
            return self._dynamic_id_to_enum_str_list[length - 1]
        if length <= SMALL_STRING_THRESHOLD:
            buffer.read_int8()
            if length <= 8:
                v1 = buffer.read_bytes_as_int64(length)
                v2 = 0
            else:
                v1 = buffer.read_int64()
                v2 = buffer.read_bytes_as_int64(length - 8)
            hashcode = v1 * 31 + v2
            enum_str = self._hash_to_enum_string.get(hashcode)
            if enum_str is None:
                str_bytes = buffer.get_bytes(buffer.reader_index - length, length)
                enum_str = MetaStringBytes(str_bytes, hashcode=hashcode)
                self._hash_to_enum_string[hashcode] = enum_str
        else:
            hashcode = buffer.read_int64()
            reader_index = buffer.reader_index
            buffer.check_bound(reader_index, length)
            buffer.reader_index = reader_index + length
            enum_str = self._hash_to_enum_string.get(hashcode)
            if enum_str is None:
                str_bytes = buffer.get_bytes(reader_index, length)
                enum_str = MetaStringBytes(str_bytes, hashcode=hashcode)
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
