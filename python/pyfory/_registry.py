# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import array
import dataclasses
import datetime
import enum
import functools
import logging
from typing import TypeVar, Union
from enum import Enum

from pyfory._serialization import ENABLE_FORY_CYTHON_SERIALIZATION
from pyfory import Language
from pyfory.error import TypeUnregisteredError
from pyfory.meta.type_info import TypeInfo

from pyfory.serializer import (
    Serializer,
    Numpy1DArraySerializer,
    NDArraySerializer,
    PyArraySerializer,
    DynamicPyArraySerializer,
    _PickleStub,
    PickleStrongCacheStub,
    PickleCacheStub,
    NoneSerializer,
    BooleanSerializer,
    ByteSerializer,
    Int16Serializer,
    Int32Serializer,
    Int64Serializer,
    Float32Serializer,
    Float64Serializer,
    StringSerializer,
    DateSerializer,
    TimestampSerializer,
    BytesSerializer,
    ListSerializer,
    TupleSerializer,
    MapSerializer,
    SetSerializer,
    EnumSerializer,
    SliceSerializer,
    PickleCacheSerializer,
    PickleStrongCacheSerializer,
    PickleSerializer,
    DataClassSerializer,
)
from pyfory._struct import ComplexObjectSerializer
from pyfory.meta.metastring import MetaStringEncoder, MetaStringDecoder
from pyfory.type import (
    TypeId,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
    load_class,
)
from pyfory._fory import (
    DYNAMIC_TYPE_ID,
    # preserve 0 as flag for class id not set in ClassInfo`
    NO_CLASS_ID,
)

from pyfory.buffer import Buffer
from pyfory.meta.meta_share import MetaContext

try:
    import numpy as np
except ImportError:
    np = None

logger = logging.getLogger(__name__)


if ENABLE_FORY_CYTHON_SERIALIZATION:
    from pyfory._serialization import ClassInfo
else:

    class ClassInfo:
        __slots__ = (
            "cls",
            "type_id",
            "serializer",
            "namespace_bytes",
            "typename_bytes",
            "dynamic_type",
            "need_to_write_class_def",
            "type_def"
        )

        def __init__(
            self,
            cls: type = None,
            type_id: int = NO_CLASS_ID,
            serializer: Serializer = None,
            namespace_bytes=None,
            typename_bytes=None,
            dynamic_type: bool = False,
            class_resolver = None,
        ):
            self.cls = cls
            self.type_id = type_id
            self.serializer = serializer
            self.namespace_bytes = namespace_bytes
            self.typename_bytes = typename_bytes
            self.dynamic_type = dynamic_type
            self.need_to_write_class_def = True
            # if class_resolver:
            #     # self.need_to_write_class_def = serializer != None and class_resolver.need_to_write_class_def(serializer)
            #     self.need_to_write_class_def = True
            # else:
            #     self.need_to_write_class_def = True
            self.type_def = None

        def __repr__(self):
            return (
                f"ClassInfo(cls={self.cls}, type_id={self.type_id}, "
                f"serializer={self.serializer})"
            )


class ClassResolver:
    __slots__ = (
        "fory",
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
        "_named_type_to_classinfo",
        "namespace_encoder",
        "namespace_decoder",
        "typename_encoder",
        "typename_decoder",
        "require_registration",
        "metastring_resolver",
        "language",
        "_type_id_to_classinfo",
        "type_def_map"
    )

    def __init__(self, fory):
        self.fory = fory
        self.metastring_resolver = fory.metastring_resolver
        self.language = fory.language
        self.require_registration = fory.require_class_registration
        self._metastr_to_str = dict()
        self._metastr_to_class = dict()
        self._hash_to_metastring = dict()
        self._hash_to_classinfo = dict()
        self._dynamic_written_metastr = []
        self._type_id_to_classinfo = dict()
        self._type_id_counter = 64
        self._dynamic_write_string_id = 0
        # hold objects to avoid gc, since `flat_hash_map/vector` doesn't
        # hold python reference.
        self._classes_info = dict()
        self._ns_type_to_classinfo = dict()
        self._named_type_to_classinfo = dict()
        self.namespace_encoder = MetaStringEncoder(".", "_")
        self.namespace_decoder = MetaStringDecoder(".", "_")
        self.typename_encoder = MetaStringEncoder("$", "_")
        self.typename_decoder = MetaStringDecoder("$", "_")

        self.type_def_map = dict()

    def initialize(self):
        self._initialize_xlang()
        if self.fory.language == Language.PYTHON:
            self._initialize_py()

    def _initialize_py(self):
        register = functools.partial(self._register_type, internal=True)
        register(
            _PickleStub,
            type_id=PickleSerializer.PICKLE_CLASS_ID,
            serializer=PickleSerializer,
        )
        register(
            PickleStrongCacheStub,
            type_id=97,
            serializer=PickleStrongCacheSerializer(self.fory),
        )
        register(
            PickleCacheStub,
            type_id=98,
            serializer=PickleCacheSerializer(self.fory),
        )
        register(type(None), serializer=NoneSerializer)
        register(tuple, serializer=TupleSerializer)
        register(slice, serializer=SliceSerializer)

    def _initialize_xlang(self):
        register = functools.partial(self._register_type, internal=True)
        register(None, type_id=TypeId.NA, serializer=NoneSerializer)
        register(bool, type_id=TypeId.BOOL, serializer=BooleanSerializer)
        register(Int8Type, type_id=TypeId.INT8, serializer=ByteSerializer)
        register(Int16Type, type_id=TypeId.INT16, serializer=Int16Serializer)
        register(Int32Type, type_id=TypeId.INT32, serializer=Int32Serializer)
        register(Int64Type, type_id=TypeId.INT64, serializer=Int64Serializer)
        register(int, type_id=TypeId.INT64, serializer=Int64Serializer)
        register(
            Float32Type,
            type_id=TypeId.FLOAT32,
            serializer=Float32Serializer,
        )
        register(
            Float64Type,
            type_id=TypeId.FLOAT64,
            serializer=Float64Serializer,
        )
        register(float, type_id=TypeId.FLOAT64, serializer=Float64Serializer)
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
                serializer=PyArraySerializer(self.fory, ftype, typeid),
            )
        register(
            array.array, type_id=DYNAMIC_TYPE_ID, serializer=DynamicPyArraySerializer
        )
        if np:
            # overwrite pyarray  with same type id.
            # if pyarray are needed, one must annotate that value with XXXArrayType
            # as a field of a struct.
            for dtype, (
                itemsize,
                format,
                ftype,
                typeid,
            ) in Numpy1DArraySerializer.dtypes_dict.items():
                register(
                    ftype,
                    type_id=typeid,
                    serializer=Numpy1DArraySerializer(self.fory, ftype, dtype),
                )
            register(np.ndarray, type_id=DYNAMIC_TYPE_ID, serializer=NDArraySerializer)
        register(list, type_id=TypeId.LIST, serializer=ListSerializer)
        register(set, type_id=TypeId.SET, serializer=SetSerializer)
        register(dict, type_id=TypeId.MAP, serializer=MapSerializer)
        try:
            import pyarrow as pa
            from pyfory.format.serializer import (
                ArrowRecordBatchSerializer,
                ArrowTableSerializer,
            )

            register(
                pa.RecordBatch,
                type_id=TypeId.ARROW_RECORD_BATCH,
                serializer=ArrowRecordBatchSerializer,
            )
            register(
                pa.Table, type_id=TypeId.ARROW_TABLE, serializer=ArrowTableSerializer
            )
        except Exception:
            pass

    def register_type(
        self,
        cls: Union[type, TypeVar],
        *,
        type_id: int = None,
        namespace: str = None,
        typename: str = None,
        serializer=None,
    ):
        return self._register_type(
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
            try:
                serializer = serializer(self.fory, cls)
            except BaseException:
                try:
                    serializer = serializer(self.fory)
                except BaseException:
                    serializer = serializer()
        n_params = len({typename, type_id, None}) - 1
        if n_params == 0 and typename is None:
            type_id = self._next_type_id()
        if n_params == 2:
            raise TypeError(
                f"type name {typename} and id {type_id} should not be set at the same time"
            )
        if type_id not in {0, None}:
            # multiple class can have same tpe id
            if type_id in self._type_id_to_classinfo and cls in self._classes_info:
                raise TypeError(f"{cls} registered already")
        elif cls in self._classes_info:
            raise TypeError(f"{cls} registered already")
        register_type = (
            self._register_xtype
            if self.fory.language == Language.XLANG
            else self._register_pytype
        )
        return register_type(
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
                serializer = EnumSerializer(self.fory, cls)
                type_id = (
                    TypeId.NAMED_ENUM
                    if type_id is None
                    else ((type_id << 8) + TypeId.ENUM)
                )
            else:
                serializer = ComplexObjectSerializer(self.fory, cls)
                type_id = (
                    TypeId.NAMED_STRUCT
                    if type_id is None
                    else ((type_id << 8) + TypeId.STRUCT)
                )
        elif not internal:
            type_id = (
                TypeId.NAMED_EXT if type_id is None else ((type_id << 8) + TypeId.EXT)
            )
        return self.__register_type(
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
        return self.__register_type(
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
        dynamic_type = type_id < 0
        if not internal and serializer is None:
            serializer = self._create_serializer(cls)
        if typename is None:
            classinfo = ClassInfo(cls, type_id, serializer, None, None, dynamic_type)
        else:
            if namespace is None:
                splits = typename.rsplit(".", 1)
                if len(splits) == 2:
                    namespace, typename = splits
            ns_metastr = self.namespace_encoder.encode(namespace or "")
            ns_meta_bytes = self.metastring_resolver.get_metastr_bytes(ns_metastr)
            type_metastr = self.typename_encoder.encode(typename)
            type_meta_bytes = self.metastring_resolver.get_metastr_bytes(type_metastr)
            classinfo = ClassInfo(
                cls, type_id, serializer, ns_meta_bytes, type_meta_bytes, dynamic_type
            )
            self._named_type_to_classinfo[(namespace, typename)] = classinfo
            self._ns_type_to_classinfo[(ns_meta_bytes, type_meta_bytes)] = classinfo
        self._classes_info[cls] = classinfo
        if type_id > 0 and (
            self.language == Language.PYTHON or not TypeId.is_namespaced_type(type_id)
        ):
            if type_id not in self._type_id_to_classinfo or not internal:
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
        if self.fory.language == Language.PYTHON:
            classinfo.serializer = serializer
            return
        type_id = prev_type_id = classinfo.type_id
        self._type_id_to_classinfo.pop(prev_type_id)
        if classinfo.serializer is not serializer:
            if classinfo.typename_bytes is not None:
                type_id = classinfo.type_id & 0xFFFFFF00 | TypeId.NAMED_EXT
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

    def get_classinfo(self, cls, create=True):
        class_info = self._classes_info.get(cls)
        if class_info is not None:
            if class_info.serializer is None:
                class_info.serializer = self._create_serializer(cls)
            return class_info
        elif not create:
            return None
        if self.language != Language.PYTHON or (
            self.require_registration and not issubclass(cls, Enum)
        ):
            raise TypeUnregisteredError(f"{cls} not registered")
        logger.info("Class %s not registered", cls)
        serializer = self._create_serializer(cls)
        type_id = None
        if self.language == Language.PYTHON:
            if isinstance(serializer, EnumSerializer):
                type_id = TypeId.NAMED_ENUM
            elif type(serializer) is PickleSerializer:
                type_id = PickleSerializer.PICKLE_CLASS_ID
            if not self.require_registration:
                if isinstance(serializer, DataClassSerializer):
                    type_id = TypeId.NAMED_STRUCT
        if type_id is None:
            raise TypeUnregisteredError(
                f"{cls} must be registered using `fory.register_type` API"
            )
        return self.__register_type(
            cls,
            type_id=type_id,
            namespace=cls.__module__,
            typename=cls.__qualname__,
            serializer=serializer,
        )

    def _create_serializer(self, cls):
        for clz in cls.__mro__:
            class_info = self._classes_info.get(clz)
            if (
                class_info
                and class_info.serializer
                and class_info.serializer.support_subclass()
            ):
                serializer = type(class_info.serializer)(self.fory, cls)
                break
        else:
            if dataclasses.is_dataclass(cls):
                from pyfory import DataClassSerializer

                serializer = DataClassSerializer(self.fory, cls)
            elif issubclass(cls, enum.Enum):
                serializer = EnumSerializer(self.fory, cls)
            else:
                serializer = PickleSerializer(self.fory, cls)
        return serializer

    def _load_metabytes_to_classinfo(self, ns_metabytes, type_metabytes):
        typeinfo = self._ns_type_to_classinfo.get((ns_metabytes, type_metabytes))
        if typeinfo is not None:
            return typeinfo
        ns = ns_metabytes.decode(self.namespace_decoder)
        typename = type_metabytes.decode(self.typename_decoder)
        # the hash computed between languages may be different.
        typeinfo = self._named_type_to_classinfo.get((ns, typename))
        if typeinfo is not None:
            self._ns_type_to_classinfo[(ns_metabytes, type_metabytes)] = typeinfo
            return typeinfo
        cls = load_class(ns + "#" + typename)
        classinfo = self.get_classinfo(cls)
        self._ns_type_to_classinfo[(ns_metabytes, type_metabytes)] = classinfo
        return classinfo

    def write_typeinfo(self, buffer, classinfo, meta_share_enabled=False):
        if classinfo.dynamic_type:
            return
        if meta_share_enabled:
            self.__write_typeinfo_with_meta_share(buffer, classinfo)
        else:
            type_id = classinfo.type_id
            internal_type_id = type_id & 0xFF
            buffer.write_varuint32(type_id)
            if TypeId.is_namespaced_type(internal_type_id):
                self.metastring_resolver.write_meta_string_bytes(
                    buffer, classinfo.namespace_bytes
                )
                self.metastring_resolver.write_meta_string_bytes(
                    buffer, classinfo.typename_bytes
                )

    def __write_typeinfo_with_meta_share(self, buffer:Buffer, typeinfo: ClassInfo):
        if typeinfo.type_id != NO_CLASS_ID and not typeinfo.need_to_write_class_def:
            buffer.write_varuint32(typeinfo.type_id << 1)
            return

        meta_context = self.fory.serialization_context.meta_context
        type_map:dict = meta_context.class_map

        if typeinfo.cls in type_map:
            id = type_map[typeinfo.cls]
            buffer.write_varuint32(id << 1 | 0b1)
        else:
            new_id = len(type_map)
            type_map[typeinfo.cls] = new_id
            buffer.write_varuint32(new_id << 1 | 0b1)
            type_def = typeinfo.type_def
            if type_def is None:
                type_def = self.build_type_def(typeinfo)
            meta_context.writing_type_defs.append(type_def)

    def read_typeinfo(self, buffer):
        type_id = buffer.read_varuint32()
        internal_type_id = type_id & 0xFF
        if TypeId.is_namespaced_type(internal_type_id):
            ns_metabytes = self.metastring_resolver.read_meta_string_bytes(buffer)
            type_metabytes = self.metastring_resolver.read_meta_string_bytes(buffer)
            typeinfo = self._ns_type_to_classinfo.get((ns_metabytes, type_metabytes))
            if typeinfo is None:
                ns = ns_metabytes.decode(self.namespace_decoder)
                typename = type_metabytes.decode(self.typename_decoder)
                typeinfo = self._named_type_to_classinfo.get((ns, typename))
                if typeinfo is not None:
                    self._ns_type_to_classinfo[
                        (ns_metabytes, type_metabytes)
                    ] = typeinfo
                    return typeinfo
                # TODO(chaokunyang) generate a dynamic class and serializer
                #  when meta share is enabled.
                name = ns + "." + typename if ns else typename
                raise TypeUnregisteredError(f"{name} not registered")
            return typeinfo
        else:
            return self._type_id_to_classinfo[type_id]

    # TODO
    def write_type_defs(self, buffer:Buffer, meta_context:MetaContext):
        writing_type_defs = meta_context.writing_type_defs
        # TODO writeVarUint32Small7
        # buffer.write_varuint32(len(writing_type_defs))
        for type_def in writing_type_defs:
            pass
            # TODO class TypeDef
           # type_def.writeClassDef(buffer)
        meta_context.writing_type_defs.clear()

    # TODO
    def read_type_defs(self, buffer:Buffer, meta_context:MetaContext):
        # TODO readVarUint32Small7
        # num_class_defs = buffer.read_varuint32()
        num_class_defs = 0
        for i in range(num_class_defs):
            pass
            # id = buffer.read_int64()
            # meta_context.read_type_defs.add()
            # meta_context.read_type_infos.add()

    def reset(self):
        pass

    def reset_read(self):
        pass

    def reset_write(self):
        pass

    def need_to_write_type_def(self, serializer:Serializer):
        raise NotImplementedError()

    def build_type_def(self, type_info:ClassInfo):
        raise NotImplementedError()
        # serializer = type_info.serializer
        # def_map = self.type_def_map
        # type_def = def_map.get(type_info.cls)
        # if not type_def:
        #     if self.need_to_write_type_def(serializer):
        #         type_def = TypeInfo.xread(cls=type_info.cls,is_xlang=True)
        #     else:
        #         pass
        #
        # type_info.type_def = type_def
