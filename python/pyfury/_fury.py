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

import enum
import logging
import os
import sys
import warnings
from dataclasses import dataclass
from typing import Union, Iterable

from pyfury._serializer import (
    Serializer,
    SerializationContext,
    NOT_SUPPORT_CROSS_LANGUAGE,
    BufferObject,
    PYINT_CLASS_ID,
    PYBOOL_CLASS_ID,
    STRING_CLASS_ID,
    NOT_NULL_STRING_FLAG,
    NOT_NULL_PYINT_FLAG,
    NOT_NULL_PYBOOL_FLAG,
    NO_CLASS_ID,
)
from pyfury.buffer import Buffer
from pyfury.lib import mmh3
from pyfury.resolver import (
    MapRefResolver,
    NoRefResolver,
    NULL_FLAG,
    NOT_NULL_VALUE_FLAG,
)
from pyfury.type import FuryType
from pyfury.util import is_little_endian, set_bit, get_bit, clear_bit

try:
    import numpy as np
except ImportError:
    np = None

from cloudpickle import Pickler

if sys.version_info[:2] < (3, 8):  # pragma: no cover
    from pickle5 import Unpickler
else:
    from pickle import Unpickler

logger = logging.getLogger(__name__)


DEFAULT_DYNAMIC_WRITE_STRING_ID = -1


MAGIC_NUMBER = 0x62D4


class MetaStringBytes:
    __slots__ = (
        "data",
        "length",
        "hashcode",
        "dynamic_write_string_id",
    )

    def __init__(self, data, hashcode=None):
        self.data = data
        self.length = len(data)
        if hashcode is None:
            hashcode = (mmh3.hash_buffer(data, 47)[0] >> 8) << 8
        self.hashcode = hashcode
        self.dynamic_write_string_id = DEFAULT_DYNAMIC_WRITE_STRING_ID

    def __eq__(self, other):
        return type(other) is MetaStringBytes and other.hashcode == self.hashcode

    def __hash__(self):
        return self.hashcode


class ClassInfo:
    __slots__ = (
        "cls",
        "class_id",
        "serializer",
        "namespace_bytes",
        "typename_bytes",
    )

    def __init__(
        self,
        cls: type = None,
        class_id: int = NO_CLASS_ID,
        serializer: Serializer = None,
        namespace_bytes: bytes = None,
        typename_bytes: bytes = None,
    ):
        self.cls = cls
        self.class_id = class_id
        self.serializer = serializer
        self.namespace_bytes = MetaStringBytes(namespace_bytes)
        self.typename_bytes = (
            MetaStringBytes(typename_bytes) if typename_bytes else None
        )

    def __repr__(self):
        return (
            f"ClassInfo(cls={self.cls}, class_id={self.class_id}, "
            f"serializer={self.serializer})"
        )


class Language(enum.Enum):
    XLANG = 0
    JAVA = 1
    PYTHON = 2
    CPP = 3
    GO = 4
    JAVA_SCRIPT = 5
    RUST = 6


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
            self.pickler = Pickler(self.buffer)
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
    def register_type(self, cls, *, class_id: int = None, type_tag: str = None):
        self.class_resolver.register_type(cls, class_id=class_id, type_tag=type_tag)

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
            self.pickler = Pickler(buffer)
        else:
            self.buffer.writer_index = 0
            buffer = self.buffer
        if self.language == Language.XLANG:
            buffer.write_int16(MAGIC_NUMBER)
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
            buffer.write_int16(NOT_NULL_STRING_FLAG)
            buffer.write_string(obj)
            return
        elif cls is int:
            buffer.write_int16(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(obj)
            return
        elif cls is bool:
            buffer.write_int16(NOT_NULL_PYBOOL_FLAG)
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
            buffer.write_varint32(STRING_CLASS_ID << 1)
            buffer.write_string(obj)
            return
        elif cls is int:
            buffer.write_varint32(PYINT_CLASS_ID << 1)
            buffer.write_varint64(obj)
            return
        elif cls is bool:
            buffer.write_varint32(PYBOOL_CLASS_ID << 1)
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
            self.unpickler = Unpickler(buffer)
        if unsupported_objects is not None:
            self._unsupported_objects = iter(unsupported_objects)
        if self.language == Language.XLANG:
            magic_numer = buffer.read_int16()
            assert magic_numer == MAGIC_NUMBER, (
                f"The fury xlang serialization must start with magic number {hex(MAGIC_NUMBER)}. "
                "Please check whether the serialization is based on the xlang protocol and the data didn't corrupt."
            )
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
