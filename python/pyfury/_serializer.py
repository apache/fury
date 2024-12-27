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

import datetime
import logging
from abc import ABC, abstractmethod
from typing import Dict, Iterable, Any

from pyfury._fury import (
    NOT_NULL_STRING_FLAG,
    NOT_NULL_PYINT_FLAG,
    NOT_NULL_PYBOOL_FLAG,
)
from pyfury.resolver import NOT_NULL_VALUE_FLAG, NULL_FLAG
from pyfury.type import (
    TypeId,
    is_primitive_type,
)

try:
    import numpy as np
except ImportError:
    np = None

logger = logging.getLogger(__name__)


class Serializer(ABC):
    __slots__ = "fury", "type_", "need_to_write_ref"

    def __init__(self, fury, type_: type):
        self.fury = fury
        self.type_: type = type_
        self.need_to_write_ref = not is_primitive_type(type_)

    def write(self, buffer, value):
        raise NotImplementedError

    def read(self, buffer):
        raise NotImplementedError

    @abstractmethod
    def xwrite(self, buffer, value):
        pass

    @abstractmethod
    def xread(self, buffer):
        pass

    @classmethod
    def support_subclass(cls) -> bool:
        return False


class CrossLanguageCompatibleSerializer(Serializer):
    def __init__(self, fury, type_):
        super().__init__(fury, type_)

    def xwrite(self, buffer, value):
        self.write(buffer, value)

    def xread(self, buffer):
        return self.read(buffer)


class BooleanSerializer(CrossLanguageCompatibleSerializer):
    def write(self, buffer, value):
        buffer.write_bool(value)

    def read(self, buffer):
        return buffer.read_bool()


class ByteSerializer(CrossLanguageCompatibleSerializer):
    def write(self, buffer, value):
        buffer.write_int8(value)

    def read(self, buffer):
        return buffer.read_int8()


class Int16Serializer(CrossLanguageCompatibleSerializer):
    def write(self, buffer, value):
        buffer.write_int16(value)

    def read(self, buffer):
        return buffer.read_int16()


class Int32Serializer(CrossLanguageCompatibleSerializer):
    def write(self, buffer, value):
        buffer.write_varint32(value)

    def read(self, buffer):
        return buffer.read_varint32()


class Int64Serializer(Serializer):
    def xwrite(self, buffer, value):
        buffer.write_varint64(value)

    def xread(self, buffer):
        return buffer.read_varint64()

    def write(self, buffer, value):
        buffer.write_varint64(value)

    def read(self, buffer):
        return buffer.read_varint64()


class DynamicIntSerializer(CrossLanguageCompatibleSerializer):
    def xwrite(self, buffer, value):
        # TODO(chaokunyang) check value range and write type and value
        buffer.write_varuint32(TypeId.INT64)
        buffer.write_varint64(value)

    def xread(self, buffer):
        type_id = buffer.read_varuint32()
        assert type_id == TypeId.INT64, type_id
        return buffer.read_varint64()


class FloatSerializer(CrossLanguageCompatibleSerializer):
    def write(self, buffer, value):
        buffer.write_float(value)

    def read(self, buffer):
        return buffer.read_float()


class DoubleSerializer(CrossLanguageCompatibleSerializer):
    def write(self, buffer, value):
        buffer.write_double(value)

    def read(self, buffer):
        return buffer.read_double()


class DynamicFloatSerializer(CrossLanguageCompatibleSerializer):
    def xwrite(self, buffer, value):
        # TODO(chaokunyang) check value range and write type and value
        buffer.write_varuint32(TypeId.FLOAT64)
        buffer.write_double(value)

    def xread(self, buffer):
        type_id = buffer.read_varuint32()
        assert type_id == TypeId.FLOAT64, type_id
        return buffer.read_double()


class StringSerializer(CrossLanguageCompatibleSerializer):
    def write(self, buffer, value: str):
        buffer.write_string(value)

    def read(self, buffer):
        return buffer.read_string()


_base_date = datetime.date(1970, 1, 1)


class DateSerializer(CrossLanguageCompatibleSerializer):
    def write(self, buffer, value: datetime.date):
        if not isinstance(value, datetime.date):
            raise TypeError(
                "{} should be {} instead of {}".format(
                    value, datetime.date, type(value)
                )
            )
        days = (value - _base_date).days
        buffer.write_int32(days)

    def read(self, buffer):
        days = buffer.read_int32()
        return _base_date + datetime.timedelta(days=days)


class TimestampSerializer(CrossLanguageCompatibleSerializer):
    def write(self, buffer, value: datetime.datetime):
        if not isinstance(value, datetime.datetime):
            raise TypeError(
                "{} should be {} instead of {}".format(value, datetime, type(value))
            )
        # TimestampType represent micro seconds
        timestamp = int(value.timestamp() * 1000000)
        buffer.write_int64(timestamp)

    def read(self, buffer):
        ts = buffer.read_int64() / 1000000
        # TODO support timezone
        return datetime.datetime.fromtimestamp(ts)


class CollectionSerializer(Serializer):
    __slots__ = "class_resolver", "ref_resolver", "elem_serializer"

    def __init__(self, fury, type_, elem_serializer=None):
        super().__init__(fury, type_)
        self.class_resolver = fury.class_resolver
        self.ref_resolver = fury.ref_resolver
        self.elem_serializer = elem_serializer

    def write(self, buffer, value: Iterable[Any]):
        buffer.write_varuint32(len(value))
        for s in value:
            cls = type(s)
            if cls is str:
                buffer.write_int16()
                buffer.write_string(s)
            elif cls is int:
                buffer.write_int16(NOT_NULL_PYINT_FLAG)
                buffer.write_varint64(s)
            elif cls is bool:
                buffer.write_int16(NOT_NULL_PYBOOL_FLAG)
                buffer.write_bool(s)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, s):
                    classinfo = self.class_resolver.get_classinfo(cls)
                    self.class_resolver.write_classinfo(buffer, classinfo)
                    classinfo.serializer.write(buffer, s)

    def read(self, buffer):
        len_ = buffer.read_varuint32()
        collection_ = self.new_instance(self.type_)
        for i in range(len_):
            self.handle_read_elem(self.fury.deserialize_ref(buffer), collection_)
        return collection_

    def new_instance(self, type_):
        # TODO support iterable subclass
        instance = []
        self.fury.ref_resolver.reference(instance)
        return instance

    def handle_read_elem(self, elem, collection_):
        collection_.append(elem)

    def xwrite(self, buffer, value):
        try:
            len_ = len(value)
        except AttributeError:
            value = list(value)
            len_ = len(value)
        buffer.write_varuint32(len_)
        for s in value:
            self.fury.xserialize_ref(buffer, s, serializer=self.elem_serializer)
            len_ += 1

    def xread(self, buffer):
        len_ = buffer.read_varuint32()
        collection_ = self.new_instance(self.type_)
        for i in range(len_):
            self.handle_read_elem(
                self.fury.xdeserialize_ref(buffer, serializer=self.elem_serializer),
                collection_,
            )
        return collection_


class ListSerializer(CollectionSerializer):
    def read(self, buffer):
        len_ = buffer.read_varuint32()
        instance = []
        self.fury.ref_resolver.reference(instance)
        for i in range(len_):
            instance.append(self.fury.deserialize_ref(buffer))
        return instance


class TupleSerializer(CollectionSerializer):
    def read(self, buffer):
        len_ = buffer.read_varuint32()
        collection_ = []
        for i in range(len_):
            collection_.append(self.fury.deserialize_ref(buffer))
        return tuple(collection_)


class StringArraySerializer(ListSerializer):
    def __init__(self, fury, type_):
        super().__init__(fury, type_, StringSerializer(fury, str))


class SetSerializer(CollectionSerializer):
    def new_instance(self, type_):
        instance = set()
        self.fury.ref_resolver.reference(instance)
        return instance

    def handle_read_elem(self, elem, set_: set):
        set_.add(elem)


class MapSerializer(Serializer):
    __slots__ = (
        "class_resolver",
        "ref_resolver",
        "key_serializer",
        "value_serializer",
    )

    def __init__(self, fury, type_, key_serializer=None, value_serializer=None):
        super().__init__(fury, type_)
        self.class_resolver = fury.class_resolver
        self.ref_resolver = fury.ref_resolver
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    def write(self, buffer, value: Dict):
        buffer.write_varuint32(len(value))
        for k, v in value.items():
            key_cls = type(k)
            if key_cls is str:
                buffer.write_int16(NOT_NULL_STRING_FLAG)
                buffer.write_string(k)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, k):
                    classinfo = self.class_resolver.get_classinfo(key_cls)
                    self.class_resolver.write_classinfo(buffer, classinfo)
                    classinfo.serializer.write(buffer, k)
            value_cls = type(v)
            if value_cls is str:
                buffer.write_int16(NOT_NULL_STRING_FLAG)
                buffer.write_string(v)
            elif value_cls is int:
                buffer.write_int16(NOT_NULL_PYINT_FLAG)
                buffer.write_varint64(v)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, v):
                    classinfo = self.class_resolver.get_classinfo(value_cls)
                    self.class_resolver.write_classinfo(buffer, classinfo)
                    classinfo.serializer.write(buffer, v)

    def read(self, buffer):
        len_ = buffer.read_varuint32()
        map_ = self.type_()
        self.fury.ref_resolver.reference(map_)
        for i in range(len_):
            k = self.fury.deserialize_ref(buffer)
            v = self.fury.deserialize_ref(buffer)
            map_[k] = v
        return map_

    def xwrite(self, buffer, value: Dict):
        buffer.write_varuint32(len(value))
        for k, v in value.items():
            self.fury.xserialize_ref(buffer, k, serializer=self.key_serializer)
            self.fury.xserialize_ref(buffer, v, serializer=self.value_serializer)

    def xread(self, buffer):
        len_ = buffer.read_varuint32()
        map_ = {}
        self.fury.ref_resolver.reference(map_)
        for i in range(len_):
            k = self.fury.xdeserialize_ref(buffer, serializer=self.key_serializer)
            v = self.fury.xdeserialize_ref(buffer, serializer=self.value_serializer)
            map_[k] = v
        return map_


SubMapSerializer = MapSerializer


class EnumSerializer(Serializer):
    @classmethod
    def support_subclass(cls) -> bool:
        return True

    def write(self, buffer, value):
        buffer.write_string(value.name)

    def read(self, buffer):
        name = buffer.read_string()
        return getattr(self.type_, name)

    def xwrite(self, buffer, value):
        raise NotImplementedError

    def xread(self, buffer):
        raise NotImplementedError


class SliceSerializer(Serializer):
    def write(self, buffer, value: slice):
        start, stop, step = value.start, value.stop, value.step
        if type(start) is int:
            # TODO support varint128
            buffer.write_int16(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(start)
        else:
            if start is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                self.fury.serialize_nonref(buffer, start)
        if type(stop) is int:
            # TODO support varint128
            buffer.write_int16(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(stop)
        else:
            if stop is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                self.fury.serialize_nonref(buffer, stop)
        if type(step) is int:
            # TODO support varint128
            buffer.write_int16(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(step)
        else:
            if step is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                self.fury.serialize_nonref(buffer, step)

    def read(self, buffer):
        if buffer.read_int8() == NULL_FLAG:
            start = None
        else:
            start = self.fury.deserialize_nonref(buffer)
        if buffer.read_int8() == NULL_FLAG:
            stop = None
        else:
            stop = self.fury.deserialize_nonref(buffer)
        if buffer.read_int8() == NULL_FLAG:
            step = None
        else:
            step = self.fury.deserialize_nonref(buffer)
        return slice(start, stop, step)

    def xwrite(self, buffer, value):
        raise NotImplementedError

    def xread(self, buffer):
        raise NotImplementedError
