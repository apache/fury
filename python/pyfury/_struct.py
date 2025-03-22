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
import typing

from pyfury.buffer import Buffer
from pyfury.error import ClassNotCompatibleError
from pyfury.serializer import (
    ListSerializer,
    MapSerializer,
    PickleSerializer,
    Serializer,
)
from pyfury.type import (
    TypeVisitor,
    infer_field,
    TypeId,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
    is_py_array_type,
    compute_string_hash,
)

logger = logging.getLogger(__name__)


basic_types = {
    bool,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
    int,
    float,
    str,
    bytes,
    datetime.datetime,
    datetime.date,
    datetime.time,
}


class ComplexTypeVisitor(TypeVisitor):
    def __init__(
        self,
        fury,
    ):
        self.fury = fury

    def visit_list(self, field_name, elem_type, types_path=None):
        # Infer type recursively for type such as List[Dict[str, str]]
        elem_serializer = infer_field("item", elem_type, self, types_path=types_path)
        return ListSerializer(self.fury, list, elem_serializer)

    def visit_dict(self, field_name, key_type, value_type, types_path=None):
        # Infer type recursively for type such as Dict[str, Dict[str, str]]
        key_serializer = infer_field("key", key_type, self, types_path=types_path)
        value_serializer = infer_field("value", value_type, self, types_path=types_path)
        return MapSerializer(self.fury, dict, key_serializer, value_serializer)

    def visit_customized(self, field_name, type_, types_path=None):
        return None

    def visit_other(self, field_name, type_, types_path=None):
        if type_ not in basic_types and not is_py_array_type(type_):
            return None
        serializer = self.fury.class_resolver.get_serializer(type_)
        assert not isinstance(serializer, (PickleSerializer,))
        return serializer


def _get_hash(fury, field_names: list, type_hints: dict):
    visitor = StructHashVisitor(fury)
    for index, key in enumerate(field_names):
        infer_field(key, type_hints[key], visitor, types_path=[])
    hash_ = visitor.get_hash()
    assert hash_ != 0
    return hash_


class ComplexObjectSerializer(Serializer):
    def __init__(self, fury, clz):
        super().__init__(fury, clz)
        self._type_hints = typing.get_type_hints(clz)
        self._field_names = sorted(self._type_hints.keys())
        self._serializers = [None] * len(self._field_names)
        visitor = ComplexTypeVisitor(fury)
        for index, key in enumerate(self._field_names):
            serializer = infer_field(key, self._type_hints[key], visitor, types_path=[])
            self._serializers[index] = serializer
        from pyfury._fury import Language

        if self.fury.language == Language.PYTHON:
            logger.warning(
                "Type of class %s shouldn't be serialized using cross-language "
                "serializer",
                clz,
            )
        self._hash = 0

    def write(self, buffer, value):
        return self.xwrite(buffer, value)

    def read(self, buffer):
        return self.xread(buffer)

    def xwrite(self, buffer: Buffer, value):
        if self._hash == 0:
            self._hash = _get_hash(self.fury, self._field_names, self._type_hints)
        buffer.write_int32(self._hash)
        for index, field_name in enumerate(self._field_names):
            field_value = getattr(value, field_name)
            serializer = self._serializers[index]
            self.fury.xserialize_ref(buffer, field_value, serializer=serializer)

    def xread(self, buffer):
        if self._hash == 0:
            self._hash = _get_hash(self.fury, self._field_names, self._type_hints)
        hash_ = buffer.read_int32()
        if hash_ != self._hash:
            raise ClassNotCompatibleError(
                f"Hash {hash_} is not consistent with {self._hash} "
                f"for class {self.type_}",
            )
        obj = self.type_.__new__(self.type_)
        self.fury.ref_resolver.reference(obj)
        for index, field_name in enumerate(self._field_names):
            serializer = self._serializers[index]
            field_value = self.fury.xdeserialize_ref(buffer, serializer=serializer)
            setattr(
                obj,
                field_name,
                field_value,
            )
        return obj


class StructHashVisitor(TypeVisitor):
    def __init__(
        self,
        fury,
    ):
        self.fury = fury
        self._hash = 17

    def visit_list(self, field_name, elem_type, types_path=None):
        # TODO add list element type to hash.
        xtype_id = self.fury.class_resolver.get_classinfo(list).type_id
        self._hash = self._compute_field_hash(self._hash, abs(xtype_id))

    def visit_dict(self, field_name, key_type, value_type, types_path=None):
        # TODO add map key/value type to hash.
        xtype_id = self.fury.class_resolver.get_classinfo(dict).type_id
        self._hash = self._compute_field_hash(self._hash, abs(xtype_id))

    def visit_customized(self, field_name, type_, types_path=None):
        classinfo = self.fury.class_resolver.get_classinfo(type_, create=False)
        hash_value = 0
        if classinfo is not None:
            hash_value = classinfo.type_id
            if TypeId.is_namespaced_type(classinfo.type_id):
                hash_value = compute_string_hash(
                    classinfo.namespace + classinfo.typename
                )
        self._hash = self._compute_field_hash(self._hash, hash_value)

    def visit_other(self, field_name, type_, types_path=None):
        classinfo = self.fury.class_resolver.get_classinfo(type_, create=False)
        if classinfo is None:
            id_ = 0
        else:
            serializer = classinfo.serializer
            assert not isinstance(serializer, (PickleSerializer,))
            id_ = classinfo.type_id
            assert id_ is not None, serializer
        id_ = abs(id_)
        self._hash = self._compute_field_hash(self._hash, id_)

    @staticmethod
    def _compute_field_hash(hash_, id_):
        new_hash = hash_ * 31 + id_
        while new_hash >= 2**31 - 1:
            new_hash = new_hash // 7
        return new_hash

    def get_hash(self):
        return self._hash
