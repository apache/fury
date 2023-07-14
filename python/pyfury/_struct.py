import datetime
import logging
import typing

from pyfury._serializer import NOT_SUPPORT_CROSS_LANGUAGE
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
    FuryType,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
    is_py_array_type,
    compute_string_hash,
    qualified_class_name,
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
    def __init__(self, fury, clz: type, type_tag: str):
        super().__init__(fury, clz)
        self._type_tag = type_tag
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

    def get_xtype_id(self):
        return FuryType.FURY_TYPE_TAG.value

    def get_xtype_tag(self):
        return self._type_tag

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
        id_ = abs(ListSerializer(self.fury, list).get_xtype_id())
        self._hash = self._compute_field_hash(self._hash, id_)

    def visit_dict(self, field_name, key_type, value_type, types_path=None):
        # TODO add map key/value type to hash.
        id_ = abs(MapSerializer(self.fury, dict).get_xtype_id())
        self._hash = self._compute_field_hash(self._hash, id_)

    def visit_customized(self, field_name, type_, types_path=None):
        serializer = self.fury.class_resolver.get_serializer(type_)
        if serializer.get_xtype_id() != NOT_SUPPORT_CROSS_LANGUAGE:
            tag = serializer.get_xtype_tag()
        else:
            tag = qualified_class_name(type_)
        tag_hash = compute_string_hash(tag)
        self._hash = self._compute_field_hash(self._hash, tag_hash)

    def visit_other(self, field_name, type_, types_path=None):
        if type_ not in basic_types and not is_py_array_type(type_):
            # FIXME ignore unknown types for hash calculation
            return None
        serializer = self.fury.class_resolver.get_serializer(type_)
        assert not isinstance(serializer, (PickleSerializer,))
        id_ = serializer.get_xtype_id()
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
