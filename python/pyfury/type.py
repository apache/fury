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
import importlib
import inspect

import typing
from typing import TypeVar
from abc import ABC, abstractmethod

try:
    import numpy as np

    ndarray = np.ndarray
except ImportError:
    np, ndarray = None, None


# modified from `fluent python`
def record_class_factory(cls_name, field_names):
    """
    record_factory: create simple classes just for holding data fields

    >>> Dog = record_class_factory('Dog', 'name weight owner')
    >>> rex = Dog('Rex', 30, 'Bob')
    >>> rex
    Dog(name='Rex', weight=30, owner='Bob')
    >>> name, weight, _ = rex
    >>> name, weight
    ('Rex', 30)
    >>> "{2}'s dog weighs {1}kg".format(*rex)
    "Bob's dog weighs 30kg"
    >>> rex.weight = 32
    >>> rex
    Dog(name='Rex', weight=32, owner='Bob')
    >>> Dog.__mro__
    (<class '_util.Dog'>, <class 'object'>)

    The factory also accepts a list or tuple of identifiers:

    >>> Dog = record_class_factory('Dog', ['name', 'weight', 'owner'])
    >>> Dog.__slots__
    ('name', 'weight', 'owner')

    """
    try:
        field_names = field_names.replace(",", " ").split()
    except AttributeError:  # no .replace or .split
        pass  # assume it's already a sequence of identifiers
    field_names = tuple(field_names)

    def __init__(self, *args, **kwargs):
        attrs = dict(zip(self.__slots__, args))
        attrs.update(kwargs)
        for name, value in attrs.items():
            setattr(self, name, value)

    def __iter__(self):
        for name in self.__slots__:
            yield getattr(self, name)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if not self.__slots__ == other.__slots__:
            return False
        else:
            for name in self.__slots__:
                if not getattr(self, name, None) == getattr(other, name, None):
                    return False
        return True

    def __hash__(self):
        return hash([getattr(self, name, None) for name in self.__slots__])

    def __str__(self):
        values = ", ".join("{}={!r}".format(*i) for i in zip(self.__slots__, self))
        return values

    def __repr__(self):
        values = ", ".join("{}={!r}".format(*i) for i in zip(self.__slots__, self))
        return "{}({})".format(self.__class__.__name__, values)

    def __reduce__(self):
        return self.__class__, tuple(self)

    cls_attrs = dict(
        __slots__=field_names,
        __init__=__init__,
        __iter__=__iter__,
        __eq__=__eq__,
        __hash__=__hash__,
        __str__=__str__,
        __repr__=__repr__,
        __reduce__=__reduce__,
    )

    cls_ = type(cls_name, (object,), cls_attrs)
    # combined with __reduce__ to make it pickable
    globals()[cls_name] = cls_
    return cls_


def get_qualified_classname(obj):
    import inspect

    t = obj if inspect.isclass(obj) else type(obj)
    return t.__module__ + "." + t.__name__


class TypeId:
    """
    Fury type for cross-language serialization.
    See `org.apache.fury.types.Type`
    """

    # a boolean value (true or false).
    BOOL = 1
    # a 8-bit signed integer.
    INT8 = 2
    # a 16-bit signed integer.
    INT16 = 3
    # a 32-bit signed integer.
    INT32 = 4
    # a 32-bit signed integer which use fury var_int32 encoding.
    VAR_INT32 = 5
    # a 64-bit signed integer.
    INT64 = 6
    # a 64-bit signed integer which use fury PVL encoding.
    VAR_INT64 = 7
    # a 64-bit signed integer which use fury SLI encoding.
    SLI_INT64 = 8
    # a 16-bit floating point number.
    FLOAT16 = 9
    #  a 32-bit floating point number.
    FLOAT32 = 10
    # a 64-bit floating point number including NaN and Infinity.
    FLOAT64 = 11
    # a text string encoded using Latin1/UTF16/UTF-8 encoding.
    STRING = 12
    # a data type consisting of a set of named values. Rust enum with non-predefined field values are not supported as
    # an enum
    ENUM = 13
    # an enum whose value will be serialized as the registered name.
    NAMED_ENUM = 14
    # a morphic(final) type serialized by Fury Struct serializer. i.e. it doesn't have subclasses. Suppose we're
    # deserializing `List[SomeClass]`, we can save dynamic serializer dispatch since `SomeClass` is morphic(final).
    STRUCT = 15
    # a type which is not morphic(not final). i.e. it have subclasses. Suppose we're deserializing
    # `List[SomeClass]`, we must dispatch serializer dynamically since `SomeClass` is polymorphic(non-final).
    POLYMORPHIC_STRUCT = 16
    # a morphic(final) type serialized by Fury compatible Struct serializer.
    COMPATIBLE_STRUCT = 17
    # a non-morphic(non-final) type serialized by Fury compatible Struct serializer.
    POLYMORPHIC_COMPATIBLE_STRUCT = 18
    # a `struct` whose type mapping will be encoded as a name.
    NAMED_STRUCT = 19
    # a `polymorphic_struct` whose type mapping will be encoded as a name.
    NAMED_POLYMORPHIC_STRUCT = 20
    # a `compatible_struct` whose type mapping will be encoded as a name.
    NAMED_COMPATIBLE_STRUCT = 21
    # a `polymorphic_compatible_struct` whose type mapping will be encoded as a name.
    NAMED_POLYMORPHIC_COMPATIBLE_STRUCT = 22
    # a type which will be serialized by a customized serializer.
    EXT = 23
    # an `ext` type which is not morphic(not final).
    POLYMORPHIC_EXT = 24
    # an `ext` type whose type mapping will be encoded as a name.
    NAMED_EXT = 25
    # an `polymorphic_ext` type whose type mapping will be encoded as a name.
    NAMED_POLYMORPHIC_EXT = 26
    # a sequence of objects.
    LIST = 27
    # an unordered set of unique elements.
    SET = 28
    # a map of key-value pairs. Mutable types such as `list/map/set/array/tensor/arrow` are not allowed as key of map.
    MAP = 29
    # an absolute length of time, independent of any calendar/timezone, as a count of nanoseconds.
    DURATION = 30
    # a point in time, independent of any calendar/timezone, as a count of nanoseconds. The count is relative
    # to an epoch at UTC midnight on January 1, 1970.
    TIMESTAMP = 31
    # a naive date without timezone. The count is days relative to an epoch at UTC midnight on Jan 1, 1970.
    LOCAL_DATE = 32
    # exact decimal value represented as an integer value in two's complement.
    DECIMAL = 33
    # an variable-length array of bytes.
    BINARY = 34
    # a multidimensional array which every sub-array can have different sizes but all have same type.
    # only allow numeric components. Other arrays will be taken as List. The implementation should support the
    # interoperability between array and list.
    ARRAY = 35
    # one dimensional bool array.
    BOOL_ARRAY = 36
    # one dimensional int16 array.
    INT8_ARRAY = 37
    # one dimensional int16 array.
    INT16_ARRAY = 38
    # one dimensional int32 array.
    INT32_ARRAY = 39
    # one dimensional int64 array.
    INT64_ARRAY = 40
    # one dimensional half_float_16 array.
    FLOAT16_ARRAY = 41
    # one dimensional float32 array.
    FLOAT32_ARRAY = 42
    # one dimensional float64 array.
    FLOAT64_ARRAY = 43
    # an arrow [record batch](https://arrow.apache.org/docs/cpp/tables.html#record-batches) object.
    ARROW_RECORD_BATCH = 44
    # an arrow [table](https://arrow.apache.org/docs/cpp/tables.html#tables) object.
    ARROW_TABLE = 45
    BOUND = 64

    @staticmethod
    def is_namespaced_type(type_id: int) -> bool:
        return type_id in __NAMESPACED_TYPES__


__NAMESPACED_TYPES__ = {
    TypeId.NAMED_EXT,
    TypeId.NAMED_POLYMORPHIC_EXT,
    TypeId.NAMED_ENUM,
    TypeId.NAMED_STRUCT,
    TypeId.NAMED_POLYMORPHIC_STRUCT,
    TypeId.NAMED_COMPATIBLE_STRUCT,
    TypeId.NAMED_POLYMORPHIC_COMPATIBLE_STRUCT,
}
Int8Type = TypeVar("Int8Type", bound=int)
Int16Type = TypeVar("Int16Type", bound=int)
Int32Type = TypeVar("Int32Type", bound=int)
Int64Type = TypeVar("Int64Type", bound=int)
Float32Type = TypeVar("Float32Type", bound=float)
Float64Type = TypeVar("Float64Type", bound=float)

_primitive_types = {
    int,
    float,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
}


# `Union[type, TypeVar]` is not supported in py3.6, so skip adding type hints for `type_`  # noqa: E501
# See more at https://github.com/python/typing/issues/492 and
# https://stackoverflow.com/questions/69427175/how-to-pass-forwardref-as-args-to-typevar-in-python-3-6  # noqa: E501
def is_primitive_type(type_) -> bool:
    return type_ in _primitive_types


# Int8ArrayType = TypeVar("Int8ArrayType", bound=array.ArrayType)
BoolArrayType = TypeVar("BoolArrayType")
Int16ArrayType = TypeVar("Int16ArrayType", bound=array.ArrayType)
Int32ArrayType = TypeVar("Int32ArrayType", bound=array.ArrayType)
Int64ArrayType = TypeVar("Int64ArrayType", bound=array.ArrayType)
Float32ArrayType = TypeVar("Float32ArrayType", bound=array.ArrayType)
Float64ArrayType = TypeVar("Float64ArrayType", bound=array.ArrayType)
BoolNDArrayType = TypeVar("BoolNDArrayType", bound=ndarray)
Int16NDArrayType = TypeVar("Int16NDArrayType", bound=ndarray)
Int32NDArrayType = TypeVar("Int32NDArrayType", bound=ndarray)
Int64NDArrayType = TypeVar("Int64NDArrayType", bound=ndarray)
Float32NDArrayType = TypeVar("Float32NDArrayType", bound=ndarray)
Float64NDArrayType = TypeVar("Float64NDArrayType", bound=ndarray)


_py_array_types = {
    # Int8ArrayType,
    Int16ArrayType,
    Int32ArrayType,
    Int64ArrayType,
    Float32ArrayType,
    Float64ArrayType,
}


def is_py_array_type(type_) -> bool:
    return type_ in _py_array_types


class TypeVisitor(ABC):
    @abstractmethod
    def visit_list(self, field_name, elem_type, types_path=None):
        pass

    @abstractmethod
    def visit_dict(self, field_name, key_type, value_type, types_path=None):
        pass

    @abstractmethod
    def visit_customized(self, field_name, type_, types_path=None):
        pass

    @abstractmethod
    def visit_other(self, field_name, type_, types_path=None):
        pass


def infer_field(field_name, type_, visitor: TypeVisitor, types_path=None):
    types_path = list(types_path or [])
    types_path.append(type_)
    origin = (
        typing.get_origin(type_)
        if hasattr(typing, "get_origin")
        else getattr(type_, "__origin__", type_)
    )
    origin = origin or type_
    args = (
        typing.get_args(type_)
        if hasattr(typing, "get_args")
        else getattr(type_, "__args__", ())
    )
    if args:
        if origin is list or origin == typing.List:
            elem_type = args[0]
            return visitor.visit_list(field_name, elem_type, types_path=types_path)
        elif origin is dict or origin == typing.Dict:
            key_type, value_type = args
            return visitor.visit_dict(
                field_name, key_type, value_type, types_path=types_path
            )
        else:
            raise TypeError(
                f"Collection types should be {list, dict} instead of {type_}"
            )
    else:
        if is_function(origin) or not hasattr(origin, "__annotations__"):
            return visitor.visit_other(field_name, type_, types_path=types_path)
        else:
            return visitor.visit_customized(field_name, type_, types_path=types_path)


def is_function(func):
    return inspect.isfunction(func) or is_cython_function(func)


def is_cython_function(func):
    return getattr(func, "func_name", None) is not None


def compute_string_hash(string):
    string_bytes = string.encode("utf-8")
    hash_ = 17
    for b in string_bytes:
        hash_ = hash_ * 31 + b
        while hash_ >= 2**31 - 1:
            hash_ = hash_ // 7
    return hash_


def qualified_class_name(cls):
    if isinstance(cls, TypeVar):
        return cls.__module__ + "#" + cls.__name__
    else:
        return cls.__module__ + "#" + cls.__qualname__


def load_class(classname: str):
    mod_name, cls_name = classname.rsplit("#", 1)
    try:
        mod = importlib.import_module(mod_name)
    except ImportError as ex:
        raise Exception(f"Can't import module {mod_name}") from ex
    try:
        classes = cls_name.split(".")
        cls = getattr(mod, classes.pop(0))
        while classes:
            cls = getattr(cls, classes.pop(0))
        return cls
    except AttributeError as ex:
        raise Exception(f"Can't import class {cls_name} from module {mod_name}") from ex


# This method is derived from https://github.com/ericvsmith/dataclasses/blob/5f6568c3468f872e8f447dc20666628387786397/dataclass_tools.py.
def dataslots(cls):
    # Need to create a new class, since we can't set __slots__
    #  after a class has been created.

    # Make sure __slots__ isn't already set.
    if "__slots__" in cls.__dict__:  # pragma: no cover
        raise TypeError(f"{cls.__name__} already specifies __slots__")

    # Create a new dict for our new class.
    cls_dict = dict(cls.__dict__)
    field_names = tuple(f.name for f in dataclasses.fields(cls))
    cls_dict["__slots__"] = field_names
    for field_name in field_names:
        # Remove our attributes, if present. They'll still be
        #  available in _MARKER.
        cls_dict.pop(field_name, None)
    # Remove __dict__ itself.
    cls_dict.pop("__dict__", None)
    # And finally create the class.
    qualname = getattr(cls, "__qualname__", None)
    cls = type(cls)(cls.__name__, cls.__bases__, cls_dict)
    if qualname is not None:
        cls.__qualname__ = qualname
    return cls
