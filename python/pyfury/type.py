import array
import dataclasses
import enum
import importlib

import typing
from typing import TypeVar
from abc import ABC, abstractmethod


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


class FuryType(enum.Enum):
    """
    Fury added type for cross-language serialization.
    See `io.fury.types.Type`
    """

    NA = 0
    # BOOL Boolean as 1 bit LSB bit-packed ordering
    BOOL = 1
    # UINT8 Unsigned 8-bit little-endian integer
    UINT8 = 2
    # INT8 Signed 8-bit little-endian integer
    INT8 = 3
    # UINT16 Unsigned 16-bit little-endian integer
    UINT16 = 4
    # INT16 Signed 16-bit little-endian integer
    INT16 = 5
    # UINT32 Unsigned 32-bit little-endian integer
    UINT32 = 6
    # INT32 Signed 32-bit little-endian integer
    INT32 = 7
    # UINT64 Unsigned 64-bit little-endian integer
    UINT64 = 8
    # INT64 Signed 64-bit little-endian integer
    INT64 = 9
    # HALF_FLOAT 2-byte floating point value
    HALF_FLOAT = 10
    # FLOAT 4-byte floating point value
    FLOAT = 11
    # DOUBLE 8-byte floating point value
    DOUBLE = 12
    # STRING UTF8 variable-length string as List<Char>
    STRING = 13
    # BINARY Variable-length bytes (no guarantee of UTF8-ness)
    BINARY = 14
    # FIXED_SIZE_BINARY Fixed-size binary. Each value occupies the same number of bytes
    FIXED_SIZE_BINARY = 15
    # DATE32 int32_t days since the UNIX epoch
    DATE32 = 16
    # DATE64 int64_t milliseconds since the UNIX epoch
    DATE64 = 17
    # TIMESTAMP Exact timestamp encoded with int64 since UNIX epoch
    # Default unit millisecond
    TIMESTAMP = 18
    # TIME32 Time as signed 32-bit integer representing either seconds or
    # milliseconds since midnight
    TIME32 = 19
    # TIME64 Time as signed 64-bit integer representing either microseconds or
    # nanoseconds since midnight
    TIME64 = 20
    # INTERVAL_MONTHS YEAR_MONTH interval in SQL style
    INTERVAL_MONTHS = 21
    # INTERVAL_DAY_TIME DAY_TIME interval in SQL style
    INTERVAL_DAY_TIME = 22
    # DECIMAL128 Precision- and scale-based decimal type with 128 bits.
    DECIMAL128 = 23
    # DECIMAL256 Precision- and scale-based decimal type with 256 bits.
    DECIMAL256 = 24
    # LIST A list of some logical data type
    LIST = 25
    # STRUCT Struct of logical types
    STRUCT = 26
    # SPARSE_UNION Sparse unions of logical types
    SPARSE_UNION = 27
    # DENSE_UNION Dense unions of logical types
    DENSE_UNION = 28
    # DICTIONARY Dictionary-encoded type also called "categorical" or "factor"
    # in other programming languages. Holds the dictionary value
    # type but not the dictionary itself which is part of the
    # ArrayData struct
    DICTIONARY = 29
    # MAP Map a repeated struct logical type
    MAP = 30
    # EXTENSION Custom data type implemented by user
    EXTENSION = 31
    # FIXED_SIZE_LIST Fixed size list of some logical type
    FIXED_SIZE_LIST = 31
    # DURATION Measure of elapsed time in either seconds milliseconds microseconds
    # or nanoseconds.
    DURATION = 33
    # LARGE_STRING Like STRING but with 64-bit offsets
    LARGE_STRING = 34
    # LARGE_BINARY Like BINARY but with 64-bit offsets
    LARGE_BINARY = 35
    # LARGE_LIST Like LIST but with 64-bit offsets
    LARGE_LIST = 36
    # MAX_ID Leave this at the end
    MAX_ID = 37
    DECIMAL = DECIMAL128

    FURY_TYPE_TAG = 256
    FURY_SET = 257
    FURY_PRIMITIVE_BOOL_ARRAY = 258
    FURY_PRIMITIVE_SHORT_ARRAY = 259
    FURY_PRIMITIVE_INT_ARRAY = 260
    FURY_PRIMITIVE_LONG_ARRAY = 261
    FURY_PRIMITIVE_FLOAT_ARRAY = 262
    FURY_PRIMITIVE_DOUBLE_ARRAY = 263
    FURY_STRING_ARRAY = 264
    FURY_SERIALIZED_OBJECT = 265
    FURY_BUFFER = 266
    FURY_ARROW_RECORD_BATCH = 267
    FURY_ARROW_TABLE = 268


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
Int16ArrayType = TypeVar("Int16ArrayType", bound=array.ArrayType)
Int32ArrayType = TypeVar("Int32ArrayType", bound=array.ArrayType)
Int64ArrayType = TypeVar("Int64ArrayType", bound=array.ArrayType)
Float32ArrayType = TypeVar("Float32ArrayType", bound=array.ArrayType)
Float64ArrayType = TypeVar("Float64ArrayType", bound=array.ArrayType)


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
        if hasattr(origin, "__annotations__"):
            return visitor.visit_customized(field_name, type_, types_path=types_path)
        else:
            return visitor.visit_other(field_name, type_, types_path=types_path)


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


# from https://github.com/ericvsmith/dataclasses/blob/master/dataclass_tools.py
# released under Apache License 2.0
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
