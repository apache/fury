from dataclasses import dataclass

import pyfury as fury
from typing import List, Dict

from pyfury.util import lazy_import

pa = lazy_import("pyarrow")


class Foo:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class Bar:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    @classmethod
    def create(cls):
        obj = cls.__new__(cls)
        obj.f1 = 1
        obj.f2 = "str"
        return obj


@dataclass
class FooPOJO:
    f1: "pa.int32"
    f2: str
    f3: List[str]
    f4: Dict[str, "pa.int32"]
    f5: List["pa.int32"]
    f6: "pa.int32"
    f7: "BarPOJO"


@dataclass
class BarPOJO:
    f1: "pa.int32"
    f2: str


def create_foo(foo_cls=Foo, bar_cls=Bar):
    obj = foo_cls.__new__(foo_cls)
    size = 10
    data = {
        "f1": 1,
        "f2": "str",
        "f3": ["str" + str(i) for i in range(size)],
        "f4": {"k" + str(i): i for i in range(size)},
        "f5": [-i for i in range(size)],
        "f6": -100,
        "f7": create_bar(bar_cls),
    }
    obj.__dict__.update(**data)
    return obj


def create_bar(cls):
    obj = cls.__new__(cls)
    obj.f1 = 1
    obj.f2 = "str"
    return obj


def create_foo_pojo():
    size = 10
    return FooPOJO(
        f1=1,
        f2="str",
        f3=["str" + str(i) for i in range(size)],
        f4={"k" + str(i): i for i in range(size)},
        f5=[-i for i in range(size)],
        f6=-100,
        f7=BarPOJO(f1=1, f2="str"),
    )


def foo_schema():
    bar_struct = pa.struct([("f1", pa.int32()), ("f2", pa.string())])
    return pa.schema(
        [
            ("f1", pa.int32()),
            ("f2", pa.string()),
            ("f3", pa.list_(pa.string())),
            ("f4", pa.map_(pa.string(), pa.int32())),
            ("f5", pa.list_(pa.int32())),
            ("f6", pa.int32()),
            pa.field(
                "f7", bar_struct, metadata={"cls": fury.get_qualified_classname(Bar)}
            ),
        ],
        metadata={"cls": fury.get_qualified_classname(Foo)},
    )
