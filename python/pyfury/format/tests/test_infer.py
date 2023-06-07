import datetime
import pyfury
import pytest

from dataclasses import dataclass
from pyfury.format.infer import infer_schema, infer_field, ArrowTypeVisitor
from pyfury.tests.record import foo_schema
from pyfury.util import lazy_import
from typing import List, Dict


pa = lazy_import("pyarrow")


@dataclass
class Foo:
    f1: "pa.int32"
    f2: str
    f3: List[str]
    f4: Dict[str, "pa.int32"]
    f5: List["pa.int32"]
    f6: "pa.int32"
    f7: "Bar"


@dataclass
class Bar:
    f1: "pa.int32"
    f2: str


def _infer_field(field_name, type_, types_path=None):
    return infer_field(field_name, type_, ArrowTypeVisitor(), types_path=types_path)


def test_infer_field():
    assert _infer_field("", pa.int8).type == pa.int8()
    assert _infer_field("", pa.int16).type == pa.int16()
    assert _infer_field("", pa.int32).type == pa.int32()
    assert _infer_field("", pa.int64).type == pa.int64()
    assert _infer_field("", pa.float32).type == pa.float32()
    assert _infer_field("", pa.float64).type == pa.float64()
    assert _infer_field("", str).type == pa.utf8()
    assert _infer_field("", bytes).type == pa.binary()
    assert _infer_field("", List[Dict[str, str]]).type == pa.list_(
        pa.map_(pa.utf8(), pa.utf8())
    )
    assert _infer_field(
        "", List[Dict[str, Dict[str, List[pa.int32]]]]
    ).type == pa.list_(pa.map_(pa.utf8(), pa.map_(pa.utf8(), pa.list_(pa.int32()))))
    with pytest.raises(TypeError):
        _infer_field("", pa.int8())
        _infer_field("", pa.utf8())

        class X:
            pass

        _infer_field("", X)


def test_infer_class_schema():
    schema = infer_schema(Foo)
    assert schema == foo_schema(), (
        f"schema {schema}\n====\n," f"foo_schema {foo_schema()}"
    )


def test_type_id():
    assert pyfury.format.infer.get_type_id(str) == pa.string().id
    assert pyfury.format.infer.get_type_id(datetime.date) == pa.date32().id
    assert pyfury.format.infer.get_type_id(datetime.datetime) == pa.timestamp("us").id


if __name__ == "__main__":
    test_infer_class_schema()
