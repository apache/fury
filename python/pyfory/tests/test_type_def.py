import os
from dataclasses import dataclass

from ..meta.field_info import FieldInfo,FieldType
from ..meta.type_info import TypeInfo
from pyfory.buffer import Buffer

def test_field_info():
    os.environ["ENABLE_FORY_CYTHON_SERIALIZATION"] = "0"
    t = FieldType(1,True,False,True)
    field_info = FieldInfo("class_a", "field_b", t)
    buffer = Buffer.allocate(32)
    field_info.xwrite(buffer)
    new_field_info = FieldInfo.xread(buffer, field_info.defined_class)
    assert new_field_info == field_info

def test_type_info():
    defined_class = "class_a"
    field_infos = [
        FieldInfo(defined_class, "f_b", FieldType(1,True,True,False)),
        FieldInfo(defined_class, "f_c", FieldType(2, True, False, True)),
    ]
    type_info = TypeInfo(field_infos, 3)
    buffer = Buffer.allocate(32)
    type_info.xwrite(buffer)
    new_type_info = TypeInfo.xread(buffer)
    for i,info in enumerate(field_infos):
        assert info == new_type_info.field_infos[i]
