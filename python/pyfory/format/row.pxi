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

from libcpp.memory cimport make_shared
from libc.stdint cimport *
from cython.operator cimport dereference as deref
from datetime import timedelta
from pyfury.includes.libformat cimport CGetter, CArrayData, CMapData, CRow
from pyfury._util cimport Buffer
from libcpp.memory cimport shared_ptr
from datetime import datetime, date
from libc.stdint cimport *
from libcpp cimport bool as c_bool
from cpython cimport *
from pyarrow.lib cimport Schema, DataType, ListType, MapType, Field

import pyarrow as pa
from pyarrow import types

cdef dict reader_map = {}


cdef class Getter:
    cdef:
        CGetter* getter

    cdef inline c_bool is_null_at(self, int i):
        return self.getter.IsNullAt(i)

    cpdef get_boolean(self, int i):
        if self.is_null_at(i):
            return None
        return self.getter.GetBoolean(i)

    cpdef get_int8(self, int i):
        if self.is_null_at(i):
            return None
        return self.getter.GetInt8(i)

    cpdef get_int16(self, int i):
        if self.is_null_at(i):
            return None
        return self.getter.GetInt16(i)

    cpdef get_int32(self, int i):
        if self.is_null_at(i):
            return None
        return self.getter.GetInt32(i)

    cpdef get_int64(self, int i):
        if self.is_null_at(i):
            return None
        return self.getter.GetInt64(i)

    cpdef get_float(self, int i):
        if self.is_null_at(i):
            return None
        return self.getter.GetFloat(i)

    cpdef get_double(self, int i):
        if self.is_null_at(i):
            return None
        return self.getter.GetDouble(i)

    cpdef get_date(self, int i):
        if self.is_null_at(i):
            return None
        cdef int32_t days = self.getter.GetInt32(i)
        return date(1970, 1, 1) + timedelta(days=days)

    cpdef get_datetime(self, int i):
        if self.is_null_at(i):
            return None
        cdef int64_t timestamp = self.getter.GetInt64(i)
        # TimestampType represent micro seconds
        return datetime.fromtimestamp(float(timestamp) / 1000000)

    cpdef get_binary(self, int i):
        if self.is_null_at(i):
            return None
        cdef unsigned char* binary_data
        cdef int32_t size = self.getter.GetBinary(i, &binary_data)
        return binary_data[:size]

    cpdef get_str(self, int i):
        if self.is_null_at(i):
            return None
        cdef unsigned char* binary_data
        cdef int32_t size = self.getter.GetBinary(i, &binary_data)
        return binary_data[:size].decode("UTF-8")

    cpdef RowData get_struct(self, int i):
        pass

    cpdef ArrayData get_array_data(self, int i):
        pass

    cpdef MapData get_map_data(self, int i):
        pass


cdef class ArrayData(Getter):
    cdef:
        ListType type_
        shared_ptr[CArrayData] data

    def __init__(self):
        raise TypeError("Do not call constructor directly, use "
                        "factory function instead.")

    @staticmethod
    cdef ArrayData wrap(shared_ptr[CArrayData] data, ListType array_type):
        cdef ArrayData array_data = ArrayData.__new__(ArrayData)
        array_data.getter = data.get()
        array_data.data = data
        array_data.type_ = array_type
        return array_data

    @property
    def num_elements(self) -> int:
        return self.data.get().num_elements()

    def buffer(self) -> Buffer:
        return Buffer.wrap(self.data.get().buffer())

    def base_offset(self) -> int:
        return self.data.get().base_offset()

    def size_bytes(self) -> int:
        return self.data.get().size_bytes()

    cpdef RowData get_struct(self, int i):
        cdef DataType data_type = self.type_.value_type
        # assert_type(i, data_type, StructType)
        if self.is_null_at(i):
            return None
        return RowData.wrap(self.data.get().GetStruct(i), pa.schema(data_type))

    cpdef ArrayData get_array_data(self, int i):
        cdef DataType data_type = self.type_.value_type
        if self.is_null_at(i):
            return None
        return ArrayData.wrap(self.data.get().GetArray(i), data_type)

    cpdef MapData get_map_data(self, int i):
        cdef DataType data_type = self.type_.value_type
        if self.is_null_at(i):
            return None
        cdef shared_ptr[CMapData] v = self.data.get().GetMap(i)
        return MapData.wrap(v, data_type)

    def __getitem__(self, i):
        if i > self.num_elements or i < 0:
            raise IndexError("length is {}, but index is {}"
                             .format(self.num_elements, i))
        return self.get(i)

    def get(self, int i):
        key = id(self.type_.value_type)
        reader = reader_map.get(key)
        if reader is None:
            reader = get_reader(self.type_.value_type, type(self))
            reader_map[key] = reader
        if self.is_null_at(i):
            return None
        else:
            # cdef methods don't bind self.
            return reader(self, i)

    def __dealloc__(self):
        reader_map.pop(id(self.type_.value_type), None)

    def __str__(self) -> str:
        cdef:
            int length = self.num_elements
            int i
            str result = "["
        getter = get_reader(self.type_.value_type, type(self))
        for i in range(length):
            if i != 0:
                result += ','
            if self.is_null_at(i):
                result += "null"
            else:
                result += str(getter(self, i))
        result += ']'
        return result


cdef class MapData:
    cdef:
        shared_ptr[CMapData] data
        MapType map_type

    def __init__(self):
        raise TypeError("Do not call constructor directly, use "
                        "factory function instead.")

    @staticmethod
    cdef MapData wrap(shared_ptr[CMapData] data, MapType map_type):
        cdef MapData map_data = MapData.__new__(MapData)
        map_data.data = data
        map_data.map_type = map_type
        return map_data

    @property
    def num_elements(self) -> int:
        return self.data.get().num_elements()

    def buffer(self) -> Buffer:
        return Buffer.wrap(self.data.get().buffer())

    def base_offset(self) -> int:
        return self.data.get().base_offset()

    def size_bytes(self) -> int:
        return self.data.get().size_bytes()

    def keys_array(self):
        array_type = pa.list_(self.map_type.key_type)
        return ArrayData.wrap(self.data.get().keys_array(), array_type)

    def values_array(self):
        array_type = pa.list_(self.map_type.item_type)
        return ArrayData.wrap(self.data.get().values_array(), array_type)

    cdef keys_array_(self, DataType array_type):
        return ArrayData.wrap(self.data.get().keys_array(), array_type)

    cdef values_array_(self, DataType array_type):
        return ArrayData.wrap(self.data.get().values_array(), array_type)

    def __str__(self):
        return 'Map{' + str(self.keys_array()) + ', ' + str(self.values_array()) + '}'


cdef class RowData(Getter):
    cdef:
        shared_ptr[CRow] data
        Schema schema
        Buffer _buf  # hold buffer reference

    def __init__(self, schema, buffer, offset=0, size_in_bytes=None):
        if size_in_bytes is None:
            size_in_bytes = len(buffer)
        if type(buffer) is not Buffer:
            buffer = Buffer(buffer, offset=offset, length=size_in_bytes)
        self._buf = buffer
        cdef:
            Buffer buf = <Buffer>buffer
            shared_ptr[CRow] row = make_shared[CRow]((<Schema>schema).sp_schema)
        deref(row).PointTo(buf.c_buffer, offset, size_in_bytes)
        self.data = row
        self.getter = row.get()
        self.schema = schema

    @staticmethod
    cdef RowData wrap(shared_ptr[CRow] data, Schema schema):
        cdef RowData row_data = RowData.__new__(RowData)
        row_data.data = data
        row_data.getter = data.get()
        row_data.schema = schema
        return row_data

    @property
    def num_fields(self) -> int:
        return self.data.get().num_fields()

    def buffer(self) -> Buffer:
        return Buffer.wrap(self.data.get().buffer())

    cpdef base_offset(self):
        return self.data.get().base_offset()

    cpdef size_bytes(self):
        return self.data.get().size_bytes()

    def to_bytes(self) -> bytes:
        end_offset = self.base_offset() + self.size_bytes()
        return self.buffer().to_bytes()[self.base_offset():end_offset]

    cpdef RowData get_struct(self, int i):
        if self.is_null_at(i):
            return None
        cdef DataType data_type = self.schema.field(i).type
        # assert_type(i, self.schema.field(i).type, StructType)
        return RowData.wrap(self.data.get().GetStruct(i), pa.schema(data_type))

    cpdef ArrayData get_array_data(self, int i):
        if self.is_null_at(i):
            return None
        cdef DataType data_type = self.schema.field(i).type
        return ArrayData.wrap(self.data.get().GetArray(i), data_type)

    cpdef MapData get_map_data(self, int i):
        if self.is_null_at(i):
            return None
        cdef DataType data_type = self.schema.field(i).type
        cdef shared_ptr[CMapData] v = self.data.get().GetMap(i)
        return MapData.wrap(v, data_type)

    def __getitem__(self, i):
        if not isinstance(i, int):
            assert type(i) is str
            i = self.schema.names.index(i)
        if i > self.num_fields or i < 0:
            raise IndexError("num_fields is {}, but index is {}"
                             .format(self.num_fields, i))
        return self.get(i)

    def __getattr__(self, item):
        return self.__getitem__(item)

    def get(self, i):
        key = id(self.schema)
        readers = reader_map.get(key)
        if readers is None:
            readers = []
            for field_index in range(len(self.schema)):
                readers.append(get_reader(
                    self.schema.field(field_index).type, type(self)))
            reader_map[key] = readers

        if self.is_null_at(i):
            return None
        else:
            return readers[i](self, i)

    def __dealloc__(self):
        reader_map.pop(id(self.schema), None)

    def __str__(self) -> str:
        cdef:
            Field field
            int num_fields = len(self.schema)
            int i
            str result = "{"
        for i in range(num_fields):
            if i != 0:
                result += ','
            field = self.schema.field(i)
            getter = get_reader(field.type, type(self))
            result += field.name
            result += '='
            if self.is_null_at(i):
                result += "null"
            else:
                result += str(getter(self, i))
        result += "}"
        return result


def assert_type(i, data_type, type_cls):
    if not isinstance(data_type, type_cls):
        raise TypeError("type for {0} is {1}, isn't {2}".
                        format(i, data_type, type_cls))


def get_reader(data_type, type_):
    if types.is_boolean(data_type):
        return type_.get_boolean
    elif types.is_int8(data_type):
        return type_.get_int8
    elif types.is_int16(data_type):
        return type_.get_int16
    elif types.is_int32(data_type):
        return type_.get_int32
    elif types.is_int64(data_type):
        return type_.get_int64
    elif types.is_float32(data_type):
        return type_.get_float
    elif types.is_float64(data_type):
        return type_.get_double
    elif types.is_date32(data_type):
        return type_.get_date
    elif types.is_timestamp(data_type):
        return type_.get_datetime
    elif types.is_binary(data_type):
        return type_.get_binary
    elif types.is_string(data_type):
        return type_.get_str
    elif types.is_struct(data_type):
        return type_.get_struct
    elif types.is_list(data_type):
        return type_.get_array_data
    elif types.is_map(data_type):
        return type_.get_map_data
    raise TypeError("Unsupported type: " + str(data_type))
