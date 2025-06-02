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

from dataclasses import dataclass
from pyfory.buffer import Buffer
from .field_info import FieldInfo
from .xtype_resolver import XtypeResolver
from .metastring import MetaStringEncoder,MetaStringDecoder
from pyfory.lib.mmh3 import hash_buffer

NUM_FIELDS_SIZE_THRESHOLD = 0b11111
REGISTER_BY_NAME_FLAG     = 0b100000

COMPRESS_META_FLAG = 0b1 << 13
HAS_FIELDS_META_FLAG = 0b1 << 12
META_SIZE_MASKS = 0b111_1111_1111
NUM_HASH_BITS = 50


@dataclass
class TypeInfo:
    namespace_encoder = MetaStringEncoder(".", "_")
    namespace_decoder = MetaStringDecoder(".", "_")
    type_name_encoder = namespace_encoder
    type_name_decoder = namespace_decoder

    def __init__(self, field_infos:list[FieldInfo], type_id=None, cls=None, namespace=None, type_name=None):
        self.type_id = type_id
        self.cls = cls
        self.field_infos = field_infos
        self.namespace = namespace
        self.type_name = type_name

    def xwrite(self, buffer:Buffer, resolver:XtypeResolver=None):
        sub_buffer = Buffer.allocate(128)
        # TODO: fields_num only relate to compatible fields
        fields_num = len(self.field_infos) - 1
        sub_header = fields_num & NUM_FIELDS_SIZE_THRESHOLD
        # is_registered_by_name = resolver.is_registered_by_name(self.cls)
        is_registered_by_name = False
        if is_registered_by_name:
            sub_header |= REGISTER_BY_NAME_FLAG
        sub_buffer.write_uint8(sub_header)

        if fields_num >= NUM_FIELDS_SIZE_THRESHOLD:
            sub_buffer.write_varuint32(fields_num - NUM_FIELDS_SIZE_THRESHOLD)

        if is_registered_by_name:
            raise NotImplementedError()
        else:
            sub_buffer.write_varuint32(self.type_id)

        for field_info in self.field_infos:
            field_info.xwrite(sub_buffer)

        is_compressed = False
        has_fields_meta = True

        meta_size = sub_buffer.writer_index
        sub_buffer_data = sub_buffer.get_bytes(0, sub_buffer.writer_index)

        hash_value = hash_buffer(sub_buffer_data,47)[0]
        hash_value = (hash_value << (64-NUM_HASH_BITS)) & ((1 << 64) - 1)
        global_header = abs(hash_value)


        if is_compressed:
            global_header |= COMPRESS_META_FLAG
        if has_fields_meta:
            global_header |= HAS_FIELDS_META_FLAG
        global_header |= (meta_size & META_SIZE_MASKS)

        buffer.write_int64(global_header)
        if meta_size >= META_SIZE_MASKS:
            buffer.write_varuint32(meta_size - META_SIZE_MASKS)
        buffer.write_bytes(sub_buffer_data)


    @staticmethod
    def xread(buffer:Buffer)->"TypeInfo":
        global_header = buffer.read_int64()

        is_compressed = (global_header & COMPRESS_META_FLAG) != 0
        has_fields_meta = (global_header & HAS_FIELDS_META_FLAG) != 0
        meta_size = global_header & META_SIZE_MASKS
        if meta_size >= META_SIZE_MASKS:
            extra = buffer.read_varuint32()
            meta_size += extra

        if is_compressed:
            raise NotImplementedError()

        if not has_fields_meta:
            raise NotImplementedError()

        sub_header = buffer.read_uint8()
        fields_num = sub_header & NUM_FIELDS_SIZE_THRESHOLD
        if fields_num == NUM_FIELDS_SIZE_THRESHOLD:
            fields_num += buffer.read_varuint32()

        true_fields_num = fields_num + 1

        is_register_by_name = (sub_header & REGISTER_BY_NAME_FLAG) >> 5

        namespace = None
        type_name = None
        type_id = None
        cls = None
        if is_register_by_name:
            raise NotImplementedError()
            namespace = None
            type_name = None
        else:
            type_id = buffer.read_varuint32()
            cls = None
        field_infos = list()
        for _ in range(true_fields_num):
            field_info = FieldInfo.xread(buffer, "defined_class")
            field_infos.append(field_info)
        return TypeInfo(
            field_infos,
            type_id,
            cls,
            namespace,
            type_name
        )


