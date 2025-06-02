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
from pyfory.type import TypeId
from .metastring import MetaStringEncoder, MetaStringDecoder, Encoding

XLANG_FIELD_NAME_SIZE_THRESHOLD = 0b1111
USE_TAG_AS_FIELD = 0b11

@dataclass
class FieldType:
    def __init__(self, type_id: int, is_monomorphic: bool, nullable: bool, tracking_ref: bool):
        self.type_id = type_id
        self.is_monomorphic = is_monomorphic
        self.nullable = nullable
        self.tracking_ref = tracking_ref

    def xwrite(self, buffer:"Buffer", write_header:bool=False):
        type_id = self.type_id
        if write_header:
            header = type_id << 2
            if self.tracking_ref:
                header |= 1
            if self.nullable:
                header |= 0b10
            type_id = header
        buffer.write_varuint32(type_id)
        if type_id in (TypeId.LIST, TypeId.SET):
            assert isinstance(self, CollectionFieldType)
            self.element_type.xwrite(buffer, True)
        elif type_id == TypeId.MAP:
            assert isinstance(self, MapFieldType)
            self.key_type.xwrite(buffer, True)
            self.value_type.xwrite(buffer, True)
        else:
            pass

    @staticmethod
    def xread(buffer, read_header:bool=False):
        header = buffer.read_varuint32()
        tracking_ref = None
        nullable = None
        if not read_header:
            tracking_ref = header | 1
            nullable = header | 0b10
            type_id = header | 0b100
        else:
            type_id = header

        field_type = None
        if type_id in (TypeId.LIST, TypeId.SET):
            field_type = CollectionFieldType(type_id, True, nullable,tracking_ref, FieldType.xread(buffer, True))
        elif type_id == TypeId.MAP:
            field_type = MapFieldType(type_id,True, nullable,tracking_ref, FieldType.xread(buffer, True), FieldType.xread(buffer, True))
        else:
            field_type = FieldType(type_id, False, nullable, tracking_ref)
        return field_type


@dataclass
class CollectionFieldType(FieldType):
    def __init__(self, type_id: int, is_monomorphic: bool, nullable: bool, tracking_ref:bool, element_type:FieldType):
        super().__init__(type_id,is_monomorphic,nullable,tracking_ref)
        self.element_type = element_type

@dataclass
class MapFieldType(FieldType):
    def __init__(self, type_id: int, is_monomorphic: bool, nullable: bool, tracking_ref:bool, key_type:FieldType, value_type:FieldType):
        super().__init__(type_id,is_monomorphic,nullable,tracking_ref)
        self.key_type = key_type
        self.value_type = value_type


# FieldInfo: {in_what_class, name, type<sub_type>}
@dataclass
class FieldInfo:
    field_name_encoder: MetaStringEncoder = MetaStringEncoder("$", "_")
    field_name_decoder: MetaStringDecoder = MetaStringDecoder("$", "_")
    field_name_encodings = [Encoding.UTF_8, Encoding.LOWER_UPPER_DIGIT_SPECIAL, Encoding.ALL_TO_LOWER_SPECIAL]
    __slots__ = (
        "defined_class",
        "field_name",
        "field_type",
    )

    def __init__(self, defined_class:str, field_name:str, field_type:FieldType):
        self.defined_class = defined_class
        self.field_name = field_name
        self.field_type = field_type

    # Returns annotated tag id for the field.
    def get_tag(self):
        return None

    @staticmethod
    def xread(buffer: "Buffer", defined_class:str):
        header = buffer.read_uint8()
        tracking_ref = header & 1
        nullable = (header >> 1) & 1
        encoding_idx = header >> 6
        if encoding_idx == USE_TAG_AS_FIELD:
            raise NotImplementedError("Type tag not supported currently")
        else:
            size = (header >> 2) & XLANG_FIELD_NAME_SIZE_THRESHOLD
            if size == XLANG_FIELD_NAME_SIZE_THRESHOLD:
                size += buffer.read_varuint32()

        field_type = FieldType.xread(buffer)
        field_type.tracking_ref = tracking_ref
        field_type.nullable = nullable

        field_name = None
        if encoding_idx != USE_TAG_AS_FIELD:
            encoding = FieldInfo.field_name_encodings[encoding_idx]
            field_name = FieldInfo.field_name_decoder.decode(buffer.read_bytes(size), encoding)

        return FieldInfo(defined_class, field_name, field_type)


    def xwrite(self, buffer: "Buffer"):
        '''
        field_info:
            header(8bits): size(3bits) + field_name_encoding(2bits) + polymorphism_flag(1bit) + nullability_flag(1bit) + ref_tracking_flag(1bit)
            type_id: field_type_id(if have sub_type, write as well, but should write its header)
            field_name: has_tag()? 0bit: field_name_bytes
        '''
        field_type = self.field_type
        header = field_type.tracking_ref
        header |= field_type.nullable << 1
        # if exist tag, use tag_id as field_name
        tag = self.get_tag()
        field_name_bytes = None
        if tag:
            raise NotImplementedError("Type tag not supported currently")
            header |= tag << 2
            header |= USE_TAG_AS_FIELD << 6
            buffer.write_uint8(header)
        else:
            field_name_meta_str = FieldInfo.field_name_encoder.encode(self.field_name, FieldInfo.field_name_encodings)
            encoding_idx = FieldInfo.field_name_encodings.index(field_name_meta_str.encoding)
            header |= encoding_idx << 6
            field_name_bytes = field_name_meta_str.encoded_data
            size = len(field_name_bytes)
            header |= (size & XLANG_FIELD_NAME_SIZE_THRESHOLD) << 2
            buffer.write_uint8(header)
            if size >= XLANG_FIELD_NAME_SIZE_THRESHOLD:
                buffer.write_varuint32(size-XLANG_FIELD_NAME_SIZE_THRESHOLD)

        field_type.xwrite(buffer)
        if field_name_bytes:
            buffer.write_bytes(field_name_bytes)
