/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { arraySerializer, stringArraySerializer, boolArraySerializer, shortArraySerializer, intArraySerializer, longArraySerializer, floatArraySerializer, doubleArraySerializer } from "./internalSerializer/array";
import stringSerializer from "./internalSerializer/string";
import binarySerializer from "./internalSerializer/binary";
import { dateSerializer, timestampSerializer } from "./internalSerializer/datetime";
import mapSerializer from "./internalSerializer/map";
import setSerializer from "./internalSerializer/set";
import boolSerializer from "./internalSerializer/bool";
import { uInt16Serializer, int16Serializer, int32Serializer, uInt32Serializer, uInt64Serializer, floatSerializer, doubleSerializer, uInt8Serializer, int64Serializer, int8Serializer } from "./internalSerializer/number";
import { InternalSerializerType, Serializer, SerializerRead, SerializerWrite, Fury, BinaryReader, BinaryWriter, SerializerConfig } from "./type";


const USESTRINGVALUE = 0;
const USESTRINGID = 1

const unreachable = () => {
    throw new Error('unreachable serializer')
}

export const unreachableSerializer = () => {
    return {
        read: unreachable,
        write: unreachable
    }
}
export default class SerializerResolver {
    private internalSerializer: Serializer[] = new Array(300).fill(unreachableSerializer());
    private customSerializer: { [key: string]: Serializer } = {
    };
    private readStringPool: string[] = [];
    private writeStringIndex: string[] = [];

    private initInternalSerializer(fury: Fury) {
        this.internalSerializer[InternalSerializerType.STRING] = stringSerializer(fury);
        this.internalSerializer[InternalSerializerType.ARRAY] = arraySerializer(fury);
        this.internalSerializer[InternalSerializerType.MAP] = mapSerializer(fury);
        this.internalSerializer[InternalSerializerType.BOOL] = boolSerializer(fury);
        this.internalSerializer[InternalSerializerType.UINT8] = uInt8Serializer(fury);
        this.internalSerializer[InternalSerializerType.INT8] = int8Serializer(fury);
        this.internalSerializer[InternalSerializerType.UINT16] = uInt16Serializer(fury);
        this.internalSerializer[InternalSerializerType.INT16] = int16Serializer(fury);
        this.internalSerializer[InternalSerializerType.UINT32] = uInt32Serializer(fury);
        this.internalSerializer[InternalSerializerType.INT32] = int32Serializer(fury);
        this.internalSerializer[InternalSerializerType.UINT64] = uInt64Serializer(fury);
        this.internalSerializer[InternalSerializerType.INT64] = int64Serializer(fury);
        this.internalSerializer[InternalSerializerType.FLOAT] = floatSerializer(fury);
        this.internalSerializer[InternalSerializerType.DOUBLE] = doubleSerializer(fury);
        this.internalSerializer[InternalSerializerType.TIMESTAMP] = timestampSerializer(fury);
        this.internalSerializer[InternalSerializerType.DATE] = dateSerializer(fury);
        this.internalSerializer[InternalSerializerType.FURY_SET] = setSerializer(fury);
        this.internalSerializer[InternalSerializerType.FURY_STRING_ARRAY] = stringArraySerializer(fury);
        this.internalSerializer[InternalSerializerType.FURY_PRIMITIVE_BOOL_ARRAY] = boolArraySerializer(fury);
        this.internalSerializer[InternalSerializerType.FURY_PRIMITIVE_SHORT_ARRAY] = shortArraySerializer(fury);
        this.internalSerializer[InternalSerializerType.FURY_PRIMITIVE_INT_ARRAY] = intArraySerializer(fury);
        this.internalSerializer[InternalSerializerType.FURY_PRIMITIVE_LONG_ARRAY] = longArraySerializer(fury);
        this.internalSerializer[InternalSerializerType.FURY_PRIMITIVE_FLOAT_ARRAY] = floatArraySerializer(fury);
        this.internalSerializer[InternalSerializerType.FURY_PRIMITIVE_DOUBLE_ARRAY] = doubleArraySerializer(fury);
        this.internalSerializer[InternalSerializerType.BINARY] = binarySerializer(fury);
    }

    init(fury: Fury) {
        this.initInternalSerializer(fury);
    }

    reset() {
        this.readStringPool = [];
        this.writeStringIndex = [];
    }

    getSerializerById(id: InternalSerializerType) {
        return this.internalSerializer[id];
    }

    registerSerializerByTag(tag: string, serializer: Serializer) {
        if (this.customSerializer[tag]) {
            throw new Error(`${tag} has been registered`);
        }
        this.customSerializer[tag] = serializer;
    }

    registerReadSerializerByTag(tag: string, serializer: SerializerRead) {
        if (!this.customSerializer[tag]) {
            this.customSerializer[tag] = {
                read: unreachable,
                write: unreachable,
                config: unreachable,
            }
        }
        const exists = this.customSerializer[tag];
        if (exists?.read && exists?.read !== unreachable) {
            throw new Error(`${tag} write has been registered`);
        }
        exists.read = serializer;
    }

    registerWriteSerializerByTag(tag: string, serializer: SerializerWrite, config: ReturnType<SerializerConfig>) {
        if (!this.customSerializer[tag]) {
            this.customSerializer[tag] = {
                read: unreachable,
                write: unreachable,
                config: unreachable,
            }
        }
        const exists = this.customSerializer[tag];
        if (exists?.write && exists?.write !== unreachable) {
            throw new Error(`${tag} write has been registered`);
        }
        exists.write = serializer;
        exists.config = () => {
            return config;
        }
    }

    existsTagReadSerializer(tag: string) {
        return this.customSerializer[tag]?.read && this.customSerializer[tag]?.read !== unreachable;
    }

    existsTagWriteSerializer(tag: string) {
        return this.customSerializer[tag]?.write && this.customSerializer[tag]?.write !== unreachable;
    }

    getSerializerByTag(tag: string) {
        return this.customSerializer[tag];
    }

    writeTag(binaryWriter: BinaryWriter, tag: string, tagBuffer: Buffer, bufferLen: number) {
        const index = this.writeStringIndex.indexOf(tag);
        if (index > -1) {
            binaryWriter.writeUInt8(USESTRINGID)
            binaryWriter.writeInt16(index);
            return;
        }
        this.writeStringIndex.push(tag);
        binaryWriter.writeUInt8(USESTRINGVALUE);
        binaryWriter.skip(8); // todo: support tag hash. skip
        binaryWriter.writeUtf8StringOfInt16(tagBuffer, bufferLen);
    }


    readTag(binaryView: BinaryReader) {
        const flag = binaryView.readUInt8();
        if (flag === USESTRINGVALUE) {
            // todo: support tag hash. skip
            binaryView.skip(8);
            const str = binaryView.readStringUtf8(binaryView.readInt16());
            this.readStringPool.push(str);
            return str
        } else {
            return this.readStringPool[binaryView.readInt16()]
        }
    }
}