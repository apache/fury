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
import { InternalSerializerType, Serializer, Fury, BinaryReader, BinaryWriter } from "./type";
import anySerializer from "./internalSerializer/any";
import { PlatformBuffer, fromUint8Array } from "./platformBuffer";
import { x64hash128 } from "./murmurHash3";
import { utf8Encoder } from "./util";

const USESTRINGVALUE = 0;
const USESTRINGID = 1;

export default class SerializerResolver {
  private internalSerializer: Serializer[] = new Array(300);
  private customSerializer: { [key: string]: Serializer } = {
  };

  private readStringPool: string[] = [];
  private writeStringCount = 0;
  private writeStringIndex: Record<string, number> = {};

  private initInternalSerializer(fury: Fury) {
    const _anySerializer = anySerializer(fury);
    this.internalSerializer[InternalSerializerType.ANY] = _anySerializer;
    this.internalSerializer[InternalSerializerType.STRING] = stringSerializer(fury);
    this.internalSerializer[InternalSerializerType.ARRAY] = arraySerializer(fury, _anySerializer);
    this.internalSerializer[InternalSerializerType.MAP] = mapSerializer(fury, _anySerializer, _anySerializer);
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
    this.internalSerializer[InternalSerializerType.FURY_SET] = setSerializer(fury, anySerializer(fury));
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
    this.writeStringIndex = {};
  }

  getSerializerById(id: InternalSerializerType) {
    return this.internalSerializer[id];
  }

  registerSerializerByTag(tag: string, serializer: Serializer) {
    if (this.customSerializer[tag]) {
      Object.assign(this.customSerializer[tag], serializer);
    } else {
      this.customSerializer[tag] = { ...serializer };
    }
    return this.customSerializer[tag];
  }

  getSerializerByTag(tag: string) {
    return this.customSerializer[tag];
  }

  computedTag(tag: string) {
    const tagBuffer = fromUint8Array(utf8Encoder.encode(tag));
    let tagHash = x64hash128(tagBuffer, 47).getBigUint64(0);
    if (tagHash === BigInt(0)) {
      tagHash = BigInt(1);
    }
    return {
      tagHash,
      tagBuffer,
    };
  }

  writeTag(binaryWriter: BinaryWriter, tag: string, bf: PlatformBuffer, tagHash: number, bufferLen: number) {
    const index = this.writeStringIndex[tag];
    if (typeof index !== "undefined") {
      binaryWriter.uint8(USESTRINGID);
      binaryWriter.int16(index);
      return;
    }
    this.writeStringIndex[tag] = this.writeStringCount++;
    binaryWriter.uint8(USESTRINGVALUE);
    binaryWriter.uint64(tagHash);
    binaryWriter.int16(bufferLen);
    binaryWriter.bufferWithoutMemCheck(bf, bufferLen);
  }

  detectTag(binaryReader: BinaryReader) {
    const flag = binaryReader.uint8();
    if (flag === USESTRINGVALUE) {
      binaryReader.skip(8); // The tag hash is not needed at the moment.
      const str = binaryReader.stringUtf8(binaryReader.int16());
      return str;
    } else {
      return this.readStringPool[binaryReader.int16()];
    }
  }

  readTag(binaryReader: BinaryReader) {
    const flag = binaryReader.uint8();
    if (flag === USESTRINGVALUE) {
      binaryReader.skip(8); // The tag hash is not needed at the moment.
      const str = binaryReader.stringUtf8(binaryReader.int16());
      this.readStringPool.push(str);
      return str;
    } else {
      return this.readStringPool[binaryReader.int16()];
    }
  }
}
