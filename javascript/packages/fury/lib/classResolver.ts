/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { InternalSerializerType, Serializer } from "./type";
import { fromString } from "./platformBuffer";
import { x64hash128 } from "./murmurHash3";
import { BinaryWriter } from "./writer";
import { generateSerializer } from "./gen";
import { Type, TypeDescription } from "./description";
import Fury from "./fury";
import { BinaryReader } from "./reader";

const USESTRINGVALUE = 0;
const USESTRINGID = 1;

class LazyString {
  private string: string | null = null;
  private start: number | null = null;
  private len: number | null = null;

  static fromPair(start: number, len: number) {
    const result = new LazyString();
    result.start = start;
    result.len = len;
    return result;
  }

  static fromString(str: string) {
    const result = new LazyString();
    result.string = str;
    return result;
  }

  toString(binaryReader: BinaryReader) {
    if (this.string == null) {
      this.string = binaryReader.stringUtf8At(this.start!, this.len!);
    }
    return this.string;
  }
}

const uninitSerialize = {
  read: () => {
    throw new Error("uninitSerialize");
  },
  write: () => {
    throw new Error("uninitSerialize");
  },
  readInner: () => {
    throw new Error("uninitSerialize");
  },
  writeInner: () => {
    throw new Error("uninitSerialize");
  },
  meta: {
    fixedSize: 0,
    type: InternalSerializerType.ANY,
    needToWriteRef: false,
    typeId: null,
  },
};

export default class SerializerResolver {
  private internalSerializer: Serializer[] = new Array(300);
  private customSerializer: { [key: string]: Serializer } = {
  };

  private readStringPool: LazyString[] = [];
  private writeStringCount = 0;
  private writeStringIndex: number[] = [];

  private registerSerializer(fury: Fury, description: TypeDescription) {
    return fury.classResolver.registerSerializerById(SerializerResolver.getTypeIdByInternalSerializerType(description.type), generateSerializer(fury, description));
  }

  private initInternalSerializer(fury: Fury) {
    this.registerSerializer(fury, Type.string());
    this.registerSerializer(fury, Type.array(Type.any()));
    this.registerSerializer(fury, Type.map(Type.any(), Type.any()));
    this.registerSerializer(fury, Type.bool());
    this.registerSerializer(fury, Type.int8());
    this.registerSerializer(fury, Type.int16());
    this.registerSerializer(fury, Type.int32());
    this.registerSerializer(fury, Type.varInt32());
    this.registerSerializer(fury, Type.int64());
    this.registerSerializer(fury, Type.sliInt64());
    this.registerSerializer(fury, Type.float16());
    this.registerSerializer(fury, Type.float32());
    this.registerSerializer(fury, Type.float64());
    this.registerSerializer(fury, Type.timestamp());
    this.registerSerializer(fury, Type.duration());
    this.registerSerializer(fury, Type.set(Type.any()));
    this.registerSerializer(fury, Type.binary());
    this.registerSerializer(fury, Type.boolArray());
    this.registerSerializer(fury, Type.int8Array());
    this.registerSerializer(fury, Type.int16Array());
    this.registerSerializer(fury, Type.int32Array());
    this.registerSerializer(fury, Type.int64Array());
    this.registerSerializer(fury, Type.float16Array());
    this.registerSerializer(fury, Type.float32Array());
    this.registerSerializer(fury, Type.float64Array());

    this.numberSerializer = this.getSerializerById(SerializerResolver.getTypeIdByInternalSerializerType(InternalSerializerType.FLOAT64));
    this.int64Serializer = this.getSerializerById(SerializerResolver.getTypeIdByInternalSerializerType(InternalSerializerType.INT64));
    this.boolSerializer = this.getSerializerById(SerializerResolver.getTypeIdByInternalSerializerType(InternalSerializerType.BOOL));
    this.dateSerializer = this.getSerializerById(SerializerResolver.getTypeIdByInternalSerializerType(InternalSerializerType.TIMESTAMP));
    this.stringSerializer = this.getSerializerById(SerializerResolver.getTypeIdByInternalSerializerType(InternalSerializerType.STRING));
    this.setSerializer = this.getSerializerById(SerializerResolver.getTypeIdByInternalSerializerType(InternalSerializerType.SET));
    this.arraySerializer = this.getSerializerById(SerializerResolver.getTypeIdByInternalSerializerType(InternalSerializerType.ARRAY));
    this.mapSerializer = this.getSerializerById(SerializerResolver.getTypeIdByInternalSerializerType(InternalSerializerType.MAP));
  }

  private numberSerializer: null | Serializer = null;
  private int64Serializer: null | Serializer = null;
  private boolSerializer: null | Serializer = null;
  private dateSerializer: null | Serializer = null;
  private stringSerializer: null | Serializer = null;
  private setSerializer: null | Serializer = null;
  private arraySerializer: null | Serializer = null;
  private mapSerializer: null | Serializer = null;

  init(fury: Fury) {
    this.initInternalSerializer(fury);
  }

  reset() {
    this.readStringPool = [];
    this.writeStringIndex.fill(-1);
  }

  getSerializerByType(type: InternalSerializerType) {
    return this.internalSerializer[SerializerResolver.getTypeIdByInternalSerializerType(type)];
  }

  getSerializerById(id: number) {
    return this.internalSerializer[id];
  }

  registerSerializerById(id: number, serializer: Serializer) {
    if (this.internalSerializer[id]) {
      Object.assign(this.internalSerializer[id], serializer);
    } else {
      this.internalSerializer[id] = { ...serializer };
    }
    return this.internalSerializer[id];
  }

  registerSerializerByTag(tag: string, serializer: Serializer = uninitSerialize) {
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

  static tagBuffer(tag: string) {
    const tagBuffer = fromString(tag);
    const bufferLen = tagBuffer.byteLength;
    const writer = new BinaryWriter({});

    let tagHash = x64hash128(tagBuffer, 47).getBigUint64(0);
    if (tagHash === 0n) {
      tagHash = 1n;
    }

    writer.uint8(USESTRINGVALUE);
    writer.uint64(tagHash);
    writer.int16(bufferLen);
    writer.bufferWithoutMemCheck(tagBuffer, bufferLen);
    return writer.dump();
  }

  createTagWriter(tag: string) {
    this.writeStringIndex.push(-1);
    const idx = this.writeStringIndex.length - 1;
    const fullBuffer = SerializerResolver.tagBuffer(tag);

    return {
      write: (binaryWriter: BinaryWriter) => {
        const tagIndex = this.writeStringIndex[idx];
        if (tagIndex > -1) {
          // equivalent of: `uint8(USESTRINGID); int16(tagIndex)`
          binaryWriter.int24((tagIndex << 8) | USESTRINGID);
          return;
        }

        this.writeStringIndex[idx] = this.writeStringCount++;
        binaryWriter.buffer(fullBuffer);
      },
    };
  }

  readTag(binaryReader: BinaryReader) {
    const flag = binaryReader.uint8();
    if (flag === USESTRINGVALUE) {
      binaryReader.skip(8); // The tag hash is not needed at the moment.
      const len = binaryReader.int16();
      const start = binaryReader.getCursor();
      binaryReader.skip(len);
      this.readStringPool.push(LazyString.fromPair(start, len));
      const idx = this.readStringPool.length;
      return () => {
        return this.readStringPool[idx - 1].toString(binaryReader);
      };
    } else {
      const idx = binaryReader.int16();
      return () => {
        return this.readStringPool[idx].toString(binaryReader);
      };
    }
  }

  getSerializerByData(v: any) {
    if (typeof v === "number") {
      return this.numberSerializer;
    }

    if (typeof v === "string") {
      return this.stringSerializer;
    }

    if (Array.isArray(v)) {
      return this.arraySerializer;
    }

    if (typeof v === "boolean") {
      return this.boolSerializer;
    }

    if (typeof v === "bigint") {
      return this.int64Serializer;
    }

    if (v instanceof Date) {
      return this.dateSerializer;
    }

    if (v instanceof Map) {
      return this.mapSerializer;
    }

    if (v instanceof Set) {
      return this.setSerializer;
    }
    throw new Error(`Failed to detect the Fury type from JavaScript type: ${typeof v}`);
  }

  static getTypeIdByInternalSerializerType(type: InternalSerializerType) {
    switch (type) {
      case InternalSerializerType.BOOL:
        return 1;
      case InternalSerializerType.INT8:
        return 2;
      case InternalSerializerType.INT16:
        return 3;
      case InternalSerializerType.INT32:
        return 4;
      case InternalSerializerType.VAR_INT32:
        return 5;
      case InternalSerializerType.INT64:
        return 6;
      case InternalSerializerType.VAR_INT64:
        return 7;
      case InternalSerializerType.SLI_INT64:
        return 8;
      case InternalSerializerType.FLOAT16:
        return 9;
      case InternalSerializerType.FLOAT32:
        return 10;
      case InternalSerializerType.FLOAT64:
        return 11;
      case InternalSerializerType.STRING:
        return 12;
      case InternalSerializerType.ENUM:
        return 13;
      case InternalSerializerType.LIST:
        return 14;
      case InternalSerializerType.SET:
        return 15;
      case InternalSerializerType.MAP:
        return 16;
      case InternalSerializerType.DURATION:
        return 17;
      case InternalSerializerType.TIMESTAMP:
        return 18;
      case InternalSerializerType.DECIMAL:
        return 19;
      case InternalSerializerType.BINARY:
        return 20;
      case InternalSerializerType.TUPLE:
      case InternalSerializerType.ARRAY:
        return 21;
      case InternalSerializerType.BOOL_ARRAY:
        return 22;
      case InternalSerializerType.INT8_ARRAY:
        return 23;
      case InternalSerializerType.INT16_ARRAY:
        return 24;
      case InternalSerializerType.INT32_ARRAY:
        return 25;
      case InternalSerializerType.INT64_ARRAY:
        return 26;
      case InternalSerializerType.FLOAT16_ARRAY:
        return 27;
      case InternalSerializerType.FLOAT32_ARRAY:
        return 28;
      case InternalSerializerType.FLOAT64_ARRAY:
        return 29;
      case InternalSerializerType.OBJECT: // todo
        return 256;
      default:
        throw new Error(`typeId is not assigned to type ${InternalSerializerType[type]}`);
    }
  }
}
