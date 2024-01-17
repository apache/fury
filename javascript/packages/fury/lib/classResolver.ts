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

import { InternalSerializerType, Serializer, BinaryReader, BinaryWriter as TBinaryWriter, USESTRINGID, USESTRINGVALUE } from "./type";
import { generateSerializer } from "./gen";
import { Type, TypeDescription } from "./description";
import Fury from "./fury";
import { tagBuffer } from "./meta";

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
    return fury.classResolver.registerSerializerById(description.type, generateSerializer(fury, description));
  }

  private initInternalSerializer(fury: Fury) {
    this.registerSerializer(fury, Type.string());
    this.registerSerializer(fury, Type.array(Type.any()));
    this.registerSerializer(fury, Type.map(Type.any(), Type.any()));
    this.registerSerializer(fury, Type.bool());
    this.registerSerializer(fury, Type.uint8());
    this.registerSerializer(fury, Type.int8());
    this.registerSerializer(fury, Type.uint16());
    this.registerSerializer(fury, Type.int16());
    this.registerSerializer(fury, Type.uint32());
    this.registerSerializer(fury, Type.int32());
    this.registerSerializer(fury, Type.uint64());
    this.registerSerializer(fury, Type.int64());
    this.registerSerializer(fury, Type.float());
    this.registerSerializer(fury, Type.double());
    this.registerSerializer(fury, Type.timestamp());
    this.registerSerializer(fury, Type.date());
    this.registerSerializer(fury, Type.set(Type.any()));
    this.registerSerializer(fury, Type.binary());
    this.registerSerializer(fury, Type.stringTypedArray());
    this.registerSerializer(fury, Type.boolTypedArray());
    this.registerSerializer(fury, Type.shortTypedArray());
    this.registerSerializer(fury, Type.intTypedArray());
    this.registerSerializer(fury, Type.longTypedArray());
    this.registerSerializer(fury, Type.floatTypedArray());
    this.registerSerializer(fury, Type.doubleTypedArray());
  }

  init(fury: Fury) {
    this.initInternalSerializer(fury);
  }

  reset() {
    this.readStringPool = [];
    this.writeStringIndex.fill(-1);
  }

  getSerializerById(id: InternalSerializerType) {
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

  createTagWriter(tag: string) {
    this.writeStringIndex.push(-1);
    const idx = this.writeStringIndex.length - 1;
    const fullBuffer = tagBuffer(tag);

    return {
      write: (binaryWriter: TBinaryWriter) => {
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
    if (v === null || v === undefined) {
      return null;
    }

    if (typeof v === "number") {
      return this.getSerializerById(InternalSerializerType.DOUBLE);
    }

    if (typeof v === "bigint") {
      return this.getSerializerById(InternalSerializerType.INT64);
    }

    if (typeof v === "boolean") {
      return this.getSerializerById(InternalSerializerType.BOOL);
    }

    if (v instanceof Date) {
      return this.getSerializerById(InternalSerializerType.TIMESTAMP);
    }

    if (typeof v === "string") {
      return this.getSerializerById(InternalSerializerType.STRING);
    }

    if (v instanceof Map) {
      return this.getSerializerById(InternalSerializerType.MAP);
    }

    if (v instanceof Set) {
      return this.getSerializerById(InternalSerializerType.FURY_SET);
    }

    if (Array.isArray(v)) {
      return this.getSerializerById(InternalSerializerType.ARRAY);
    }

    throw new Error(`Failed to detect the Fury type from JavaScript type: ${typeof v}`);
  }
}
