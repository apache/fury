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

import ClassResolver from "./classResolver";
import { BinaryWriter } from "./writer";
import { BinaryReader } from "./reader";
import { ReferenceResolver } from "./referenceResolver";
import { ConfigFlags, Serializer, Config, Language, MAGIC_NUMBER } from "./type";
import { OwnershipError } from "./error";
import { InputType, ResultType, TypeDescription } from "./description";
import { generateSerializer, AnySerializer } from "./gen";

export default class {
  binaryReader: BinaryReader;
  binaryWriter: BinaryWriter;
  classResolver = new ClassResolver();
  referenceResolver: ReferenceResolver;
  anySerializer: AnySerializer;

  constructor(public config: Config = {
    refTracking: false,
    useSliceString: false,
    hooks: {
    },
  }) {
    this.binaryReader = new BinaryReader(config);
    this.binaryWriter = new BinaryWriter(config);
    this.referenceResolver = new ReferenceResolver(this.binaryReader);
    this.classResolver.init(this);
    this.anySerializer = new AnySerializer(this);
  }

  registerSerializer<T extends TypeDescription>(description: T) {
    const serializer = generateSerializer(this, description);
    return {
      serializer,
      serialize: (data: InputType<T>) => {
        return this.serialize(data, serializer);
      },
      serializeVolatile: (data: InputType<T>) => {
        return this.serializeVolatile(data, serializer);
      },
      deserialize: (bytes: Uint8Array) => {
        return this.deserialize(bytes, serializer) as ResultType<T>;
      },
    };
  }

  deserialize<T = any>(bytes: Uint8Array, serializer: Serializer = this.anySerializer): T | null {
    this.referenceResolver.reset();
    this.classResolver.reset();
    this.binaryReader.reset(bytes);
    if (this.binaryReader.int16() !== MAGIC_NUMBER) {
      throw new Error("the fury xlang serialization must start with magic number 0x%x. Please check whether the serialization is based on the xlang protocol and the data didn't corrupt");
    }
    const bitmap = this.binaryReader.uint8();
    if ((bitmap & ConfigFlags.isNullFlag) === ConfigFlags.isNullFlag) {
      return null;
    }
    const isLittleEndian = (bitmap & ConfigFlags.isLittleEndianFlag) === ConfigFlags.isLittleEndianFlag;
    if (!isLittleEndian) {
      throw new Error("big endian is not supported now");
    }
    const isCrossLanguage = (bitmap & ConfigFlags.isCrossLanguageFlag) == ConfigFlags.isCrossLanguageFlag;
    if (!isCrossLanguage) {
      throw new Error("support crosslanguage mode only");
    }
    const isOutOfBandEnabled = (bitmap & ConfigFlags.isOutOfBandFlag) === ConfigFlags.isOutOfBandFlag;
    if (isOutOfBandEnabled) {
      throw new Error("outofband mode is not supported now");
    }

    this.binaryReader.uint8(); // skip language
    this.binaryReader.int32(); // native object offset. should skip.  javascript support cross mode only
    this.binaryReader.int32(); // native object size. should skip.
    return serializer.read();
  }

  private serializeInternal<T = any>(data: T, serializer: Serializer) {
    try {
      this.binaryWriter.reset();
    } catch (e) {
      if (e instanceof OwnershipError) {
        throw new Error("Permission denied. To release the serialization ownership, you must call the dispose function returned by serializeVolatile.");
      }
      throw e;
    }
    this.referenceResolver.reset();
    this.classResolver.reset();
    let bitmap = 0;
    if (data === null) {
      bitmap |= ConfigFlags.isNullFlag;
    }
    bitmap |= ConfigFlags.isLittleEndianFlag;
    bitmap |= ConfigFlags.isCrossLanguageFlag;
    this.binaryWriter.int16(MAGIC_NUMBER);
    this.binaryWriter.uint8(bitmap);
    this.binaryWriter.uint8(Language.XLANG);
    const cursor = this.binaryWriter.getCursor();
    this.binaryWriter.skip(4); // preserve 4-byte for nativeObjects start offsets.
    this.binaryWriter.uint32(0); // nativeObjects length.
    // reserve fixed size
    this.binaryWriter.reserve(serializer.meta.fixedSize);
    // start write
    serializer.write(data);
    this.binaryWriter.setUint32Position(cursor, this.binaryWriter.getCursor()); // nativeObjects start offsets;
    return this.binaryWriter;
  }

  serialize<T = any>(data: T, serializer: Serializer = this.anySerializer) {
    return this.serializeInternal(data, serializer).dump();
  }

  serializeVolatile<T = any>(data: T, serializer: Serializer = this.anySerializer) {
    return this.serializeInternal(data, serializer).dumpAndOwn();
  }
}
