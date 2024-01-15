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
import { ConfigFlags, Serializer, Config, InternalSerializerType, Language } from "./type";
import { OwnershipError } from "./error";

export default (config: Config) => {
  const binaryReader = BinaryReader(config);
  const binaryWriter = BinaryWriter(config);

  const classResolver = new ClassResolver();
  const referenceResolver = ReferenceResolver(config, binaryWriter, binaryReader);

  const fury = {
    config,
    deserialize,
    serialize,
    referenceResolver,
    classResolver,
    binaryReader,
    binaryWriter,
    serializeVolatile,
  };
  classResolver.init(fury);

  function deserialize<T = any>(bytes: Uint8Array, serializer?: Serializer): T | null {
    referenceResolver.reset();
    classResolver.reset();
    binaryReader.reset(bytes);
    const bitmap = binaryReader.uint8();
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
    binaryReader.uint8(); // skip language
    const isOutOfBandEnabled = (bitmap & ConfigFlags.isOutOfBandFlag) === ConfigFlags.isOutOfBandFlag;
    if (isOutOfBandEnabled) {
      throw new Error("outofband mode is not supported now");
    }
    binaryReader.int32(); // native object offset. should skip.  javascript support cross mode only
    binaryReader.int32(); // native object size. should skip.
    if (serializer) {
      return serializer.read();
    } else {
      return classResolver.getSerializerById(InternalSerializerType.ANY).read();
    }
  }

  function serializeInternal<T = any>(data: T, serializer?: Serializer) {
    try {
      binaryWriter.reset();
    } catch (e) {
      if (e instanceof OwnershipError) {
        throw new Error("Permission denied. To release the serialization ownership, you must call the dispose function returned by serializeVolatile.");
      }
      throw e;
    }
    referenceResolver.reset();
    classResolver.reset();
    let bitmap = 0;
    if (data === null) {
      bitmap |= ConfigFlags.isNullFlag;
    }
    bitmap |= ConfigFlags.isLittleEndianFlag;
    bitmap |= ConfigFlags.isCrossLanguageFlag;
    binaryWriter.uint8(bitmap);
    binaryWriter.uint8(Language.XLANG);
    const cursor = binaryWriter.getCursor();
    binaryWriter.skip(4); // preserve 4-byte for nativeObjects start offsets.
    binaryWriter.uint32(0); // nativeObjects length.
    if (!serializer) {
      serializer = classResolver.getSerializerById(InternalSerializerType.ANY);
    }
    // reserve fixed size
    binaryWriter.reserve(serializer.meta.fixedSize);
    // start write
    serializer.write(data);

    binaryWriter.setUint32Position(cursor, binaryWriter.getCursor()); // nativeObjects start offsets;
    return binaryWriter;
  }

  function serialize<T = any>(data: T, serializer?: Serializer) {
    return serializeInternal(data, serializer).dump();
  }

  function serializeVolatile<T = any>(data: T, serializer?: Serializer) {
    return serializeInternal(data, serializer).dumpAndOwn();
  }

  return fury;
};
