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

import { Type } from "./description";
import { getMeta } from "./meta";
import { Fury, MaxInt32, MinInt32, Serializer } from "./type";
import { InternalSerializerType, RefFlags } from "./type";

export default (fury: Fury) => {
  const { binaryReader, binaryWriter, referenceResolver, classResolver } = fury;

  function detectSerializer() {
    const typeId = binaryReader.int16();
    let serializer: Serializer;
    if (typeId === InternalSerializerType.FURY_TYPE_TAG) {
      const tag = classResolver.readTag(binaryReader)();
      serializer = classResolver.getSerializerByTag(tag);
    } else {
      serializer = classResolver.getSerializerById(typeId);
    }
    if (!serializer) {
      throw new Error(`cant find implements of typeId: ${typeId}`);
    }
    return serializer;
  }

  return {
    readInner: () => {
      throw new Error("any can not call readInner");
    },
    writeInner: () => {
      throw new Error("any can not call writeInner");
    },
    read: () => {
      const flag = referenceResolver.readRefFlag();
      switch (flag) {
        case RefFlags.RefValueFlag:
          return detectSerializer().readInner();
        case RefFlags.RefFlag:
          return referenceResolver.getReadObjectByRefId(binaryReader.varUInt32());
        case RefFlags.NullFlag:
          return null;
        case RefFlags.NotNullValueFlag:
          return detectSerializer().readInner();
      }
    },
    write: (v: any) => {
      const { write: writeInt64, meta: int64Meta } = classResolver.getSerializerById(InternalSerializerType.INT64);
      const { write: writeDouble, meta: doubleMeta } = classResolver.getSerializerById(InternalSerializerType.DOUBLE);
      const { write: writeInt32, meta: int32Meta } = classResolver.getSerializerById(InternalSerializerType.INT32);
      const { write: writeBool, meta: boolMeta } = classResolver.getSerializerById(InternalSerializerType.BOOL);
      const { write: stringWrite, meta: stringMeta } = classResolver.getSerializerById(InternalSerializerType.STRING);
      const { write: arrayWrite, meta: arrayMeta } = classResolver.getSerializerById(InternalSerializerType.ARRAY);
      const { write: mapWrite, meta: mapMeta } = classResolver.getSerializerById(InternalSerializerType.MAP);
      const { write: setWrite, meta: setMeta } = classResolver.getSerializerById(InternalSerializerType.FURY_SET);
      const { write: timestampWrite, meta: timestampMeta } = classResolver.getSerializerById(InternalSerializerType.TIMESTAMP);

      // NullFlag
      if (v === null || v === undefined) {
        binaryWriter.reserve(1);
        binaryWriter.int8(RefFlags.NullFlag); // null
        return;
      }

      // NotNullValueFlag
      if (typeof v === "number") {
        if (Number.isNaN(v) || !Number.isFinite(v)) {
          binaryWriter.reserve(1);
          binaryWriter.int8(RefFlags.NullFlag); // null
          return;
        }
        if (Number.isInteger(v)) {
          if (v > MaxInt32 || v < MinInt32) {
            binaryWriter.reserve(int64Meta.fixedSize);
            writeInt64(BigInt(v));
            return;
          } else {
            binaryWriter.reserve(int32Meta.fixedSize);
            writeInt32(v);
            return;
          }
        } else {
          binaryWriter.reserve(doubleMeta.fixedSize);
          writeDouble(v);
          return;
        }
      }

      if (typeof v === "bigint") {
        binaryWriter.reserve(int64Meta.fixedSize);
        writeInt64(v);
        return;
      }

      if (typeof v === "boolean") {
        binaryWriter.reserve(boolMeta.fixedSize);
        writeBool(v);
        return;
      }

      if (v instanceof Date) {
        binaryWriter.reserve(timestampMeta.fixedSize);
        timestampWrite(v);
        return;
      }

      if (typeof v === "string") {
        binaryWriter.reserve(stringMeta.fixedSize);
        stringWrite(v);
        return;
      }

      if (v instanceof Map) {
        binaryWriter.reserve(5);
        binaryWriter.reserve(mapMeta.fixedSize);
        mapWrite(v);
        return;
      }
      if (v instanceof Set) {
        binaryWriter.reserve(setMeta.fixedSize);
        setWrite(v);
        return;
      }
      if (Array.isArray(v)) {
        binaryWriter.reserve(arrayMeta.fixedSize);
        arrayWrite(v);
        return;
      }

      throw new Error(`serializer not support ${typeof v} yet`);
    },
    meta: getMeta(Type.any(), fury),
  };
};
