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

import { Config } from "./type";
import { isNodeEnv } from "./util";
import { PlatformBuffer, alloc, fromUint8Array } from './platformBuffer';

export const BinaryReader = (config: Config) => {
  const sliceStringEnable = isNodeEnv && config.useSliceString;
  let cursor = 0;
  let dataView!: DataView;
  let buffer!: PlatformBuffer;
  let bigString: string;
  function reset(ab: Uint8Array) {
    buffer = fromUint8Array(ab);
    dataView = new DataView(buffer.buffer, buffer.byteOffset);
    if (sliceStringEnable) {
      bigString = buffer.latin1Slice(0, buffer.byteLength);
    }
    cursor = 0;
  }

  function uint8() {
    return dataView.getUint8(cursor++);
  }

  function int8() {
    return dataView.getInt8(cursor++);
  }

  function uint16() {
    const result = dataView.getUint16(cursor, true);
    cursor += 2;
    return result;
  }

  function int16() {
    const result = dataView.getInt16(cursor, true);
    cursor += 2;
    return result;
  }

  function skip(len: number) {
    cursor += len;
  }

  function int32() {
    const result = dataView.getInt32(cursor, true);
    cursor += 4;
    return result;
  }

  function uint32() {
    const result = dataView.getUint32(cursor, true);
    cursor += 4;
    return result;
  }

  function int64() {
    const result = dataView.getBigInt64(cursor, true);
    cursor += 8;
    return result;
  }

  function uint64() {
    const result = dataView.getBigUint64(cursor, true);
    cursor += 8;
    return result;
  }

  function float() {
    const result = dataView.getFloat32(cursor, true);
    cursor += 4;
    return result;
  }

  function double() {
    const result = dataView.getFloat64(cursor, true);
    cursor += 8;
    return result;
  }

  function stringUtf8(len: number) {
    const result = buffer.utf8Slice(cursor, cursor + len);
    cursor += len;
    return result;
  }

  function stringLatin1Fast(len: number) {
    const result = bigString.substring(cursor, cursor + len);
    cursor += len;
    return result;
  }

  function stringLatin1Slow(len: number) {
    const result = buffer.latin1Slice(cursor, cursor + len);
    cursor += len;
    return result;
  }

  function binary(len: number) {
    const result = alloc(len);
    buffer.copy(result, 0, cursor, cursor + len);
    cursor += len;
    return result;
  }

  function varInt32() {
    let byte_ = int8();
    let result = byte_ & 0x7f;
    if ((byte_ & 0x80) != 0) {
      byte_ = int8();
      result |= (byte_ & 0x7f) << 7;
      if ((byte_ & 0x80) != 0) {
        byte_ = int8();
        result |= (byte_ & 0x7f) << 14;
        if ((byte_ & 0x80) != 0) {
          byte_ = int8();
          result |= (byte_ & 0x7f) << 21;
          if ((byte_ & 0x80) != 0) {
            byte_ = int8();
            result |= (byte_ & 0x7f) << 28;
          }
        }
      }
    }
    return result;
  }

  return {
    getCursor: () => cursor,
    setCursor: (v: number) => (cursor = v),
    varInt32,
    int8,
    buffer: binary,
    uint8,
    reset,
    stringUtf8,
    stringLatin1: sliceStringEnable ? stringLatin1Fast : stringLatin1Slow,
    double,
    float,
    uint16,
    int16,
    uint64,
    skip,
    int64,
    uint32,
    int32,
  };
};
