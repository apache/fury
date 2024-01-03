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

import { Config, LATIN1 } from "./type";
import { isNodeEnv } from "./util";
import { PlatformBuffer, alloc, fromUint8Array } from "./platformBuffer";
import { read1, read10, read11, read12, read13, read14, read15, read2, read3, read4, read5, read6, read7, read8, read9 } from "./string";

export const BinaryReader = (config: Config) => {
  const sliceStringEnable = isNodeEnv && config.useSliceString;
  let cursor = 0;
  let dataView!: DataView;
  let buffer!: PlatformBuffer;
  let bigString: string;

  const stringLatin1 = sliceStringEnable ? stringLatin1Fast : stringLatin1Slow;

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

  function sliLong() {
    const i = dataView.getUint32(cursor, true);
    if ((i & 0b1) != 0b1) {
      cursor += 4;
      return BigInt(i >> 1);
    }
    cursor += 1;
    return varInt64();
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

  function stringUtf8At(start: number, len: number) {
    return buffer.utf8Slice(start, start + len);
  }

  function stringUtf8(len: number) {
    const result = buffer.utf8Slice(cursor, cursor + len);
    cursor += len;
    return result;
  }

  function stringOfVarUInt32() {
    const isLatin1 = uint8() === LATIN1;
    const len = varUInt32();
    return isLatin1 ? stringLatin1(len) : stringUtf8(len);
  }

  function stringLatin1Fast(len: number) {
    const result = bigString.substring(cursor, cursor + len);
    cursor += len;
    return result;
  }

  function stringLatin1Slow(len: number) {
    const rawCursor = cursor;
    cursor += len;
    switch (len) {
      case 0:
        return "";
      case 1:
        return read1(buffer, rawCursor);
      case 2:
        return read2(buffer, rawCursor);
      case 3:
        return read3(buffer, rawCursor);
      case 4:
        return read4(buffer, rawCursor);
      case 5:
        return read5(buffer, rawCursor);
      case 6:
        return read6(buffer, rawCursor);
      case 7:
        return read7(buffer, rawCursor);
      case 8:
        return read8(buffer, rawCursor);
      case 9:
        return read9(buffer, rawCursor);
      case 10:
        return read10(buffer, rawCursor);
      case 11:
        return read11(buffer, rawCursor);
      case 12:
        return read12(buffer, rawCursor);
      case 13:
        return read13(buffer, rawCursor);
      case 14:
        return read14(buffer, rawCursor);
      case 15:
        return read15(buffer, rawCursor);
      default:
        return buffer.latin1Slice(rawCursor, cursor);
    }
  }

  function binary(len: number) {
    const result = alloc(len);
    buffer.copy(result, 0, cursor, cursor + len);
    cursor += len;
    return result;
  }

  function bufferRef(len: number) {
    const result = buffer.subarray(cursor, cursor + len);
    cursor += len;
    return result;
  }

  function zigZag(v: number) {
    return (v >> 1) ^ -(v & 1);
  }

  function zigZagBigInt(v: bigint) {
    return (v >> 1n) ^ -(v & 1n);
  }

  function varUInt32() {
    let byte_ = uint8();
    let result = byte_ & 0x7f;
    if ((byte_ & 0x80) != 0) {
      byte_ = uint8();
      result |= (byte_ & 0x7f) << 7;
      if ((byte_ & 0x80) != 0) {
        byte_ = uint8();
        result |= (byte_ & 0x7f) << 14;
        if ((byte_ & 0x80) != 0) {
          byte_ = uint8();
          result |= (byte_ & 0x7f) << 21;
          if ((byte_ & 0x80) != 0) {
            byte_ = uint8();
            result |= (byte_) << 28;
          }
        }
      }
    }
    return result;
  }

  function bigUInt8() {
    return BigInt(uint8() >>> 0);
  }

  function varUInt64() {
    let byte_ = bigUInt8();
    let result = byte_ & 0x7fn;
    if ((byte_ & 0x80n) != 0n) {
      byte_ = bigUInt8();
      result |= (byte_ & 0x7fn) << 7n;
      if ((byte_ & 0x80n) != 0n) {
        byte_ = bigUInt8();
        result |= (byte_ & 0x7fn) << 14n;
        if ((byte_ & 0x80n) != 0n) {
          byte_ = bigUInt8();
          result |= (byte_ & 0x7fn) << 21n;
          if ((byte_ & 0x80n) != 0n) {
            byte_ = bigUInt8();
            result |= (byte_ & 0x7fn) << 28n;
            if ((byte_ & 0x80n) != 0n) {
              byte_ = bigUInt8();
              result |= (byte_ & 0x7fn) << 35n;
              if ((byte_ & 0x80n) != 0n) {
                byte_ = bigUInt8();
                result |= (byte_ & 0x7fn) << 42n;
                if ((byte_ & 0x80n) != 0n) {
                  byte_ = bigUInt8();
                  result |= (byte_ & 0x7fn) << 49n;
                  if ((byte_ & 0x80n) != 0n) {
                    byte_ = bigUInt8();
                    result |= (byte_) << 56n;
                  }
                }
              }
            }
          }
        }
      }
    }
    return result;
  }

  function varInt32() {
    return zigZag(varUInt32());
  }

  function varInt64() {
    return zigZagBigInt(varUInt64());
  }

  return {
    getCursor: () => cursor,
    setCursor: (v: number) => (cursor = v),
    varInt32,
    varInt64,
    varUInt32,
    varUInt64,
    int8,
    buffer: binary,
    bufferRef,
    uint8,
    reset,
    stringUtf8At,
    stringUtf8,
    stringLatin1,
    stringOfVarUInt32,
    double,
    float,
    uint16,
    int16,
    uint64,
    skip,
    int64,
    sliLong,
    uint32,
    int32,
  };
};
