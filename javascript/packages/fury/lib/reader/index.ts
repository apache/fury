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

import { Config, LATIN1 } from "../type";
import { isNodeEnv } from "../util";
import { PlatformBuffer, alloc, fromUint8Array } from "../platformBuffer";
import { readLatin1String } from "./string";

export const BinaryReader = (config: Config) => {
  const sliceStringEnable = isNodeEnv && config.useSliceString;
  let cursor = 0;
  let dataView!: DataView;
  let buffer!: PlatformBuffer;
  let bigString: string;
  let byteLength: number;

  const stringLatin1 = sliceStringEnable ? stringLatin1Fast : stringLatin1Slow;

  function reset(ab: Uint8Array) {
    buffer = fromUint8Array(ab);
    byteLength = buffer.byteLength;
    dataView = new DataView(buffer.buffer, buffer.byteOffset, byteLength);
    if (sliceStringEnable) {
      bigString = buffer.toString("latin1", 0, byteLength);
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
    return buffer.toString("utf8", start, start + len);
  }

  function stringUtf8(len: number) {
    const result = buffer.toString("utf8", cursor, cursor + len);
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
    return readLatin1String(buffer, len, rawCursor);
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

  function varUInt32() {
    // Reduce memory reads as much as possible. Reading a uint32 at once is far faster than reading four uint8s separately.
    if (byteLength - cursor >= 5) {
      const u32 = dataView.getUint32(cursor++, true);
      let result = u32 & 0x7f;
      if ((u32 & 0x80) != 0) {
        cursor++;
        const b2 = u32 >> 8;
        result |= (b2 & 0x7f) << 7;
        if ((b2 & 0x80) != 0) {
          cursor++;
          const b3 = u32 >> 16;
          result |= (b3 & 0x7f) << 14;
          if ((b3 & 0x80) != 0) {
            cursor++;
            const b4 = u32 >> 24;
            result |= (b4 & 0x7f) << 21;
            if ((b4 & 0x80) != 0) {
              result |= (uint8()) << 28;
            }
          }
        }
      }
      return result;
    }
    let byte = uint8();
    let result = byte & 0x7f;
    if ((byte & 0x80) != 0) {
      byte = uint8();
      result |= (byte & 0x7f) << 7;
      if ((byte & 0x80) != 0) {
        byte = uint8();
        result |= (byte & 0x7f) << 14;
        if ((byte & 0x80) != 0) {
          byte = uint8();
          result |= (byte & 0x7f) << 21;
          if ((byte & 0x80) != 0) {
            byte = uint8();
            result |= (byte) << 28;
          }
        }
      }
    }
    return result;
  }

  function varInt32() {
    const v = varUInt32();
    return (v >> 1) ^ -(v & 1); // zigZag decode
  }

  function bigUInt8() {
    return BigInt(uint8() >>> 0);
  }

  function varUInt64() {
    // Creating BigInts is too performance-intensive; we'll use uint32 instead.
    if (byteLength - cursor < 8) {
      let byte = bigUInt8();
      let result = byte & 0x7fn;
      if ((byte & 0x80n) != 0n) {
        byte = bigUInt8();
        result |= (byte & 0x7fn) << 7n;
        if ((byte & 0x80n) != 0n) {
          byte = bigUInt8();
          result |= (byte & 0x7fn) << 14n;
          if ((byte & 0x80n) != 0n) {
            byte = bigUInt8();
            result |= (byte & 0x7fn) << 21n;
            if ((byte & 0x80n) != 0n) {
              byte = bigUInt8();
              result |= (byte & 0x7fn) << 28n;
              if ((byte & 0x80n) != 0n) {
                byte = bigUInt8();
                result |= (byte & 0x7fn) << 35n;
                if ((byte & 0x80n) != 0n) {
                  byte = bigUInt8();
                  result |= (byte & 0x7fn) << 42n;
                  if ((byte & 0x80n) != 0n) {
                    byte = bigUInt8();
                    result |= (byte & 0x7fn) << 49n;
                    if ((byte & 0x80n) != 0n) {
                      byte = bigUInt8();
                      result |= (byte) << 56n;
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
    const l32 = dataView.getUint32(cursor++, true);
    let byte = l32 & 0xff;
    let rl28 = byte & 0x7f;
    let rh28 = 0;
    if ((byte & 0x80) != 0) {
      byte = l32 & 0xff00 >> 8;
      cursor++;
      rl28 |= (byte & 0x7f) << 7;
      if ((byte & 0x80) != 0) {
        byte = l32 & 0xff0000 >> 16;
        cursor++;
        rl28 |= (byte & 0x7f) << 14;
        if ((byte & 0x80) != 0) {
          byte = l32 & 0xff000000 >> 24;
          cursor++;
          rl28 |= (byte & 0x7f) << 21;
          if ((byte & 0x80) != 0) {
            const h32 = dataView.getUint32(cursor++, true);
            byte = h32 & 0xff;
            rh28 |= (byte & 0x7f);
            if ((byte & 0x80) != 0) {
              byte = h32 & 0xff00 >> 8;
              cursor++;
              rh28 |= (byte & 0x7f) << 7;
              if ((byte & 0x80) != 0) {
                byte = h32 & 0xff0000 >> 16;
                cursor++;
                rh28 |= (byte & 0x7f) << 14;
                if ((byte & 0x80) != 0) {
                  byte = h32 & 0xff000000 >> 24;
                  cursor++;
                  rh28 |= (byte & 0x7f) << 21;
                  if ((byte & 0x80) != 0) {
                    return (BigInt(uint8()) << 56n) | BigInt(rh28) << 28n | BigInt(rl28);
                  }
                }
              }
            }
          }
        }
      }
    }

    return BigInt(rh28) << 28n | BigInt(rl28);
  }

  function varInt64() {
    const v = varUInt64();
    return (v >> 1n) ^ -(v & 1n); // zigZag decode
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
