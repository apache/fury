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

import { Config, LATIN1, UTF8 } from "./type";

const MAX_POOL_SIZE = 1024 * 1024 * 3; // 3MB

export const BinaryWriter = (config: Config) => {
  let cursor = 0;
  let byteLength: number;
  let arrayBuffer: Buffer;
  let dataView: DataView;
  let reserved = 0;

  function initPoll() {
    byteLength = 1024 * 100;
    arrayBuffer = Buffer.allocUnsafe(byteLength);
    dataView = new DataView(arrayBuffer.buffer);
  }

  initPoll();

  function reserve(len: number) {
    reserved += len;
    if (byteLength - cursor <= reserved) {
      arrayBuffer = Buffer.concat([arrayBuffer, Buffer.allocUnsafe(byteLength + len)]);
      byteLength = arrayBuffer.byteLength;
      dataView = new DataView(arrayBuffer.buffer);
    }
  }

  function reset() {
    cursor = 0;
    reserved = 0;
  }

  function uint8(v: number) {
    dataView.setUint8(cursor, v);
    cursor++;
  }

  function int8(v: number) {
    dataView.setInt8(cursor, v);
    cursor++;
  }

  function int24(v: number) {
    dataView.setUint32(cursor, v, true);
    cursor += 3;
  }

  function uint16(v: number) {
    dataView.setUint16(cursor, v, true);
    cursor += 2;
  }

  function int16(v: number) {
    dataView.setInt16(cursor, v, true);
    cursor += 2;
  }

  function skip(len: number) {
    cursor += len;
  }

  function int32(v: number) {
    dataView.setInt32(cursor, v, true);
    cursor += 4;
  }

  function uint32(v: number) {
    dataView.setUint32(cursor, v, true);
    cursor += 4;
  }

  function int64(v: bigint | number) {
    if (typeof v === "number") {
      dataView.setBigInt64(cursor, BigInt(v), true);
    } else {
      dataView.setBigInt64(cursor, v, true);
    }
    cursor += 8;
  }

  function float(v: number) {
    dataView.setFloat32(cursor, v, true);
    cursor += 4;

  }

  function double(v: number) {
    dataView.setFloat64(cursor, v, true);
    cursor += 8;
  }

  function buffer(v: Uint8Array) {
    reserve(v.byteLength);
    arrayBuffer.set(v, cursor);
    cursor += v.byteLength;
  }

  function uint64(v: bigint | number) {
    if (typeof v === "number") {
      dataView.setBigUint64(cursor, BigInt(v), true);
    } else {
      dataView.setBigUint64(cursor, v, true);
    }
    cursor += 8;
  }

  function utf8StringOfInt16(bf: Buffer, bufferLen: number) {
    int16(bufferLen);
    bf.copy(arrayBuffer, cursor);
    cursor += bufferLen;
  }

  function fastWriteStringUtf8(string: string, buffer: Buffer, offset: number) {
    let c1: number;
    let c2: number;
    for (let i = 0; i < string.length; ++i) {
      c1 = string.charCodeAt(i);
      if (c1 < 128) {
        buffer[offset++] = c1;
      } else if (c1 < 2048) {
        const u1 = (c1 >> 6) | 192;
        const u2 = (c1 & 63) | 128;
        dataView.setUint16(offset, (u1 << 8) | u2);
        offset += 2;
      } else if (
        (c1 & 0xfc00) === 0xd800 &&
        ((c2 = string.charCodeAt(i + 1)) & 0xfc00) === 0xdc00
      ) {
        c1 = 0x10000 + ((c1 & 0x03ff) << 10) + (c2 & 0x03ff);
        ++i;
        const u1 = (c1 >> 18) | 240;
        const u2 = ((c1 >> 12) & 63) | 128;
        const u3 = ((c1 >> 6) & 63) | 128;
        const u4 = (c1 & 63) | 128;
        dataView.setUint32(offset, (u1 << 24) | (u2 << 16) | (u3 << 8) | u4);
        offset += 4;
      } else {
        const u1 = (c1 >> 12) | 224;
        const u2 = ((c1 >> 6) & 63) | 128;
        dataView.setUint16(offset, (u1 << 8) | u2);
        offset += 2;
        buffer[offset++] = (c1 & 63) | 128;
      }
    }
  }

  function stringOfVarInt32Fast() {
    const { isLatin1: detectIsLatin1, stringCopy } = config!.hps!;
    return function (v: string) {
      const isLatin1 = detectIsLatin1(v);
      const len = isLatin1 ? v.length : Buffer.byteLength(v);
      if (config.useLatin1) {
        dataView.setUint8(cursor++, isLatin1 ? LATIN1 : UTF8);
      }
      varInt32(len);
      reserve(len);
      if (isLatin1) {
        stringCopy(v, arrayBuffer, cursor);
      } else {
        if (len < 40) {
          fastWriteStringUtf8(v, arrayBuffer, cursor);
        } else {
          (arrayBuffer as any).utf8Write(v, cursor);
        }
      }
      cursor += len;
    };
  }

  function stringOfVarInt32Slow(v: string) {
    const len = Buffer.byteLength(v);
    const isLatin1 = len === v.length;
    if (config.useLatin1) {
      dataView.setUint8(cursor++, isLatin1 ? LATIN1 : UTF8);
    }
    varInt32(len);
    reserve(len);
    if (isLatin1) {
      if (len < 40) {
        for (let index = 0; index < v.length; index++) {
          arrayBuffer[cursor + index] = v.charCodeAt(index);
        }
      } else {
        (arrayBuffer as any).latin1Write(v, cursor);
      }
    } else {
      if (len < 40) {
        fastWriteStringUtf8(v, arrayBuffer, cursor);
      } else {
        (arrayBuffer as any).utf8Write(v, cursor);
      }
    }
    cursor += len;
  }

  function varInt32(value: number) {
    if (value >> 7 == 0) {
      arrayBuffer[cursor++] = value;
      return;
    }
    if (value >> 14 == 0) {
      const u1 = (value & 0x7f) | 0x80;
      const u2 = value >> 7;
      dataView.setUint16(cursor, (u1 << 8) | u2);
      cursor += 2;
      return;
    }
    if (value >> 21 == 0) {
      const u1 = (value & 0x7f) | 0x80;
      const u2 = (value >> 7) | 0x80;
      dataView.setUint16(cursor, (u1 << 8) | u2);
      arrayBuffer[cursor + 2] = value >> 14;
      cursor += 3;
      return;
    }
    if (value >> 28 == 0) {
      const u1 = (value & 0x7f) | 0x80;
      const u2 = (value >> 7) | 0x80;
      const u3 = (value >> 14) | 0x80;
      const u4 = (value >> 21) | 0x80;
      dataView.setUint32(cursor, (u1 << 24) | (u2 << 16) | (u3 << 8) | u4);
      cursor += 4;
      return;
    }
    const u1 = (value & 0x7f) | 0x80;
    const u2 = (value >> 7) | 0x80;
    const u3 = (value >> 14) | 0x80;
    const u4 = (value >> 21) | 0x80;
    dataView.setUint32(cursor, (u1 << 24) | (u2 << 16) | (u3 << 8) | u4);
    arrayBuffer[cursor + 4] = value >> 28;
    cursor += 5;
    return;
  }

  function tryFreePool() {
    if (byteLength > MAX_POOL_SIZE) {
      initPoll();
    }
  }

  function dump() {
    const result = Buffer.allocUnsafe(cursor);
    arrayBuffer.copy(result, 0, 0, cursor);
    tryFreePool();
    return result;
  }

  function getCursor() {
    return cursor;
  }

  function setUint32Position(offset: number, v: number) {
    dataView.setUint32(offset, v, true);
  }

  return {
    skip,
    reset,
    reserve,
    uint16,
    int8,
    int24,
    dump,
    uint8,
    int16,
    varInt32,
    stringOfVarInt32: config?.hps
      ? stringOfVarInt32Fast()
      : stringOfVarInt32Slow,
    utf8StringOfInt16: utf8StringOfInt16,
    uint64,
    buffer,
    double,
    float,
    int64,
    uint32,
    int32,
    getCursor,
    setUint32Position,
  };
};
