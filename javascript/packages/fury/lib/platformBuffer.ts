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

import { hasBuffer } from "./util";

let utf8Encoder: TextEncoder | null;
let textDecoder: TextDecoder | null;

export type SupportedEncodings = "latin1" | "utf8";

export interface PlatformBuffer extends Uint8Array {
  toString(encoding?: SupportedEncodings, start?: number, end?: number): string;
  write(string: string, offset: number, encoding?: SupportedEncodings): void;
  copy(target: Uint8Array, targetStart?: number, sourceStart?: number, sourceEnd?: number): void;
}

export class BrowserBuffer extends Uint8Array implements PlatformBuffer {
  write(string: string, offset: number, encoding: SupportedEncodings = "utf8"): void {
    if (encoding === "latin1") {
      return this.latin1Write(string, offset);
    }
    return this.utf8Write(string, offset);
  }

  toString(encoding: SupportedEncodings = "utf8", start = 0, end = this.length): string {
    if (encoding === "latin1") {
      return this.latin1Slice(start, end);
    }
    return this.utf8Slice(start, end);
  }

  static alloc(size: number) {
    return new BrowserBuffer(new Uint8Array(size));
  }

  latin1Write(string: string, offset: number) {
    let index = 0;
    for (; index < string.length; index++) {
      this[offset++] = string.charCodeAt(index);
    }
  }

  utf8Write(string: string, offset: number) {
    let c1: number;
    let c2: number;
    for (let i = 0; i < string.length; ++i) {
      c1 = string.charCodeAt(i);
      if (c1 < 128) {
        this[offset++] = c1;
      } else if (c1 < 2048) {
        this[offset++] = (c1 >> 6) | 192;
        this[offset++] = (c1 & 63) | 128;
      } else if (
        (c1 & 0xfc00) === 0xd800
        && ((c2 = string.charCodeAt(i + 1)) & 0xfc00) === 0xdc00
      ) {
        c1 = 0x10000 + ((c1 & 0x03ff) << 10) + (c2 & 0x03ff);
        ++i;
        this[offset++] = (c1 >> 18) | 240;
        this[offset++] = ((c1 >> 12) & 63) | 128;
        this[offset++] = ((c1 >> 6) & 63) | 128;
        this[offset++] = (c1 & 63) | 128;
      } else {
        this[offset++] = (c1 >> 12) | 224;
        this[offset++] = ((c1 >> 6) & 63) | 128;
        this[offset++] = (c1 & 63) | 128;
      }
    }
  }

  latin1Slice(start: number, end: number) {
    if (end - start < 1) {
      return "";
    }
    let str = "";
    for (let i = start; i < end;) {
      str += String.fromCharCode(this[i++]);
    }
    return str;
  }

  utf8Slice(start: number, end: number) {
    if (end - start < 1) {
      return "";
    }

    if (!textDecoder) {
      textDecoder = new TextDecoder("utf-8");
    }

    return textDecoder.decode(this.subarray(start, end));
  }

  copy(target: Uint8Array, targetStart?: number, sourceStart?: number, sourceEnd?: number) {
    target.set(this.subarray(sourceStart, sourceEnd), targetStart);
  }

  static byteLength(str: string) {
    let len = 0;
    let c = 0;
    for (let i = 0; i < str.length; ++i) {
      c = str.charCodeAt(i);
      if (c < 128)
        len += 1;
      else if (c < 2048)
        len += 2;
      else if ((c & 0xFC00) === 0xD800 && (str.charCodeAt(i + 1) & 0xFC00) === 0xDC00) {
        ++i;
        len += 4;
      } else
        len += 3;
    }
    return len;
  }
}

export const fromUint8Array = hasBuffer
  ? (ab: Buffer | Uint8Array) => {
      if (!Buffer.isBuffer(ab)) {
        // https://nodejs.org/docs/latest/api/buffer.html#static-method-bufferfromarraybuffer-byteoffset-length
        // Create a zero-copy Buffer wrapper around the ArrayBuffer pointed to by the Uint8Array
        return (Buffer.from(ab.buffer, ab.byteOffset, ab.byteLength) as unknown as PlatformBuffer);
      } else {
        return ab as unknown as PlatformBuffer;
      }
    }
  : (ab: Buffer | Uint8Array) => new BrowserBuffer(ab);

export const alloc = (hasBuffer ? Buffer.allocUnsafe : BrowserBuffer.alloc) as unknown as (size: number) => PlatformBuffer;

export const strByteLength = hasBuffer ? Buffer.byteLength : BrowserBuffer.byteLength;

export const fromString
= hasBuffer
  ? (str: string) => Buffer.from(str) as unknown as PlatformBuffer
  : (str: string) => {
      if (!utf8Encoder) {
        utf8Encoder = new TextEncoder();
      }
      return new BrowserBuffer(utf8Encoder.encode(str));
    };
