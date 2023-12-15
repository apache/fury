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

import { Fury } from "../type";
import { InternalSerializerType } from "../type";

export default (fury: Fury) => {
  const { binaryReader, binaryWriter, referenceResolver } = fury;

  const { uint8: writeUInt8, int32: writeInt32, buffer: writeBuffer } = binaryWriter;
  const { uint8: readUInt8, int32: readInt32, buffer: readBuffer } = binaryReader;
  const { pushReadObject } = referenceResolver;

  return {
    ...referenceResolver.deref(() => {
      readUInt8(); // isInBand
      const len = readInt32();
      const result = readBuffer(len);
      pushReadObject(result);
      return result;
    }),
    write: referenceResolver.withNullableOrRefWriter(InternalSerializerType.BINARY, (v: Uint8Array) => {
      writeUInt8(1); // is inBand
      writeInt32(v.byteLength);
      writeBuffer(v);
    }),
    config: () => {
      return {
        reserve: 8,
      };
    },
  };
};
