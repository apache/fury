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

import { InternalSerializerType, Serializer } from "../type";
import { Fury } from "../type";

export const tupleSerializer = (fury: Fury, serializers: Serializer[]) => {
  const { binaryReader, binaryWriter, referenceResolver } = fury;

  const { pushReadObject } = referenceResolver;
  const { varInt32: writeVarInt32, reserve: reserves } = binaryWriter;
  const { varInt32: readVarInt32 } = binaryReader;

  return {
    ...referenceResolver.deref(() => {
      const len = readVarInt32();
      const result = new Array(len);
      pushReadObject(result);
      for (let i = 0; i < len; i++) {
        const item = serializers[i];
        result[i] = item.read();
      }
      return result;
    }),
    write: referenceResolver.withNullableOrRefWriter(InternalSerializerType.TUPLE, (v: any[]) => {
      writeVarInt32(serializers.length);

      for (let i = 0; i < serializers.length; i++) {
        const item = serializers[i];
        reserves(item.config().reserve);
        item.write(v[i]);
      }
    }),
    config: () => {
      return {
        reserve: 7,
      };
    },
  };
};
