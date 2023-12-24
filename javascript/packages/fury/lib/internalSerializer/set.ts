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

import { Fury, Serializer } from "../type";
import { InternalSerializerType } from "../type";

export default (fury: Fury, nestedSerializer: Serializer) => {
  const { binaryReader, binaryWriter, referenceResolver } = fury;
  const { varInt32: writeVarInt32, reserve: reserves } = binaryWriter;
  const { varInt32: readVarInt32 } = binaryReader;
  const { pushReadObject } = referenceResolver;
  const innerHeadSize = nestedSerializer.config().reserve;
  return {
    ...referenceResolver.deref(() => {
      const len = readVarInt32();
      const result = new Set();
      pushReadObject(result);
      for (let index = 0; index < len; index++) {
        result.add(nestedSerializer.read());
      }
      return result;
    }),
    write: referenceResolver.withNullableOrRefWriter(InternalSerializerType.FURY_SET, (v: Set<any>) => {
      const len = v.size;
      writeVarInt32(len);
      reserves(innerHeadSize * v.size);
      for (const value of v.values()) {
        nestedSerializer.write(value);
      }
    }),
    config: () => {
      return {
        reserve: 7,
      };
    },
  };
};
