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

import { Fury } from "../type";
import { InternalSerializerType, RefFlags } from "../type";

export default (fury: Fury) => {
  const { binaryReader, binaryWriter, referenceResolver } = fury;
  const { stringOfVarUInt32: writeStringOfVarUInt32, int8 } = binaryWriter;
  const { stringOfVarUInt32: readStringOfVarUInt32 } = binaryReader;

  return {
    ...referenceResolver.deref(() => {
      return readStringOfVarUInt32();
    }),
    write: referenceResolver.withNotNullableWriter(InternalSerializerType.STRING, "", (v: string) => {
      writeStringOfVarUInt32(v);
    }),
    writeWithoutType: (v: string) => {
      if (v === null) {
        binaryWriter.int8(RefFlags.NullFlag);
        return;
      }
      int8(RefFlags.NotNullValueFlag);
      writeStringOfVarUInt32(v);
    },
    config: () => {
      return {
        reserve: 8,
      };
    },
  };
};
