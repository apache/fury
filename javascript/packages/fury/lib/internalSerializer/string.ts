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
import { InternalSerializerType, RefFlags } from "../type";



export default (fury: Fury) => {
    const { binaryReader, binaryWriter, referenceResolver } = fury;
    const { stringOfVarInt32: writeStringOfVarInt32, int8 } = binaryWriter
    const { stringOfVarInt32: readStringOfVarInt32 } = binaryReader;

    return {
        ...referenceResolver.deref(() => {
            return readStringOfVarInt32();
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.STRING, "", (v: string) => {
            writeStringOfVarInt32(v);
        }),
        writeWithoutType: (v: string) => {
            if (v === null) {
                binaryWriter.int8(RefFlags.NullFlag);
                return;
            }
            int8(RefFlags.NotNullValueFlag);
            writeStringOfVarInt32(v);
        },
        config: () => {
            return {
                reserve: 8,
            }
        }
    }
}
