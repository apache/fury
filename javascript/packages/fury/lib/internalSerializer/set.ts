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
    const { binaryView, binaryWriter, writeNullOrRef, referenceResolver, write, read } = fury;
    const { writeInt8, writeInt16, writeVarInt32 } = binaryWriter;
    const { readVarInt32 } = binaryView;

    const { pushReadObject, pushWriteObject } = referenceResolver;
    return {
        read: () => {
            const len = readVarInt32();
            const result = new Set();
            pushReadObject(result);
            for (let index = 0; index < len; index++) {
                result.add(read());
            }
            return result;
        },
        write: (v: Set<any>) => {
            if (writeNullOrRef(v)) {
                return;
            }
            writeInt8(RefFlags.RefValueFlag);
            writeInt16(InternalSerializerType.FURY_SET);
            pushWriteObject(v);
            const len = v.size;
            writeVarInt32(len);
            for (const value of v.values()) {
                write(value);
            }
        },
        config: () => {
            return {
                reserve: 7,
                refType: true,
            }
        }
    }
}
