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
    const { binaryView, binaryWriter, referenceResolver, writeNullOrRef } = fury;
    const { writeInt8, writeInt16, writeUInt8, writeInt32, writeBuffer } = binaryWriter;
    const { readUInt8, readInt32, readBuffer } = binaryView;
    const { pushReadObject, pushWriteObject } = referenceResolver;
    
    return {
        read: () => {
            readUInt8(); // isInBand
            const len = readInt32();
            const result = readBuffer(len);
            pushReadObject(result);
            return result
        },
        write: (v: Uint8Array) => {
            if (writeNullOrRef(v)) {
                return;
            }
            writeInt8(RefFlags.RefValueFlag);
            writeInt16(InternalSerializerType.BINARY);
            pushWriteObject(v);
            writeUInt8(1); // is inBand
            writeInt32(v.byteLength);
            writeBuffer(v);
        },
        config: () => {
            return {
                reserve: 8,
                refType: true,
            }
        }
    }
}