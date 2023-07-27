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
    const { binaryView, binaryWriter, writeNull } = fury;
    const { writeInt8, writeInt16, writeStringOfVarInt32 } = binaryWriter
    const { readVarInt32, readStringUtf8 } = binaryView;

    return {
        read: () => {
            // todo support latin1
            //const type = readUInt8();
            const len = readVarInt32();
            const result = readStringUtf8(len);
            return result;
        },
        write: (v: string) => {
            if (writeNull(v)) {
                return;
            }
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.STRING);
            writeStringOfVarInt32(v);
        },
        writeWithOutType: (v: string) => {
            if (writeNull(v)) {
                return;
            }
            writeInt8(RefFlags.NotNullValueFlag);
            writeStringOfVarInt32(v);
        },
        config: () => {
            return {
                reserve: 7,
            }
        }
    }
}
