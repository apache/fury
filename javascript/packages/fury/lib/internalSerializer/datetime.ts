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

import  {Fury} from "../type";
import { InternalSerializerType } from "../type";

const epoch =  new Date('1970/01/01 00:00').getTime();

export const timestampSerializer = (fury: Fury) => {
    const { binaryReader, binaryWriter, referenceResolver} = fury;
    const { int64: writeInt64 } = binaryWriter;
    const { int64: readInt64} = binaryReader;

    return {
        ...referenceResolver.deref(() => {
            return new Date(Number(readInt64()));
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.TIMESTAMP, (v: Date) => {
            writeInt64(BigInt(v.getTime()));
        }),
        config: () => {
            return {
                reserve: 11
            }
        }
    }
}

export const dateSerializer = (fury: Fury) => {
    const { binaryReader, binaryWriter, referenceResolver} = fury;
    const { int32: writeInt32 } = binaryWriter;
    const { int32: readInt32} = binaryReader;
    return {
        ...referenceResolver.deref(() => {
            const day = readInt32();
            return new Date(epoch + (day * (24*60*60) * 1000));
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.DATE, (v: Date) => {
            const diff = v.getTime() - epoch;
            const day = Math.floor(diff / 1000 / (24*60*60))
            writeInt32(day);
        }),
        config: () => {
            return {
                reserve: 7,
            }
        }
    }
}
