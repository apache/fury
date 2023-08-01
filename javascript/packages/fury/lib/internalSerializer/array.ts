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


export const buildArray = (fury: Fury, item: Serializer, type: InternalSerializerType) => {
    const { binaryReader, binaryWriter, referenceResolver } = fury;

    const { pushReadObject } = referenceResolver;
    const { varInt32: writeVarInt32, reserve: reserves } = binaryWriter;
    const { varInt32: readVarInt32 } = binaryReader;
    const { write, read } = item;
    const innerHeadSize = (item.config().reserve);
    return {
        ...referenceResolver.deref(() => {
            const len = readVarInt32();
            const result = new Array(len);
            pushReadObject(result);
            for (let i = 0; i < result.length; i++) {
                result[i] = read();
            }
            return result;
        }),
        write: referenceResolver.withNullableOrRefWriter(type, (v: any[]) => {
            writeVarInt32(v.length);

            reserves(innerHeadSize * v.length);
            
            for (const x of v) {
                write(x);
            }
        }),
        config: () => {
            return {
                reserve: 7,
            }
        }
    }
}

const buildTypedArray = (fury: Fury, serializeType: InternalSerializerType, typeArrayType: InternalSerializerType) => {
    const serializer = fury.classResolver.getSerializerById(serializeType);
    if (!serializer.readWithoutType || !serializer.writeWithoutType) {
        throw new Error(`${serializeType} should implements readWithoutType and writeWithoutType`)
    }
    return buildArray(fury, {
        read: serializer.readWithoutType!,
        write: serializer.writeWithoutType!,
        config: serializer.config,
    }, typeArrayType)
}

export const stringArraySerializer = (fury: Fury) => buildTypedArray(fury, InternalSerializerType.STRING, InternalSerializerType.FURY_STRING_ARRAY);
export const boolArraySerializer = (fury: Fury) => buildTypedArray(fury, InternalSerializerType.BOOL, InternalSerializerType.FURY_PRIMITIVE_BOOL_ARRAY);
export const longArraySerializer = (fury: Fury) => buildTypedArray(fury, InternalSerializerType.INT64, InternalSerializerType.FURY_PRIMITIVE_LONG_ARRAY);
export const intArraySerializer = (fury: Fury) => buildTypedArray(fury, InternalSerializerType.INT32, InternalSerializerType.FURY_PRIMITIVE_INT_ARRAY);
export const floatArraySerializer = (fury: Fury) => buildTypedArray(fury, InternalSerializerType.FLOAT, InternalSerializerType.FURY_PRIMITIVE_FLOAT_ARRAY);
export const doubleArraySerializer = (fury: Fury) => buildTypedArray(fury, InternalSerializerType.DOUBLE, InternalSerializerType.FURY_PRIMITIVE_DOUBLE_ARRAY);
export const shortArraySerializer = (fury: Fury) => buildTypedArray(fury, InternalSerializerType.INT16, InternalSerializerType.FURY_PRIMITIVE_SHORT_ARRAY);



export const arraySerializer = (fury: Fury, item: Serializer) => buildArray(fury, item, InternalSerializerType.ARRAY);
