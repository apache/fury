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

import { InternalSerializerType, RefFlags, Serializer } from "../type";
import { Fury } from "../type";
import stringSerializer from "../internalSerializer/string";
import boolSerializer from "../internalSerializer/bool";
import { int8Serializer, int32Serializer, int64Serializer, floatSerializer, doubleSerializer } from "../internalSerializer/number";


const buildTypedArray = <T>(fury: Fury, read: () => void,  write: (p: T) => void) => {
    const serializer = arraySerializer(fury);
    return {
        read: () => {
            const result =  serializer.read()
            for (let i = 0 ;i < result.length; i++) {
                result[i] = read();
            }
            return result;
        },
        write: (v: T[]) => {
            serializer.write(v);
            for (const item of v) {
                write(item);
            }
        },
        config: serializer.config,
    } as Serializer<T[]>
}

export const stringArraySerializer = (fury: Fury) => {
    const { readBySerializerWithOutTypeId, classResolver } = fury;
    const { read } = classResolver.getSerializerById(InternalSerializerType.STRING);
    const { writeWithOutType } = stringSerializer(fury);
    return buildTypedArray<string>(
        fury,
        () => readBySerializerWithOutTypeId(read),
        writeWithOutType,
    )
};

export const boolArraySerializer = (fury: Fury) => {
    const { read } = boolSerializer(fury);
    const { binaryWriter } = fury;
    const { writeUInt8 } = binaryWriter;

    return buildTypedArray<number>(
        fury,
        read,
        writeUInt8,
    )
};


export const shortArraySerializer = (fury: Fury) => {
    const { read } = int8Serializer(fury);
    const { binaryWriter } = fury;
    const { writeInt8 } = binaryWriter;
    return buildTypedArray<number>(
        fury,
        read,
        writeInt8,
    )
};

export const intArraySerializer = (fury: Fury) => {
    const { read } = int32Serializer(fury);
    const { binaryWriter } = fury;
    const { writeInt32 } = binaryWriter;

    return buildTypedArray<number>(
        fury,
        read,
        writeInt32
    )
};
export const longArraySerializer = (fury: Fury) => {
    const { read } = int64Serializer(fury);
    const { binaryWriter } = fury;
    const { writeInt64 } = binaryWriter;

    return buildTypedArray<number>(
        fury,
        read,
        writeInt64
    )
};
export const floatArraySerializer = (fury: Fury) => {
    const { read } = floatSerializer(fury);
    const { binaryWriter } = fury;
    const { writeFloat } = binaryWriter;

    return buildTypedArray<number>(
        fury,
        read,
        writeFloat
    )
};
export const doubleArraySerializer = (fury: Fury) => {
    const { read } = doubleSerializer(fury);
    const { binaryWriter } = fury;
    const { writeDouble } = binaryWriter;


    return buildTypedArray<number>(
        fury,
        read,
        writeDouble
    )
};


export const arraySerializer = (fury: Fury) => {
    const { binaryView, binaryWriter, referenceResolver, writeNullOrRef } = fury;
    const { pushReadObject, pushWriteObject } = referenceResolver;
    const { writeInt8, writeInt16, writeVarInt32 } = binaryWriter;
    const { readVarInt32 } = binaryView;
    return {
        read: () => {
            const len = readVarInt32();
            const result = new Array(len);
            pushReadObject(result);
            return result;
        },
        write: (v: any[]) => {
            if (writeNullOrRef(v)) {
                return;
            }
            writeInt8(RefFlags.RefValueFlag);
            writeInt16(InternalSerializerType.ARRAY);
            pushWriteObject(v);
            writeVarInt32(v.length);
        },
        config: () => {
            return {
                reserve: 7,
                refType: true,
            }
        }
    }
}
