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

import {
    genSerializer,
} from "./lib/codeGen";
import {
    Cast,
    ObjectTypeDescription,
    TypeDescription,
    ArrayTypeDescription,
    Type,
    ToRecordType,
} from "./lib/description";
import { Serializer, Fury, InternalSerializerType, Config } from "./lib/type";
import FuryInternal from "./lib/fury";

export {
    Serializer,
    InternalSerializerType,
    TypeDescription,
    ArrayTypeDescription,
    ObjectTypeDescription,
    Type,
};


export default class {
    constructor(private config?: Config) { }
    private fury: Fury = FuryInternal(this.config || {});

    registerSerializer<T extends TypeDescription>(description: T) {
        if (
            description.type !== InternalSerializerType.FURY_TYPE_TAG ||
            !Cast<ObjectTypeDescription>(description)?.options.tag
        ) {
            throw new Error("root type should be object");
        }
        const serializer = genSerializer(this.fury, description);
        return {
            serializer,
            serialize: (data: ToRecordType<T>) => {
                return this.fury.serialize(data, serializer);
            },
            deserialize: (bytes: Uint8Array) => {
                return this.fury.deserialize(bytes, serializer) as ToRecordType<T>;
            },
        };
    }

    serialize(v: any, serialize?: Serializer) {
        return this.fury.serialize(v, serialize);
    }

    deserialize(bytes: Uint8Array) {
        return this.fury.deserialize(bytes);
    }
}
