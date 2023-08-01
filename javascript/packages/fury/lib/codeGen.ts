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

import { InternalSerializerType, MaxInt32, RefFlags, Fury } from './type';
import { replaceBackslashAndQuote, safePropAccessor, safePropName } from './util';
import mapSerializer from './internalSerializer/map';
import setSerializer from './internalSerializer/set';
import { arraySerializer } from './internalSerializer/array';

export interface TypeDescription {
    type: InternalSerializerType,
    label?: string,
}

export interface ObjectTypeDescription extends TypeDescription {
    options: {
        props: { [key: string]: TypeDescription },
        tag: string,
    }
}

export interface ArrayTypeDescription extends TypeDescription {
    options: {
        inner: TypeDescription;
    }
}

export interface SetTypeDescription extends TypeDescription {
    options: {
        key: TypeDescription;
    }
}

export interface MapTypeDescription extends TypeDescription {
    options: {
        key: TypeDescription;
        value: TypeDescription
    }
}

export function Cast<T1 extends TypeDescription>(p: TypeDescription) {
    return p as unknown as T1;
}



const BASIC_TYPES = [
    InternalSerializerType.BOOL,
    InternalSerializerType.INT8,
    InternalSerializerType.INT16,
    InternalSerializerType.INT32,
    InternalSerializerType.INT64,
    InternalSerializerType.FLOAT,
    InternalSerializerType.DOUBLE,
    InternalSerializerType.STRING,
    InternalSerializerType.BINARY,
    InternalSerializerType.DATE,
    InternalSerializerType.TIMESTAMP,
];

function computeFieldHash(hash: number, id: number): number {
    let newHash = (hash) * 31 + (id);
    while (newHash >= MaxInt32) {
        newHash = Math.floor(newHash / 7);
    }
    return newHash
}

export const computeStructHash = (description: TypeDescription) => {
    if (description.type !== InternalSerializerType.FURY_TYPE_TAG) {
        throw new Error('only object is hashable');
    }
    let hash = 17;
    for (const [, value] of Object.entries(Cast<ObjectTypeDescription>(description).options.props).sort()) {
        if (value.type === InternalSerializerType.ARRAY || value.type === InternalSerializerType.MAP) {
            hash = computeFieldHash(hash, value.type);
        }
        if (BASIC_TYPES.includes(value.type)) {
            hash = computeFieldHash(hash, value.type);
        }
    }
    return hash;
}

function typeHandlerDeclaration(fury: Fury) {
    let declarations: string[] = [];
    let count = 0;
    const exists = new Map<string, string>();
    function addDeclar(name: string, declar: string, uniqueKey?: string) {
        const unique = uniqueKey || name;
        if (exists.has(unique)) {
            return exists.get(unique)!;
        }
        declarations.push(declar);
        exists.set(unique, name);
        return name;
    }

    const genBuiltinDeclaration = (type: number) => {
        const name = `type_${type}`.replace('-', '_');
        return addDeclar(name, `
        const ${name} = classResolver.getSerializerById(${type})`);
    }

    const genTagDeclaration = (tag: string) => {
        const name = `tag_${count++}`;
        return addDeclar(name, `
        const ${name} = classResolver.getSerializerByTag("${replaceBackslashAndQuote(tag)}")`, tag);
    }

    const genDeclaration = (description: TypeDescription): string => {
        if (description.type === InternalSerializerType.FURY_TYPE_TAG) {
            genSerializer(fury, description);
            return genTagDeclaration(Cast<ObjectTypeDescription>(description).options.tag);
        }
        if (description.type === InternalSerializerType.ARRAY) {
            const inner = genDeclaration(Cast<ArrayTypeDescription>(description).options.inner);
            const name = `array_${inner}`;
            return addDeclar(name, `
                const ${name} = arraySerializer(fury, ${inner})`
            )
        }
        if (description.type === InternalSerializerType.FURY_SET) {
            const inner = genDeclaration(Cast<SetTypeDescription>(description).options.key);
            const name = `set_${inner}`;
            return addDeclar(name, `
                const ${name} = setSerializer(fury, ${inner})`
            )
        }
        if (description.type === InternalSerializerType.MAP) {
            const key = genDeclaration(Cast<MapTypeDescription>(description).options.key);
            const value = genDeclaration(Cast<MapTypeDescription>(description).options.value);

            const name = `map_${key}_${value}`;
            return addDeclar(name, `
                const ${name} = mapSerializer(fury, ${key}, ${value})`
            )
        }
        return genBuiltinDeclaration(description.type);
    }
    return {
        genDeclaration,
        finish() {
            const result = {
                declarations,
                names: [...exists.values()]
            };
            declarations = [];
            exists.clear();
            return result;
        }
    }
}

export const genSerializer = (fury: Fury, description: TypeDescription) => {
    const { genDeclaration, finish } = typeHandlerDeclaration(fury);
    const tag = Cast<ObjectTypeDescription>(description).options?.tag;
    if (fury.classResolver.getSerializerByTag(tag)) {
        return;
    }
    
    fury.classResolver.registerSerializerByTag(tag, fury.classResolver.getSerializerById(InternalSerializerType.ANY));
    const tagByteLen = Buffer.from(tag).byteLength;
    const expectHash = computeStructHash(description);
    const read = `
    // relation tag: ${Cast<ObjectTypeDescription>(description).options?.tag}
    const result = {
        ${Object.entries(Cast<ObjectTypeDescription>(description).options.props).sort().map(([key]) => {
        return `${safePropName(key)}: null`
    }).join(',\n')}
    };
    pushReadObject(result);
    ${Object.entries(Cast<ObjectTypeDescription>(description).options.props).sort().map(([key, value]) => {
        return `result${safePropAccessor(key)} = ${genDeclaration(value)}.read()`;
    }).join(';\n')
        }
    return result;
`;
    const write = Object.entries(Cast<ObjectTypeDescription>(description).options.props).sort().map(([key, value]) => {
        return `${genDeclaration(value)}.write(v${safePropAccessor(key)})`;
    }).join(';\n');
    const { names, declarations} = finish();
    return fury.classResolver.registerSerializerByTag(tag, new Function(
        `
return function (fury, scope) {
    const { referenceResolver, binaryWriter, classResolver, binaryReader } = fury;
    const { writeNullOrRef, pushReadObject } = referenceResolver;
    const { RefFlags, InternalSerializerType, arraySerializer, mapSerializer, setSerializer } = scope;
        ${declarations.join('')}
    const tagBuffer = Buffer.from("${replaceBackslashAndQuote(tag)}");
    const bufferLen = ${tagByteLen};
    const reserves = ${names.map(x => `${x}.config().reserve`).join(' + ')};
    return {
        ...referenceResolver.deref(() => {
            const hash = binaryReader.int32();
            if (hash !== ${expectHash}) {
                throw new Error("validate hash failed: ${replaceBackslashAndQuote(tag)}. expect ${expectHash}, but got" + hash);
            }
            {
                ${read}
            }
        }),
        write: referenceResolver.withNullableOrRefWriter(InternalSerializerType.FURY_TYPE_TAG, (v) => {
            classResolver.writeTag(binaryWriter, "${replaceBackslashAndQuote(tag)}", tagBuffer, bufferLen);
            binaryWriter.int32(${computeStructHash(description)});
            binaryWriter.reserve(reserves);
            ${write}
        }),
        config() {
            return {
                reserve: ${tagByteLen + 8},
            }
        }
    }
}
`
    )()(fury, {
        InternalSerializerType,
        RefFlags,
        arraySerializer,
        mapSerializer,
        setSerializer,
    }));
}