import { InternalSerializerType, MaxInt32, RefFlags, Fury } from './type';
import { replaceBackslashAndQuote, safePropAccessor, safePropName, utf8Encoder } from './util';


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

export function Cast<T1 extends TypeDescription>(p: TypeDescription) {
    return p as unknown as T1;
}


export const computeStringHash = (str: string) => {
    const bytes = utf8Encoder.encode(str);
    let hash = 17
    bytes.forEach(b => {
        hash = hash * 31 + b
        do {
            hash = Math.round(hash / 7)
        } while (hash >= MaxInt32);
    });
    return hash;
}

export const computeFieldHash = (hash: number, t: TypeDescription) => {
    let id = 0;
    if (t.type === InternalSerializerType.FURY_TYPE_TAG) {
        id = computeStringHash(Cast<ObjectTypeDescription>(t).options!.tag!);
    } else {
        id = t.type;
    }
    let newHash = hash * 31 + id;
    do {
        newHash = Math.round(newHash / 7)
    } while (newHash >= MaxInt32);
    return newHash;
}

export const computeTagHash = (description: TypeDescription) => {
    if (description.type !== InternalSerializerType.FURY_TYPE_TAG) {
        throw new Error('only object is hashable');
    }
    let hash = 17;
    for (const [, value] of Object.entries(Cast<ObjectTypeDescription>(description).options!.props!)) {
        hash = computeFieldHash(hash, value)
    }
    return hash;
}

function typeHandlerDeclaration(readOrWrite: 'read' | 'write', fury: Fury) {
    let declarations: string[] = [];
    let count = 0;
    const exists = new Set();
    function addDeclar(name: string, declar: string) {
        if (exists.has(name)) {
            return name;
        }
        declarations.push(declar);
        exists.add(name);
        return name;
    }

    const genBuiltinDeclaration = (type: number) => {
        const name = `type_${type}_${readOrWrite}`;
        if (readOrWrite === 'read') {
            return addDeclar(name, `
                const ${name} = buildRefTypeReader(classResolver.getSerializerById(${type}).read)`);
        }
        return addDeclar(name, `
        const ${name} = classResolver.getSerializerById(${type}).${readOrWrite};`)
    }
    const genTagDeclaration = (tag: string) => {
        const name = `tag_${count++}_${readOrWrite}`;
        if (readOrWrite === 'read') {
            return addDeclar(name, `
            const ${name} = (serializer => buildRefTypeReader(() => {
                return serializer.read();
            }))(classResolver.getSerializerByTag("${replaceBackslashAndQuote(tag)}"))`)
        } else {
            return addDeclar(name, `
            const ${name} = classResolver.getSerializerByTag("${replaceBackslashAndQuote(tag)}");`
            )
        }

    }
    return {
        genBuiltinDeclaration,
        genTagDeclaration,
        finish() {
            const result = declarations;
            declarations = [];
            exists.clear();
            return result;
        }
    }
}

export const genReadSerializer = (fury: Fury, description: TypeDescription, stack: string[] = []) => {
    const { genBuiltinDeclaration, genTagDeclaration, finish } = typeHandlerDeclaration('read', fury);
    const descriptionReadExpression = (key: string, description: TypeDescription): string => {
        if (description.type === InternalSerializerType.ARRAY) {
            const readFunction = genBuiltinDeclaration(description.type);
            return `
            {
                const array = ${readFunction}();
                if (array) {
                    for (let i = 0; i < array.length; i++) {
                        ${descriptionReadExpression('array[i]', Cast<ArrayTypeDescription>(description).options!.inner)}
                    }
                }
                ${key} = array;
            }
            `
        }
        if (description.type === InternalSerializerType.FURY_TYPE_TAG) {
            genReadSerializer(fury, description, stack);
            return `${key} = ${genTagDeclaration(Cast<ObjectTypeDescription>(description).options!.tag!)}();`;
        }
        return `${key} = ${genBuiltinDeclaration(description.type)}();`;
    }
    const genEntry = (description: TypeDescription) => {
        return `
            {
                // relation tag: ${Cast<ObjectTypeDescription>(description).options?.tag}
                const result = {
                    ${Object.entries(Cast<ObjectTypeDescription>(description).options?.props!).map(([key]) => {
            return `${safePropName(key)}: null,`
        }).join('\n')}
                };
                pushReadObject(result);
                ${Object.entries(Cast<ObjectTypeDescription>(description).options?.props!).map(([key, value]) => {
            return descriptionReadExpression(`result${safePropAccessor(key)}`, value);
        }).join('\n')}
                return result;
            }
        `;
    }
    if (description.type !== InternalSerializerType.FURY_TYPE_TAG || !Cast<ObjectTypeDescription>(description).options?.tag) {
        throw new Error('root type should be object')
    }
    const tag = Cast<ObjectTypeDescription>(description).options!.tag;
    if (fury.classResolver.existsTagReadSerializer(tag) || stack.includes(tag)) {
        return;
    }
    stack.push(tag);
    const entry = genEntry(description);
    const expectHash = computeTagHash(description);
    fury.classResolver.registerReadSerializerByTag(tag, new Function(
        `
    return function(fury) {
        const { referenceResolver, binaryView, classResolver, skipType, buildRefTypeReader } = fury; 
        const { pushReadObject, readRefFlag, getReadObjectByRefId } = referenceResolver;

        ${finish().join('\n')}
        return function() {
            const hash = binaryView.readInt32();
            if (hash !== ${expectHash}) {
                throw new Error("validate hash failed: ${replaceBackslashAndQuote(tag)}. expect ${expectHash}, but got" + hash);
            }
            ${entry};
        }
    }
`
    )()(fury));
}



export const genWriteSerializer = (fury: Fury, description: TypeDescription, stack: string[] = []) => {
    const { genBuiltinDeclaration, genTagDeclaration, finish } = typeHandlerDeclaration('write', fury);
    const genTagWriterStmt = (v: string, description: TypeDescription): string => {
        if (description.type === InternalSerializerType.ARRAY) {
            const inner = genTagWriterStmt("item", Cast<ArrayTypeDescription>(description).options?.inner);
            return `
            ${genBuiltinDeclaration(description.type)}(${v});
            for (const item of ${v}) {
                        ${inner}
            }
            `
        }
        if (description.type === InternalSerializerType.FURY_TYPE_TAG) {
            const tag = Cast<ObjectTypeDescription>(description).options?.tag;
            genWriteSerializer(fury, description, stack);
            return `
            ${genTagDeclaration(tag!)}.write(${v});`

        }
        return `
            ${genBuiltinDeclaration(description.type)}(${v});`
    }
    const genEntry = (description: TypeDescription) => {
        return Object.entries(Cast<ObjectTypeDescription>(description).options?.props).map(([key, value]) => {
            return `${genTagWriterStmt(`v${safePropAccessor(key)}`, value)}`
        }).join('');
    }
    if (description.type !== InternalSerializerType.FURY_TYPE_TAG || !Cast<ObjectTypeDescription>(description).options?.tag) {
        throw new Error('root type should be object')
    }
    const tag = Cast<ObjectTypeDescription>(description).options?.tag;
    if (fury.classResolver.existsTagWriteSerializer(tag) || stack.includes(tag)) {
        return;
    }
    stack.push(tag);
    const entry = genEntry(description);
    const tagByteLen = Buffer.from(tag).byteLength;
    fury.classResolver.registerWriteSerializerByTag(tag, new Function(
        `
    return function(fury, enums) {
        const { referenceResolver, binaryWriter, classResolver, writeNullOrRef } = fury; 
        const { pushWriteObject } = referenceResolver;
        ${finish().join('')}
        const tagBuffer = Buffer.from("${replaceBackslashAndQuote(tag)}");
        const bufferLen = ${tagByteLen};
        return function(v) {
            // relation tag: ${tag}
            const { RefFlags, InternalSerializerType } = enums;
            // is ref
            if (writeNullOrRef(v)) {
                return;
            }
            binaryWriter.reserves(${Object.values(Cast<ObjectTypeDescription>(description).options?.props).map(x => {
            const serializer = fury.classResolver.getSerializerById(x.type);
            if (serializer && serializer.config) {
                return serializer.config().reserve;
            }
            return 0;
        }).reduce((accumulator, currentValue) => accumulator + currentValue, 0)});
            // write ref ant typeId
            binaryWriter.writeInt8(RefFlags.RefValueFlag);
            pushWriteObject(v);
            binaryWriter.writeInt16(InternalSerializerType.FURY_TYPE_TAG);
            classResolver.writeTag(binaryWriter, "${replaceBackslashAndQuote(tag)}", tagBuffer, bufferLen);
            binaryWriter.writeInt32(${computeTagHash(description)});
            ${entry}
        }
    }
`
    )()(fury, {
        InternalSerializerType,
        RefFlags
    }), {
        reserve: tagByteLen + 7,
        refType: true
    });
}