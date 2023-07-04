import { InternalSerializerType, MaxInt32, RefFlags, Serializer, SerializerRead, Fury } from './type';
import { utf8Encoder } from './util';


export interface TypeDescription {
    type: InternalSerializerType,
    label?: string,
    asObject?: {
        props?: { [key: string]: TypeDescription },
        tag: string,
    }
    asArray?: {
        item: TypeDescription,
    }
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
        id = computeStringHash(t.asObject!.tag!);
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
    for (const [, value] of Object.entries(description.asObject!.props!)) {
        hash = computeFieldHash(hash, value)
    }
    return hash;
}

export const genReadSerializer = (fury: Fury, description: TypeDescription, regTag: (tag: string, reader: SerializerRead) => void, exists: (tag: string) => boolean, stack: string[] = []) => {
    const readerSerializerStmt: string[] = [];
    let count = 0;
    const existsStmt = new Map();
    const existsBuiltinStmt = new Map();

    const genBuiltinReaderSerializerStmt = (type: number) => {
        if (existsBuiltinStmt.has(type)) {
            return existsBuiltinStmt.get(type);
        }
        readerSerializerStmt.push(`const type_${type}_serializer = classResolver.getSerializerById(${type}).read;`)
        existsBuiltinStmt.set(type, `type_${type}_serializer`);
        return `type_${type}_serializer`;
    }
    const genTagReaderSerializerStmt = (tag: string) => {
        if (existsStmt.has(tag)) {
            return existsStmt.get(tag);
        }
        const result = `tag_reader_${count++}`;
        readerSerializerStmt.push(`
            // ${tag}
            let ${result} = null;
            const ${result}_get = (a, b) => {
                if (${result}) {
                    return ${result}(a, b);
                }
                ${result} = classResolver.getSerializerByTag("${tag}").read;
                return ${result}(a, b);
            }`
        )
        existsStmt.set(tag, `${result}_get`);
        return `${result}_get`;
    }
    const descriptionReadExpression = (key: string, description: TypeDescription): string => {
        const template = key ? (readFunctionName: string, genericReaders?: string) => `
        switch (readRefFlag(binaryView)) {
            case ${RefFlags.RefValueFlag}:
                skipType();
                result.${key} = ${readFunctionName}(true, ${genericReaders});
                break;
            case ${RefFlags.RefFlag}:
                result.${key} =  getReadObjectByRefId(binaryView.readVarInt32());
                break;
            case ${RefFlags.NullFlag}:
                result.${key} = null;
                break;
            case ${RefFlags.NotNullValueFlag}:
                skipType();
                result.${key} = ${readFunctionName}(false, ${genericReaders})
                break;
        }
        ` : (readFunctionName: string, genericReaders?: string) => `
        () => {
            switch (readRefFlag(binaryView)) {
                case ${RefFlags.RefValueFlag}:
                    skipType();
                    return ${readFunctionName}(true, ${genericReaders});
                case ${RefFlags.RefFlag}:
                    return getReadObjectByRefId(binaryView.readVarInt32());
                case ${RefFlags.NullFlag}:
                    return null;
                case ${RefFlags.NotNullValueFlag}:
                    skipType();
                    return ${readFunctionName}(false, ${genericReaders})
            }
        }
        `
        if (description.type === InternalSerializerType.ARRAY) {
            return template(genBuiltinReaderSerializerStmt(description.type), `[
                ${descriptionReadExpression('', description.asArray!.item)}
            ]`);
        }
        if (description.type === InternalSerializerType.FURY_TYPE_TAG) {
            genReadSerializer(fury, description, regTag, exists, stack);
            return template(genTagReaderSerializerStmt(description.asObject!.tag!));
        }
        return template(genBuiltinReaderSerializerStmt(description.type));
    }
    const genEntry = (description: TypeDescription) => {
        return `
            {
                // relation tag: ${description.asObject?.tag}
                const result = {
                    ${Object.entries(description.asObject!.props!).map(([key]) => {
            return `${key}: null,`
        }).join('\n')
            }
                };
                if (shouldSetRef) {
                    pushReadObject(result);
                }
                ${Object.entries(description.asObject!.props!).map(([key, value]) => {
                return descriptionReadExpression(key, value);
            }).join('\n')
            }
                return result;
            }
        `;
    }
    if (description.type !== InternalSerializerType.FURY_TYPE_TAG || !description.asObject?.tag) {
        throw new Error('root type should be object')
    }
    if (exists(description.asObject.tag) || stack.includes(description.asObject.tag)) {
        return;
    }
    stack.push(description.asObject.tag);
    const entry = genEntry(description);
    const expectHash = computeTagHash(description);
    regTag(description.asObject!.tag, new Function(
        `
    return function(fury) {
        const { referenceResolver, binaryView, classResolver, skipType } = fury; 
        const { pushReadObject, readRefFlag, getReadObjectByRefId } = referenceResolver;

        ${readerSerializerStmt.join('\n')}
        return function(shouldSetRef) {
            const hash = binaryView.readInt32();
            if (hash !== ${expectHash}) {
                throw new Error("validate hash failed: ${description.asObject.tag}. expect ${expectHash}, but got" + hash);
            }
            ${entry};
        }
    }
`
    )()(fury));
}



export const genWriteSerializer = (fury: Fury, description: TypeDescription, regTag: (tag: string, reader: Serializer["write"]) => void, exists: (tag: string) => boolean, stack: string[] = []) => {
    const writerSerializerStmt: string[] = [];
    let count = 0;
    const genTagWriterSerializerStmt = (tag: string) => {
        const result = `tag_reader_${count++}`;
        writerSerializerStmt.push(`
            // ${tag}
            let ${result} = null;
            const ${result}_get = () => {
                if (${result}) {
                    return ${result};
                }
                ${result} = classResolver.getSerializerByTag("${tag}").write;
                return ${result};
            }`
        )
        return `${result}_get`;
    }
    const descriptionWriteExpression = (v: string, description: TypeDescription): string => {
        if (description.type === InternalSerializerType.ARRAY) {
            return `
                type_${description.type}_serializer(${v}, [
                    (v) => ${descriptionWriteExpression("v", description.asArray!.item)}
                ])
            `
        }
        if (description.type === InternalSerializerType.FURY_TYPE_TAG) {
            genWriteSerializer(fury, description, regTag, exists, stack);
            return `
                ${genTagWriterSerializerStmt(description.asObject!.tag!)}()(${v}, [],"${description.asObject!.tag!}")`
        }
        return `type_${description.type}_serializer(${v})`
    }
    const genEntry = (description: TypeDescription) => {
        return `
            {
                const { ${Object.keys(description.asObject!.props!).join(',')}} = v;
                // relation tag: ${description.asObject?.tag}
                ${Object.entries(description.asObject!.props!).map(([key, value]) => {
            return `
                            ${descriptionWriteExpression(key, value)};
                        `
        }).join('\n')
            }
            }
        `;
    }
    if (description.type !== InternalSerializerType.FURY_TYPE_TAG || !description.asObject?.tag) {
        throw new Error('root type should be object')
    }
    const tag = description.asObject.tag;
    if (exists(tag) || stack.includes(tag)) {
        return;
    }
    stack.push(tag);
    const entry = genEntry(description);
    regTag(tag, new Function(
        `
    return function(fury, enums) {
        const { referenceResolver, binaryWriter, classResolver, writeNullOrRef } = fury; 
        const { pushWriteObject } = referenceResolver;
        ${Object.values(InternalSerializerType).filter(x => typeof x === 'number').map(y => {
            return `const type_${y}_serializer = classResolver.getSerializerById(${y}).write;`
        }).join('\n')
        }

        ${writerSerializerStmt.join('\n')}
        const tagBuffer = Buffer.from("${tag}");
        return function(v, genericWriters) {
            const { RefFlags, InternalSerializerType } = enums;
            // is ref
            if (writeNullOrRef(v)) {
                return;
            }
            // write ref ant typeId
            binaryWriter.writeInt8(RefFlags.RefValueFlag);
            pushWriteObject(v);
            binaryWriter.writeInt16(InternalSerializerType.FURY_TYPE_TAG);
            classResolver.writeTag(binaryWriter, "${tag}", tagBuffer);
            binaryWriter.writeInt32(${computeTagHash(description)});
            ${entry};
        }
    }
`
    )()(fury, {
        InternalSerializerType,
        RefFlags
    }));
}