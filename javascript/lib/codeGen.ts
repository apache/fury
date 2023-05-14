import { InternalSerializerType, MaxInt32, RefFlags, Serializer, SerializerRead, Fury } from './type';


export interface TypeDefinition {
    type: InternalSerializerType,
    label?: string,
    asObject?: {
        props?: { [key: string]: TypeDefinition },
        tag: string,
    }
    asArray?: {
        item: TypeDefinition,
    }
}

export const computeStringHash = (str: string) => {
    const bytes = Buffer.from(str, 'utf-8');
    let hash = 17
    bytes.forEach(b => {
        hash = hash * 31 + b
        do {
            hash = Math.round(hash / 7)
        } while (hash >= MaxInt32);
    });
    return hash;
}

export const computeFieldHash = (hash: number, t: TypeDefinition) => {
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

export const computeTagHash = (definition: TypeDefinition) => {
    if (definition.type !== InternalSerializerType.FURY_TYPE_TAG) {
        throw new Error('only object is hashable');
    }
    let hash = 17;
    for (const [, value] of Object.entries(definition.asObject!.props!)) {
        hash = computeFieldHash(hash, value)
    }
    return hash;
}

export const genReadSerializer = (fury: Fury, definition: TypeDefinition, regTag: (tag: string, reader: SerializerRead) => void, exists: (tag: string) => boolean, stack: string[] = []) => {
    const readerSerializerStmt: string[] = [];
    let count = 0;
    const genTagReaderSerializerStmt = (tag: string) => {
        const result = `tag_reader_${count++}`;
        readerSerializerStmt.push(`
            // ${tag}
            let ${result} = null;
            const ${result}_get = () => {
                if (${result}) {
                    return ${result};
                }
                ${result} = classResolver.getSerializerByTag("${tag}").read;
                return ${result};
            }`
        )
        return `${result}_get`;
    }
    const definitionReadExpression = (definition: TypeDefinition): string => {
        if (definition.type === InternalSerializerType.ARRAY) {
            return `
            readBySerializer(type_${definition.type}_serializer, [
                () => ${definitionReadExpression(definition.asArray!.item)}
            ])`
        }
        if (definition.type === InternalSerializerType.FURY_TYPE_TAG) {
            genReadSerializer(fury, definition, regTag, exists, stack);
            return `
            readBySerializer(${genTagReaderSerializerStmt(definition.asObject!.tag!)}())`
        }
        return `
            readBySerializer(type_${definition.type}_serializer)`
    }
    const genEntry = (definition: TypeDefinition) => {
        return `
            const entry = () => {
                // relation tag: ${definition.asObject?.tag}
                const result = {
                    ${Object.entries(definition.asObject!.props!).map(([key]) => {
            return `${key}: null,`
        }).join('\n')
            }
                };
                if (shouldSetRef) {
                    pushReadObject(result);
                }
                ${Object.entries(definition.asObject!.props!).map(([key, value]) => {
                return `
                            result.${key} = ${definitionReadExpression(value)};
                        `
            }).join('\n')
            }
                return result;
            }
            return entry();
        `;
    }
    if (definition.type !== InternalSerializerType.FURY_TYPE_TAG || !definition.asObject?.tag) {
        throw new Error('root type should be object')
    }
    if (exists(definition.asObject.tag) || stack.includes(definition.asObject.tag)) {
        return;
    }
    stack.push(definition.asObject.tag);
    const entry = genEntry(definition);
    const expectHash = computeTagHash(definition);
    regTag(definition.asObject!.tag, new Function(
        `
    return function(fury) {
        const { readBySerializer, referenceResolver, binaryView, classResolver } = fury; 
        const { pushReadObject } = referenceResolver;
        ${Object.values(InternalSerializerType).filter(x => typeof x === 'number').map(y => {
            return `const type_${y}_serializer = classResolver.getSerializerById(${y}).read;`
        }).join('\n')
        }

        ${readerSerializerStmt.join('\n')}
        return function(shouldSetRef) {
            const hash = binaryView.readInt32();
            if (hash !== ${expectHash}) {
                throw new Error("validate hash failed: ${definition.asObject.tag}. expect ${expectHash}, but got" + hash);
            }
            ${entry};
        }
    }
`
    )()(fury));
}



export const genWriteSerializer = (fury: Fury, definition: TypeDefinition, regTag: (tag: string, reader: Serializer["write"]) => void, exists: (tag: string) => boolean, stack: string[] = []) => {
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
    const definitionWriteExpression = (definition: TypeDefinition): string => {
        if (definition.type === InternalSerializerType.ARRAY) {
            return `
                type_${definition.type}_serializer(v, [
                    (v) => ${definitionWriteExpression(definition.asArray!.item)}
                ])
            `
        }
        if (definition.type === InternalSerializerType.FURY_TYPE_TAG) {
            genWriteSerializer(fury, definition, regTag, exists, stack);
            return `
                ${genTagWriterSerializerStmt(definition.asObject!.tag!)}()(v, [],"${definition.asObject!.tag!}")`
        }
        return `type_${definition.type}_serializer(v)`
    }
    const genEntry = (definition: TypeDefinition) => {
        return `
            const entry = (input) => {
                // relation tag: ${definition.asObject?.tag}
                ${Object.entries(definition.asObject!.props!).map(([key, value]) => {
            return `
                            // write ${key}
                            {
                                const v = input.${key};
                                ${definitionWriteExpression(value)};
                            };
                        `
        }).join('\n')
            }
            }
            return entry(v);
        `;
    }
    if (definition.type !== InternalSerializerType.FURY_TYPE_TAG || !definition.asObject?.tag) {
        throw new Error('root type should be object')
    }
    if (exists(definition.asObject.tag) || stack.includes(definition.asObject.tag)) {
        return;
    }
    stack.push(definition.asObject.tag);
    const entry = genEntry(definition);
    regTag(definition.asObject!.tag, new Function(
        `
    return function(fury, enums) {
        const { readBySerializer, referenceResolver, binaryWriter, classResolver, writeNullOrRef } = fury; 
        const { pushWriteObject } = referenceResolver;
        ${Object.values(InternalSerializerType).filter(x => typeof x === 'number').map(y => {
            return `const type_${y}_serializer = classResolver.getSerializerById(${y}).write;`
        }).join('\n')
        }

        ${writerSerializerStmt.join('\n')}
        return function(v, genericWriters, tag) {
            const { RefFlags, InternalSerializerType } = enums;
            // is ref
            if (writeNullOrRef(v)) {
                return;
            }
            // write ref ant typeId
            binaryWriter.writeInt8(RefFlags.RefValueFlag);
            pushWriteObject(v);
            binaryWriter.writeInt16(InternalSerializerType.FURY_TYPE_TAG);
            classResolver.writeTag(binaryWriter, tag);
            binaryWriter.writeInt32(${computeTagHash(definition)});
            ${entry};
        }
    }
`
    )()(fury, {
        InternalSerializerType,
        RefFlags
    }));
}