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

function typeHandlerDeclaration(readOrWrite: 'read' | 'write') {
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
        return addDeclar(name, `
        const ${name} = classResolver.getSerializerById(${type}).${readOrWrite};`)
    }
    const genTagDeclaration = (tag: string) => {
        const name = `tag_${count++}_${readOrWrite}`;
        return addDeclar(name, `
        const ${name} = classResolver.getSerializerByTag("${tag}");`
        )
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
    const { genBuiltinDeclaration, genTagDeclaration, finish } = typeHandlerDeclaration('read');
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
            return template(genBuiltinDeclaration(description.type), `[
                ${descriptionReadExpression('', description.asArray!.item)}
            ]`);
        }
        if (description.type === InternalSerializerType.FURY_TYPE_TAG) {
            genReadSerializer(fury, description, stack);
            return template(`${genTagDeclaration(description.asObject!.tag!)}.read`);
        }
        return template(genBuiltinDeclaration(description.type));
    }
    const genEntry = (description: TypeDescription) => {
        return `
            {
                // relation tag: ${description.asObject?.tag}
                const result = {
                    ${Object.entries(description.asObject!.props!).map(([key]) => {
            return `${key}: null,`
        }).join('\n')}
                };
                if (shouldSetRef) {
                    pushReadObject(result);
                }
                ${Object.entries(description.asObject!.props!).map(([key, value]) => {
            return descriptionReadExpression(key, value);
        }).join('\n')}
                return result;
            }
        `;
    }
    if (description.type !== InternalSerializerType.FURY_TYPE_TAG || !description.asObject?.tag) {
        throw new Error('root type should be object')
    }
    const tag = description.asObject.tag;
    if (fury.classResolver.existsTagReadSerializer(tag) || stack.includes(tag)) {
        return;
    }
    stack.push(tag);
    const entry = genEntry(description);
    const expectHash = computeTagHash(description);
    fury.classResolver.registerReadSerializerByTag(tag, new Function(
        `
    return function(fury) {
        const { referenceResolver, binaryView, classResolver, skipType } = fury; 
        const { pushReadObject, readRefFlag, getReadObjectByRefId } = referenceResolver;

        ${finish().join('\n')}
        return function(shouldSetRef) {
            const hash = binaryView.readInt32();
            if (hash !== ${expectHash}) {
                throw new Error("validate hash failed: ${tag}. expect ${expectHash}, but got" + hash);
            }
            ${entry};
        }
    }
`
    )()(fury));
}



export const genWriteSerializer = (fury: Fury, description: TypeDescription, stack: string[] = []) => {
    const { genBuiltinDeclaration, genTagDeclaration, finish } = typeHandlerDeclaration('write');
    const genTagWriterStmt = (v: string, description: TypeDescription): string => {
        if (description.type === InternalSerializerType.ARRAY) {
            return `
                ${genBuiltinDeclaration(description.type)}(${v}, [
                    (v) => {
                        ${genTagWriterStmt("v", description.asArray!.item)} 
                    }
                ]);`
        }
        if (description.type === InternalSerializerType.FURY_TYPE_TAG) {
            const tag = description.asObject?.tag;
            genWriteSerializer(fury, description, stack);
            return `
                ${genTagDeclaration(tag!)}.write(${v}, []);`

        }
        return `
                ${genBuiltinDeclaration(description.type)}(${v}, []);`
    }
    const genEntry = (description: TypeDescription) => {
        return `
            {
                const { ${Object.keys(description.asObject!.props!).join(',')}} = v;
                ${Object.entries(description.asObject!.props!).map(([key, value]) => {
            return `${genTagWriterStmt(key, value)}`
        }).join('')}
            }
        `;
    }
    if (description.type !== InternalSerializerType.FURY_TYPE_TAG || !description.asObject?.tag) {
        throw new Error('root type should be object')
    }
    const tag = description.asObject.tag;
    if (fury.classResolver.existsTagWriteSerializer(tag) || stack.includes(tag)) {
        return;
    }
    stack.push(tag);
    const entry = genEntry(description);
    fury.classResolver.registerWriteSerializerByTag(tag, new Function(
        `
    return function(fury, enums) {
        const { referenceResolver, binaryWriter, classResolver, writeNullOrRef } = fury; 
        const { pushWriteObject } = referenceResolver;
        ${finish().join('')}
        const tagBuffer = Buffer.from("${tag}");
        return function(v, genericWriters) {
            // relation tag: ${tag}
            const { RefFlags, InternalSerializerType } = enums;
            // is ref
            if (writeNullOrRef(v)) {
                return;
            }
            binaryWriter.reserves(${
                Object.values(description.asObject.props!).map(x => {
                const serializer = fury.classResolver.getSerializerById(x.type);
                if (serializer && serializer.reserveWhenWrite) {
                    return serializer.reserveWhenWrite();
                }
                return 0;
            }).reduce((accumulator, currentValue) => accumulator + currentValue, 0)});
            // write ref ant typeId
            binaryWriter.writeInt8(RefFlags.RefValueFlag);
            pushWriteObject(v);
            binaryWriter.writeInt16(InternalSerializerType.FURY_TYPE_TAG);
            classResolver.writeTag(binaryWriter, "${tag}", tagBuffer);
            binaryWriter.writeInt32(${computeTagHash(description)});
            ${entry}
        }
    }
`
    )()(fury, {
        InternalSerializerType,
        RefFlags
    }));
}