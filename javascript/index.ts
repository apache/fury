import { genReadSerializer, genWriteSerializer, TypeDescription } from './lib/codeGen';
import { Serializer, SerializerRead, SerializerWrite, Fury, InternalSerializerType } from './lib/type';
import FuryInternal from './lib/fury';


export {
    Serializer,
    InternalSerializerType,
    TypeDescription,
}

export default class {
    private fury: Fury = FuryInternal();

    registerSerializerByDescription(description: TypeDescription) {
        if (description.type !== InternalSerializerType.FURY_TYPE_TAG || !description.asObject?.tag) {
            throw new Error('root type should be object')
        }
        genReadSerializer(
            this.fury,
            description,
        );
        genWriteSerializer(
            this.fury,
            description
        );
        return this.fury.classResolver.getSerializerByTag(description.asObject.tag);
    }

    registerSerializer(tag: string, serializer: Serializer) {
        this.fury.classResolver.registerSerializerByTag(tag, serializer);
    }

    marshal(v: any, serialize?: Serializer) {
        return this.fury.marshal(v, serialize);
    }

    unmarshal(bytes: Buffer) {
        return this.fury.unmarshal(bytes);
    }
}
