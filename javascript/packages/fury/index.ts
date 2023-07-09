import { genReadSerializer, genWriteSerializer, TypeDescription } from './lib/codeGen';
import { Serializer, Fury, InternalSerializerType, Hps } from './lib/type';
import FuryInternal from './lib/fury';

export {
    Serializer,
    InternalSerializerType,
    TypeDescription,
}

export default class {
    constructor(private config?: {
        hps: Hps | null;
    }) {

    }
    private fury: Fury = FuryInternal(this.config);

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
