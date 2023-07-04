import { genReadSerializer, genWriteSerializer, TypeDescription } from './lib/codeGen';
import { Serializer, SerializerRead, SerializerWrite, Fury, InternalSerializerType } from './lib/type';
import FuryInternal from './lib/fury';

export default class {
    private fury: Fury = FuryInternal();

    registerSerializerByDescription(description: TypeDescription) {
        if (description.type !== InternalSerializerType.FURY_TYPE_TAG || !description.asObject?.tag) {
            throw new Error('root type should be object')
        }
        genReadSerializer(
            this.fury,
            description,
            (tag: string, reader: SerializerRead) => {
                this.fury.classResolver.registerReadSerializerByTag(tag, reader);
            },
            (tag: string) => {
                return this.fury.classResolver.existsTagReadSerializer(tag);
            }
        );
        genWriteSerializer(
            this.fury,
            description,
            (tag: string, writer: SerializerWrite) => {
                this.fury.classResolver.registerWriteSerializerByTag(tag, writer);
            },
            (tag: string) => {
                return this.fury.classResolver.existsTagWriteSerializer(tag);
            }
        );
        return this.fury.classResolver.getSerializerByTag(description.asObject.tag);
    }

    registerSerializer(tag: string, serializer: Serializer) {
        this.fury.classResolver.registerSerializerByTag(tag, serializer);
    }

    marshal(v: any, serialize?: Serializer) {
        return this.fury.marshal(v, serialize);
    }

    unmarshal(bytes: Uint8Array) {
        return this.fury.unmarshal(bytes);
    }
}
