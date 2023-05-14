import { genReadSerializer, genWriteSerializer, TypeDefinition } from './lib/codeGen';
import { Serializer, SerializerRead, SerializerWrite, Fury } from './lib/type';
import FuryInternal from './lib/fury';

export default class {
    private fury: Fury = FuryInternal();

    registerSerializerByDefinition(definition: TypeDefinition) {
        genReadSerializer(
            this.fury,
            definition,
            (tag: string, reader: SerializerRead) => {
                this.fury.classResolver.registerReadSerializerByTag(tag, reader);
            },
            (tag: string) => {
                return this.fury.classResolver.existsTagReadSerializer(tag);
            }
        );
        genWriteSerializer(
            this.fury,
            definition,
            (tag: string, writer: SerializerWrite) => {
                this.fury.classResolver.registerWriteSerializerByTag(tag, writer);
            },
            (tag: string) => {
                return this.fury.classResolver.existsTagWriteSerializer(tag);
            }
        );
    }

    registerSerializer(tag: string, serializer: Serializer) {
        this.fury.classResolver.registerSerializerByTag(tag, serializer);
    }

    marshal(v: any, tag = '') {
        return this.fury.marshal(v, tag);
    }

    unmarshal(bytes: Uint8Array) {
        return this.fury.unmarshal(bytes);
    }
}
