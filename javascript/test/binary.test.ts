import Fury, { TypeDescription, InternalSerializerType, ObjectTypeDescription } from '@furyjs/fury';
import { describe, expect, test } from '@jest/globals';


describe('binary', () => {
    test('should binary work', () => {
        const description = {
            type: InternalSerializerType.FURY_TYPE_TAG,
            options: {
                props: {
                    a: {
                        type: InternalSerializerType.BINARY
                    }
                },
                tag: "example.foo"
            }
        };
        const hps = process.env.enableHps ? require('@furyjs/hps') : null;
        const fury = new Fury({ hps });    
        const serializer = fury.registerSerializer(description).serializer;
        const input = fury.serialize({ a: new Uint8Array([1, 2, 3]) }, serializer);
        const result = fury.deserialize(
            input
        );
        expect(result).toEqual({ a: new Uint8Array([1, 2, 3]) })
    });
});


