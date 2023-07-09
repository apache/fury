import Fury, { TypeDescription, InternalSerializerType } from '@furyjs/fury';
import { describe, expect, test } from '@jest/globals';


describe('binary', () => {
    test('should binary work', () => {
        const description: TypeDescription = {
            type: InternalSerializerType.FURY_TYPE_TAG,
            asObject: {
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
        const serializer = fury.registerSerializerByDescription(description);
        const input = fury.marshal({ a: new Uint8Array([1, 2, 3]) }, serializer);
        const result = fury.unmarshal(
            input
        );
        expect(result).toEqual({ a: new Uint8Array([1, 2, 3]) })
    });
});


