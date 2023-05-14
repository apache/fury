import Fury from '../index';
import { describe, expect, test } from '@jest/globals';
import { TypeDefinition } from '../lib/codeGen';
import { InternalSerializerType } from '../lib/type';

describe('binary', () => {
    test('should binary work', () => {
        const definition: TypeDefinition = {
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
        const fury = new Fury();
        fury.registerSerializerByDefinition(definition);
        const input = fury.marshal({ a: new Uint8Array([1, 2, 3]) }, "example.foo");
        const result = fury.unmarshal(
            new Uint8Array(input)
        );
        expect(result).toEqual({ a: new Uint8Array([1, 2, 3]) })
    });
});


