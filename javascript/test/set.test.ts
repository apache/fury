import Fury from '../index';
import { genReadSerializer, TypeDefinition } from '../lib/codeGen';
import { describe, expect, test } from '@jest/globals';
import { InternalSerializerType } from '../lib/type';

describe('set', () => {
    test('should set work', () => {
        const fury = new Fury();
        const input = fury.marshal(new Set(["foo1", "bar1", "cc2"]));
        const result = fury.unmarshal(
            new Uint8Array(input)
        );
        expect(result).toEqual(new Set(["foo1", "bar1", "cc2"]))
    });
    test('should set in object work', () => {
        const definition: TypeDefinition = {
            type: InternalSerializerType.FURY_TYPE_TAG,
            asObject: {
                props: {
                    a: {
                        type: InternalSerializerType.FURY_SET
                    },
                },
                tag: "example.foo"
            }
        };
        const fury = new Fury();
        fury.registerSerializerByDefinition(definition);
        const input = fury.marshal({ a: new Set(["foo1", "bar2"]) }, "example.foo");
        const result = fury.unmarshal(
            new Uint8Array(input)
        );
        expect(result).toEqual({ a: new Set(["foo1", "bar2"]) })
    });
});


