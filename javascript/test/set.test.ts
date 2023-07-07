import Fury from '../index';
import { genReadSerializer, TypeDescription } from '../lib/codeGen';
import { describe, expect, test } from '@jest/globals';
import { InternalSerializerType } from '../lib/type';

describe('set', () => {
    test('should set work', () => {
        const fury = new Fury();
        const input = fury.marshal(new Set(["foo1", "bar1", "cc2"]));
        const result = fury.unmarshal(
            input
        );
        expect(result).toEqual(new Set(["foo1", "bar1", "cc2"]))
    });
    test('should set in object work', () => {
        const description: TypeDescription = {
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
        const serializer = fury.registerSerializerByDescription(description);
        const input = fury.marshal({ a: new Set(["foo1", "bar2"]) }, serializer);
        const result = fury.unmarshal(
            input
        );
        expect(result).toEqual({ a: new Set(["foo1", "bar2"]) })
    });
});


