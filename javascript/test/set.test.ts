import Fury, { TypeDescription, InternalSerializerType } from '@furyjs/fury';
import { describe, expect, test } from '@jest/globals';

describe('set', () => {
    test('should set work', () => {
        const hps = process.env.enableHps ? require('@furyjs/hps') : null;
        const fury = new Fury({ hps });    
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
        const hps = process.env.enableHps ? require('@furyjs/hps') : null;
        const fury = new Fury({ hps });    
        const serializer = fury.registerSerializerByDescription(description);
        const input = fury.marshal({ a: new Set(["foo1", "bar2"]) }, serializer);
        const result = fury.unmarshal(
            input
        );
        expect(result).toEqual({ a: new Set(["foo1", "bar2"]) })
    });
});


