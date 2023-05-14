
import Fury from '../index';
import {describe, expect, test} from '@jest/globals';
import { TypeDefinition } from '../lib/codeGen';
import { InternalSerializerType } from '../lib/type';

describe('datetime', () => {
  test('should date work', () => {
    const fury = new Fury();
    const now = new Date();
    const input = fury.marshal(now);
    const result = fury.unmarshal(
        new Uint8Array(input)
    );
    expect(result).toEqual(now)
  });
  test('should datetime work', () => {
    const definition: TypeDefinition = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        props: {
          a: {
            type: InternalSerializerType.TIMESTAMP,
          },
          b: {
            type: InternalSerializerType.DATE,
          }
        },
        tag: "example.foo"
      }
    };
    const fury = new Fury();
    fury.registerSerializerByDefinition(definition);
    const d = new Date('2021/10/20 09:13');
    const input = fury.marshal({ a:  d, b: d}, "example.foo");
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: d, b: new Date('2021/10/20 00:00') })
  });
});


