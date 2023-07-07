import Fury from '../index';
import { describe, expect, test } from '@jest/globals';
import { TypeDescription } from '../lib/codeGen';
import { InternalSerializerType } from '../lib/type';

describe('array', () => {
  test('should array work', () => {
    const fury = new Fury();
    const result = fury.unmarshal(
      Buffer.from([6, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 1, 2, 0, 0, 0, 0, 0, 4, 115, 116, 114, 49, 254, 1])
    );
    expect(result).toEqual(["str1", "str1"])
  });
  test('should typedarray work', () => {
    const description: TypeDescription = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        props: {
          a: {
            type: InternalSerializerType.FURY_PRIMITIVE_BOOL_ARRAY,
          },
          a2: {
            type: InternalSerializerType.FURY_PRIMITIVE_SHORT_ARRAY,
          },
          a3: {
            type: InternalSerializerType.FURY_PRIMITIVE_INT_ARRAY,
          },
          a4: {
            type: InternalSerializerType.FURY_PRIMITIVE_LONG_ARRAY,
          },
          a6: {
            type: InternalSerializerType.FURY_PRIMITIVE_DOUBLE_ARRAY,
          },
          a7: {
            type: InternalSerializerType.FURY_STRING_ARRAY
          },
        },
        tag: "example.foo"
      }
    };
    const fury = new Fury();
    const serializer = fury.registerSerializerByDescription(description);
    const input = fury.marshal({
      a: [true, false],
      a2: [1, 2, 3],
      a3: [3, 5, 76],
      a4: [634, 564, 76],
      a6: [234243.555, 55654.6786],
      a7: ["hello", "world"]
    }, serializer);
    const result = fury.unmarshal(
      input
    );
    expect(result).toEqual({
      a: [true, false],
      a2: [1, 2, 3],
      a3: [3, 5, 76],
      a4: [634, 564, 76],
      a6: [234243.555, 55654.6786],
      a7: ["hello", "world"]
    })
  });
  test('should floatarray work', () => {
    const description: TypeDescription = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        props: {
          a5: {
            type: InternalSerializerType.FURY_PRIMITIVE_FLOAT_ARRAY,
          },
        },
        tag: "example.foo"
      }
    };
    const fury = new Fury();
    const serialize = fury.registerSerializerByDescription(description);
    const input = fury.marshal({
      a5: [2.43, 654.4, 55],
    }, serialize);
    const result = fury.unmarshal(
      input
    );
    expect(result.a5[0]).toBeCloseTo(2.43)
    expect(result.a5[1]).toBeCloseTo(654.4)
    expect(result.a5[2]).toBeCloseTo(55)
  });
});


