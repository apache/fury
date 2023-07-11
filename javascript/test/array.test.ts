import Fury, { TypeDescription, InternalSerializerType } from '@furyjs/fury';
import { describe, expect, test } from '@jest/globals';

describe('array', () => {
  test('should array work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps }); const result = fury.deserialize(
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
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
  const fury = new Fury({ hps }); const serializer = fury.registerSerializer(description).serializer;
    const input = fury.serialize({
      a: [true, false],
      a2: [1, 2, 3],
      a3: [3, 5, 76],
      a4: [634, 564, 76],
      a6: [234243.555, 55654.6786],
      a7: ["hello", "world"]
    }, serializer);
    const result = fury.deserialize(
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
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps }); const serialize = fury.registerSerializer(description).serializer;
    const input = fury.serialize({
      a5: [2.43, 654.4, 55],
    }, serialize);
    const result = fury.deserialize(
      input
    );
    expect(result.a5[0]).toBeCloseTo(2.43)
    expect(result.a5[1]).toBeCloseTo(654.4)
    expect(result.a5[2]).toBeCloseTo(55)
  });
});


