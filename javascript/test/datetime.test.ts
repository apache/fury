
import Fury, { TypeDescription, InternalSerializerType } from '@furyjs/fury';
import {describe, expect, test} from '@jest/globals';

describe('datetime', () => {
  test('should date work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const now = new Date();
    const input = fury.serialize(now);
    const result = fury.deserialize(
        input
    );
    expect(result).toEqual(now)
  });
  test('should datetime work', () => {
    const description: TypeDescription = {
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
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serializer = fury.registerSerializer(description).serializer;
    const d = new Date('2021/10/20 09:13');
    const input = fury.serialize({ a:  d, b: d}, serializer);
    const result = fury.deserialize(
      input
    );
    expect(result).toEqual({ a: d, b: new Date('2021/10/20 00:00') })
  });
});


