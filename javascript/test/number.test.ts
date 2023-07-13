import Fury, { TypeDescription, InternalSerializerType } from '@furyjs/fury';
import { describe, expect, test } from '@jest/globals';

describe('number', () => {
  test('should i8 work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serialize = fury.registerSerializer({
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.INT8
          }
        }
      }
    }).serializer;
    const input = fury.serialize({ a: 1 }, serialize);
    const result = fury.deserialize(
      input
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should i16 work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serialize = fury.registerSerializer({
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.INT16,
          }
        }
      }
    }).serializer;
    const input = fury.serialize({ a: 1 }, serialize);
    const result = fury.deserialize(
      input
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should i32 work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.INT32,
          }
        }
      }
    }).serializer;
    const input = fury.serialize({ a: 1 }, serializer);
    const result = fury.deserialize(
      input
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should i64 work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.INT64
          }
        }
      }
    }).serializer;
    const input = fury.serialize({ a: 1 }, serializer);
    const result = fury.deserialize(
      input
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should u8 work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.UINT8,
          }
        }
      }
    }).serializer;
    const input = fury.serialize({ a: 1 }, serializer);
    const result = fury.deserialize(
      input
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should u16 work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.UINT16
          }
        }
      }
    }).serializer;
    const input = fury.serialize({ a: 1 }, serializer);
    const result = fury.deserialize(
      input
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should u32 work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.UINT32
          }
        }
      }
    }).serializer;
    const input = fury.serialize({ a: 1 }, serializer);
    const result = fury.deserialize(
      input
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should u64 work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.UINT64
          }
        }
      }
    }).serializer;
    const input = fury.serialize({ a: 1 }, serializer);
    const result = fury.deserialize(
      input
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should float work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.FLOAT
          }
        }
      }
    }).serializer;
    const input = fury.serialize({ a: 1.2 }, serializer);
    const result = fury.deserialize(
      input
    );
    expect(result.a).toBeCloseTo(1.2)
  });
  test('should double work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.DOUBLE
          }
        }
      }
    }).serializer;
    const input = fury.serialize({ a: 1.2 }, serializer);
    const result = fury.deserialize(
      input
    );
    expect(result.a).toBeCloseTo(1.2)
  });
});


