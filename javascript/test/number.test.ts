import Fury from '../index';
import { describe, expect, test } from '@jest/globals';
import { InternalSerializerType } from '../lib/type';

describe('number', () => {
  test('should i8 work', () => {
    const fury = new Fury();
    const serialize = fury.registerSerializerByDescription({
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.INT8
          }
        }
      }
    });
    const input = fury.marshal({ a: 1 }, serialize);
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should i16 work', () => {
    const fury = new Fury();
    const serialize = fury.registerSerializerByDescription({
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.INT16,
          }
        }
      }
    });
    const input = fury.marshal({ a: 1 }, serialize);
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should i32 work', () => {
    const fury = new Fury();
    const serializer = fury.registerSerializerByDescription({
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.INT32,
          }
        }
      }
    });
    const input = fury.marshal({ a: 1 }, serializer);
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should i64 work', () => {
    const fury = new Fury();
    const serializer = fury.registerSerializerByDescription({
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.INT64
          }
        }
      }
    });
    const input = fury.marshal({ a: 1 }, serializer);
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should u8 work', () => {
    const fury = new Fury();
    const serializer = fury.registerSerializerByDescription({
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.UINT8,
          }
        }
      }
    });
    const input = fury.marshal({ a: 1 }, serializer);
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should u16 work', () => {
    const fury = new Fury();
    const serializer = fury.registerSerializerByDescription({
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.UINT16
          }
        }
      }
    });
    const input = fury.marshal({ a: 1 }, serializer);
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should u32 work', () => {
    const fury = new Fury();
    const serializer = fury.registerSerializerByDescription({
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.UINT32
          }
        }
      }
    });
    const input = fury.marshal({ a: 1 }, serializer);
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should u64 work', () => {
    const fury = new Fury();
    const serializer = fury.registerSerializerByDescription({
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.UINT64
          }
        }
      }
    });
    const input = fury.marshal({ a: 1 }, serializer);
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: 1 })
  });
  test('should float work', () => {
    const fury = new Fury();
    const serializer = fury.registerSerializerByDescription({
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.FLOAT
          }
        }
      }
    });
    const input = fury.marshal({ a: 1.2 }, serializer);
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result.a).toBeCloseTo(1.2)
  });
  test('should double work', () => {
    const fury = new Fury();
    const serializer = fury.registerSerializerByDescription({
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.DOUBLE
          }
        }
      }
    });
    const input = fury.marshal({ a: 1.2 }, serializer);
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result.a).toBeCloseTo(1.2)
  });
});


