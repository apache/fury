import Fury, { TypeDescription, InternalSerializerType, Type } from '@furyjs/fury';
import { describe, expect, test } from '@jest/globals';

describe('object', () => {
  test('should object work', () => {
    const description = {
      type: InternalSerializerType.FURY_TYPE_TAG as const,
      options: {
        props: {
          a: {
            type: InternalSerializerType.FURY_TYPE_TAG as const,
            options: {
              tag: "example.bar",
              props: {
                b: {
                  type: InternalSerializerType.STRING as const,
                },
              }
            }
          }
        },
        tag: "example.foo"
      }
    };
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });
    const { serialize, deserialize } = fury.registerSerializer(description);
    const input = serialize({ a: { b: "hel" } });
    const result = deserialize(
      input
    );
    expect(result).toEqual({ a: { b: "hel" } })
  });

  test('should object in array work', () => {
    const description = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        props: {
          a: {
            type: InternalSerializerType.ARRAY,
            options: {
              inner: {
                type: InternalSerializerType.FURY_TYPE_TAG,
                options: {
                  tag: "example.bar",
                  props: {
                    b: {
                      type: InternalSerializerType.STRING
                    },
                  }
                }
              }
            }
          }
        },
        tag: "example.foo"
      }
    };
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });
    const serializer = fury.registerSerializer(description).serializer;
    const input = fury.serialize({ a: [{ b: "hel" }] }, serializer);
    const result = fury.deserialize(
      input
    );
    expect(result).toEqual({ a: [{ b: "hel" }] })
  });

  test('should write tag and read tag work', () => {
    const description = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        props: {
          a: {
            type: InternalSerializerType.FURY_TYPE_TAG as const,
            options: {
              tag: "example.bar",
              props: {
                b: {
                  type: InternalSerializerType.STRING
                },
              }
            }
          },
          a2: {
            type: InternalSerializerType.FURY_TYPE_TAG as const,
            options: {
              tag: "example.bar",
            }
          }
        },
        tag: "example.foo"
      }
    };
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });
    const serializer = fury.registerSerializer(description).serializer;
    const input = fury.serialize({ a: { b: "hel" }, a2: { b: "hel2" } }, serializer);
    const result = fury.deserialize(
      input
    );
    expect(result).toEqual({ a: { b: "hel" }, a2: { b: "hel2" } })
  });

  test('should ciycle ref work', () => {
    const description = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        props: {
          a: {
            type: InternalSerializerType.FURY_TYPE_TAG as const,
            options: {
              tag: "example.bar",
              props: {
                b: {
                  type: InternalSerializerType.STRING
                },
              }
            }
          },
          a2: {
            type: InternalSerializerType.FURY_TYPE_TAG as const,
            options: {
              tag: "example.foo",
            }
          }
        },
        tag: "example.foo"
      }
    };
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });
    const serialize = fury.registerSerializer(description).serializer;
    const param: any = {};
    param.a = { b: "hel" };
    param.a2 = param;
    const input = fury.serialize(param, serialize);
    const result = fury.deserialize(
      input
    );
    expect(result.a).toEqual({ b: "hel" })
    expect(result.a2).toEqual(result)
  });

  test('should type function tools work', () => {
    const description = Type.object("example.foo", {
      a: Type.object("example\".bar", {
        b: Type.string(),
        c: Type.array(Type.object("example\\\".foo2", {
          d: Type.string(),
        }))
      }),
    })
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });
    const { serialize, deserialize } = fury.registerSerializer(description);
    const input = serialize({ a: { b: "hel", c: [{ d: "hello" }] } });
    const result = deserialize(
      input
    );
    expect(result).toEqual({ a: { b: "hel", c: [{ d: "hello" }] } })
  });
});


