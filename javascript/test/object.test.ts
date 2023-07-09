import Fury, { TypeDescription, InternalSerializerType } from '@furyjs/fury';
import { describe, expect, test } from '@jest/globals';

describe('object', () => {
  test('should object work', () => {
    const description: TypeDescription = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        props: {
          a: {
            type: InternalSerializerType.FURY_TYPE_TAG as const,
            asObject: {
              tag: "example.bar",
              props: {
                b: {
                  type: InternalSerializerType.STRING
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
    const serializer = fury.registerSerializerByDescription(description);
    const input = fury.marshal({ a: { b: "hel" } }, serializer);
    const result = fury.unmarshal(
      input
    );
    expect(result).toEqual({ a: { b: "hel" } })
  });

  test('should object in array work', () => {
    const description: TypeDescription = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        props: {
          a: {
            type: InternalSerializerType.ARRAY,
            asArray: {
              item: {
                type: InternalSerializerType.FURY_TYPE_TAG,
                asObject: {
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
    const serializer = fury.registerSerializerByDescription(description);
    const input = fury.marshal({ a: [{ b: "hel" }] }, serializer);
    const result = fury.unmarshal(
      input
    );
    expect(result).toEqual({ a: [{ b: "hel" }] })
  });

  test('should write tag and read tag work', () => {
    const description: TypeDescription = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        props: {
          a: {
            type: InternalSerializerType.FURY_TYPE_TAG as const,
            asObject: {
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
            asObject: {
              tag: "example.bar",
            }
          }
        },
        tag: "example.foo"
      }
    };
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serializer = fury.registerSerializerByDescription(description);
    const input = fury.marshal({ a: { b: "hel" }, a2: { b: "hel2" } }, serializer);
    const result = fury.unmarshal(
      input
    );
    expect(result).toEqual({ a: { b: "hel" }, a2: { b: "hel2" } })
  });

  test('should ciycle ref work', () => {
    const description: TypeDescription = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      asObject: {
        props: {
          a: {
            type: InternalSerializerType.FURY_TYPE_TAG as const,
            asObject: {
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
            asObject: {
              tag: "example.foo",
            }
          }
        },
        tag: "example.foo"
      }
    };
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const serialize = fury.registerSerializerByDescription(description);
    const param: any = {};
    param.a = {b: "hel"};
    param.a2 = param;
    const input = fury.marshal(param, serialize);
    const result = fury.unmarshal(
      input
    );
    expect(result.a).toEqual({ b: "hel" })
    expect(result.a2).toEqual(result)
  });
});


