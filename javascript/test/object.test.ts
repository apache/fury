import Fury from '../index';
import { describe, expect, test } from '@jest/globals';
import { TypeDefinition } from '../lib/codeGen';
import { InternalSerializerType } from '../lib/type';

describe('object', () => {
  test('should object work', () => {
    const definition: TypeDefinition = {
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
    const fury = new Fury();
    fury.registerSerializerByDefinition(definition);
    const input = fury.marshal({ a: { b: "hel" } }, "example.foo");
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: { b: "hel" } })
  });

  test('should object in array work', () => {
    const definition: TypeDefinition = {
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
    const fury = new Fury();
    fury.registerSerializerByDefinition(definition);
    const input = fury.marshal({ a: [{ b: "hel" }] }, "example.foo");
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: [{ b: "hel" }] })
  });

  test('should write tag and read tag work', () => {
    const definition: TypeDefinition = {
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
    const fury = new Fury();
    fury.registerSerializerByDefinition(definition);
    const input = fury.marshal({ a: { b: "hel" }, a2: { b: "hel2" } }, "example.foo");
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result).toEqual({ a: { b: "hel" }, a2: { b: "hel2" } })
  });

  test('should ciycle ref work', () => {
    const definition: TypeDefinition = {
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
    const fury = new Fury();
    fury.registerSerializerByDefinition(definition);
    const param: any = {};
    param.a = {b: "hel"};
    param.a2 = param;
    const input = fury.marshal(param, "example.foo");
    const result = fury.unmarshal(
      new Uint8Array(input)
    );
    expect(result.a).toEqual({ b: "hel" })
    expect(result.a2).toEqual(result)
  });
});


