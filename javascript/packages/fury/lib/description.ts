/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { InternalSerializerType } from "./type";

export interface TypeDescription {
  type: InternalSerializerType;
  label?: string;
}

export interface ObjectTypeDescription extends TypeDescription {
  options: {
    props: { [key: string]: TypeDescription };
    tag: string;
  };
}

export interface EnumTypeDescription extends TypeDescription {
  options: {
    inner: { [key: string]: any };
  };
}

export interface OneofTypeDescription extends TypeDescription {
  options: {
    inner: { [key: string]: TypeDescription };
  };
}

export interface ArrayTypeDescription extends TypeDescription {
  options: {
    inner: TypeDescription;
  };
}

export interface TupleTypeDescription extends TypeDescription {
  options: {
    inner: TypeDescription[];
  };
}

export interface SetTypeDescription extends TypeDescription {
  options: {
    key: TypeDescription;
  };
}

export interface MapTypeDescription extends TypeDescription {
  options: {
    key: TypeDescription;
    value: TypeDescription;
  };
}

type Props<T> = T extends {
  options: {
    props?: infer T2 extends { [key: string]: any };
    tag: string;
  };
}
  ? {
      [P in keyof T2]?: (InputType<T2[P]> | null);
    }
  : unknown;

type InnerProps<T> = T extends {
  options: {
    inner: infer T2 extends TypeDescription;
  };
}
  ? (InputType<T2> | null)[]
  : unknown;

type MapProps<T> = T extends {
  options: {
    key: infer T2 extends TypeDescription;
    value: infer T3 extends TypeDescription;
  };
}
  ? Map<InputType<T2>, InputType<T3> | null>
  : unknown;

type TupleProps<T> = T extends {
  options: {
    inner: infer T2 extends readonly [...TypeDescription[]];
  };
}
  ? { [K in keyof T2]: InputType<T2[K]> }
  : unknown;

type Value<T> = T extends { [s: string]: infer T2 } ? T2 : unknown;

type EnumProps<T> = T extends {
  options: {
    inner: infer T2;
  };
}
  ? Value<T2>
  : unknown;

type OneofProps<T> = T extends {
  options: {
    inner?: infer T2 extends { [key: string]: any };
  };
}
  ? {
      [P in keyof T2]?: (InputType<T2[P]> | null);
    }
  : unknown;

type OneofResult<T> = T extends {
  options: {
    inner?: infer T2;
  };
}
  ? ResultType<Value<T2>>
  : unknown;

type SetProps<T> = T extends {
  options: {
    key: infer T2 extends TypeDescription;
  };
}
  ? Set<(InputType<T2> | null)>
  : unknown;

export type InputType<T> = T extends {
  type: InternalSerializerType.OBJECT;
}
  ? Props<T>
  : T extends {
    type: InternalSerializerType.STRING;
  }
    ? string
    : T extends {
      type: InternalSerializerType.TUPLE;
    }
      ? TupleProps<T>
      : T extends {
        type:
          | InternalSerializerType.INT8
          | InternalSerializerType.INT16
          | InternalSerializerType.INT32
          | InternalSerializerType.VAR_INT32
          | InternalSerializerType.FLOAT16
          | InternalSerializerType.FLOAT32
          | InternalSerializerType.FLOAT64;
      }
        ? number

        : T extends {
          type: InternalSerializerType.VAR_INT64
            | InternalSerializerType.SLI_INT64
            | InternalSerializerType.INT64;
        }
          ? bigint
          : T extends {
            type: InternalSerializerType.MAP;
          }
            ? MapProps<T>
            : T extends {
              type: InternalSerializerType.SET;
            }
              ? SetProps<T>
              : T extends {
                type: InternalSerializerType.ARRAY;
              }
                ? InnerProps<T>
                : T extends {
                  type: InternalSerializerType.BOOL;
                }
                  ? boolean
                  : T extends {
                    type: InternalSerializerType.DURATION;
                  }
                    ? Date
                    : T extends {
                      type: InternalSerializerType.TIMESTAMP;
                    }
                      ? number
                      : T extends {
                        type: InternalSerializerType.ANY;
                      }
                        ? any
                        : T extends { type: InternalSerializerType.BINARY;
                        }
                          ? Uint8Array
                          : T extends {
                            type: InternalSerializerType.ENUM;
                          }
                            ? EnumProps<T> : T extends {
                              type: InternalSerializerType.ONEOF;
                            } ? OneofProps<T> : unknown;

export type ResultType<T> = T extends {
  type: InternalSerializerType.OBJECT;
}
  ? Props<T>
  : T extends {
    type: InternalSerializerType.STRING;
  }
    ? string
    : T extends {
      type: InternalSerializerType.TUPLE;
    }
      ? TupleProps<T>
      : T extends {
        type:
          | InternalSerializerType.INT8
          | InternalSerializerType.INT16
          | InternalSerializerType.INT32
          | InternalSerializerType.VAR_INT32
          | InternalSerializerType.FLOAT16
          | InternalSerializerType.FLOAT32
          | InternalSerializerType.FLOAT64;
      }
        ? number

        : T extends {
          type: InternalSerializerType.SLI_INT64
            | InternalSerializerType.INT64;
        }
          ? bigint
          : T extends {
            type: InternalSerializerType.MAP;
          }
            ? MapProps<T>
            : T extends {
              type: InternalSerializerType.SET;
            }
              ? SetProps<T>
              : T extends {
                type: InternalSerializerType.ARRAY;
              }
                ? InnerProps<T>
                : T extends {
                  type: InternalSerializerType.BOOL;
                }
                  ? boolean
                  : T extends {
                    type: InternalSerializerType.DURATION;
                  }
                    ? Date
                    : T extends {
                      type: InternalSerializerType.TIMESTAMP;
                    }
                      ? number
                      : T extends { type: InternalSerializerType.BINARY;
                      }
                        ? Uint8Array : T extends {
                          type: InternalSerializerType.ANY;
                        }
                          ? any
                          : T extends {
                            type: InternalSerializerType.ENUM;
                          }
                            ? EnumProps<T> : T extends {
                              type: InternalSerializerType.ONEOF;
                            } ? OneofResult<T> : unknown;

export const Type = {
  any() {
    return {
      type: InternalSerializerType.ANY as const,
    };
  },
  enum<T1 extends { [key: string]: any }>(t1: T1) {
    return {
      type: InternalSerializerType.ENUM as const,
      options: {
        inner: t1,
      },
    };
  },
  oneof<T extends { [key: string]: TypeDescription }>(inner?: T) {
    return {
      type: InternalSerializerType.ONEOF as const,
      options: {
        inner,
      },
    };
  },
  string() {
    return {
      type: InternalSerializerType.STRING as const,
    };
  },
  array<T extends TypeDescription>(def: T) {
    return {
      type: InternalSerializerType.ARRAY as const,
      options: {
        inner: def,
      },
    };
  },
  tuple<T1 extends readonly [...readonly TypeDescription[]]>(t1: T1) {
    return {
      type: InternalSerializerType.TUPLE as const,
      options: {
        inner: t1,
      },
    };
  },
  map<T1 extends TypeDescription, T2 extends TypeDescription>(
    key: T1,
    value: T2
  ) {
    return {
      type: InternalSerializerType.MAP as const,
      options: {
        key,
        value,
      },
    };
  },
  set<T extends TypeDescription>(key: T) {
    return {
      type: InternalSerializerType.SET as const,
      options: {
        key,
      },
    };
  },
  bool() {
    return {
      type: InternalSerializerType.BOOL as const,
    };
  },
  object<T extends { [key: string]: TypeDescription }>(tag: string, props?: T) {
    return {
      type: InternalSerializerType.OBJECT as const,
      options: {
        tag,
        props,
      },
    };
  },
  int8() {
    return {
      type: InternalSerializerType.INT8 as const,
    };
  },
  int16() {
    return {
      type: InternalSerializerType.INT16 as const,
    };
  },
  int32() {
    return {
      type: InternalSerializerType.INT32 as const,
    };
  },
  varInt32() {
    return {
      type: InternalSerializerType.VAR_INT32 as const,
    };
  },
  int64() {
    return {
      type: InternalSerializerType.INT64 as const,
    };
  },
  sliInt64() {
    return {
      type: InternalSerializerType.SLI_INT64 as const,
    };
  },
  float16() {
    return {
      type: InternalSerializerType.FLOAT16 as const,
    };
  },
  float32() {
    return {
      type: InternalSerializerType.FLOAT32 as const,
    };
  },
  float64() {
    return {
      type: InternalSerializerType.FLOAT64 as const,
    };
  },
  binary() {
    return {
      type: InternalSerializerType.BINARY as const,
    };
  },
  duration() {
    return {
      type: InternalSerializerType.DURATION as const,
    };
  },
  timestamp() {
    return {
      type: InternalSerializerType.TIMESTAMP as const,
    };
  },
  boolArray() {
    return {
      type: InternalSerializerType.BOOL_ARRAY as const,
    };
  },
  int8Array() {
    return {
      type: InternalSerializerType.INT8_ARRAY as const,
    };
  },
  int16Array() {
    return {
      type: InternalSerializerType.INT16_ARRAY as const,
    };
  },
  int32Array() {
    return {
      type: InternalSerializerType.INT32_ARRAY as const,
    };
  },
  int64Array() {
    return {
      type: InternalSerializerType.INT64_ARRAY as const,
    };
  },
  float16Array() {
    return {
      type: InternalSerializerType.FLOAT16_ARRAY as const,
    };
  },
  float32Array() {
    return {
      type: InternalSerializerType.FLOAT32_ARRAY as const,
    };
  },
  float64Array() {
    return {
      type: InternalSerializerType.FLOAT64_ARRAY as const,
    };
  },
};
