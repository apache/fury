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
  type: InternalSerializerType
  label?: string
}

export interface ObjectTypeDescription extends TypeDescription {
  options: {
    props: { [key: string]: TypeDescription }
    tag: string
  }
}

export interface ArrayTypeDescription extends TypeDescription {
  options: {
    inner: TypeDescription
  }
}

export interface TupleTypeDescription extends TypeDescription {
  options: {
    inner: TypeDescription[]
  }
}

export interface SetTypeDescription extends TypeDescription {
  options: {
    key: TypeDescription
  }
}

export interface MapTypeDescription extends TypeDescription {
  options: {
    key: TypeDescription
    value: TypeDescription
  }
}

type Props<T> = T extends {
  options: {
    props?: infer T2 extends { [key: string]: any }
    tag: string
  }
}
  ? {
      [P in keyof T2]?: (ToRecordType<T2[P]> | null);
    }
  : unknown;

type InnerProps<T> = T extends {
  options: {
    inner: infer T2 extends TypeDescription
  }
}
  ? (ToRecordType<T2> | null)[]
  : unknown;

type MapProps<T> = T extends {
  options: {
    key: infer T2 extends TypeDescription
    value: infer T3 extends TypeDescription
  }
}
  ? Map<ToRecordType<T2>, ToRecordType<T3> | null>
  : unknown;

type TupleProps<T> = T extends {
  options: {
    inner: infer T2 extends readonly [...TypeDescription[]]
  }
}
  ? { [K in keyof T2]: ToRecordType<T2[K]> }
  : unknown;

type SetProps<T> = T extends {
  options: {
    key: infer T2 extends TypeDescription
  }
}
  ? Set<(ToRecordType<T2> | null)>
  : unknown;

export type ToRecordType<T> = T extends {
  type: InternalSerializerType.FURY_TYPE_TAG
}
  ? Props<T>
  : T extends {
    type: InternalSerializerType.STRING
  }
    ? string
    : T extends {
      type: InternalSerializerType.TUPLE
    }
      ? TupleProps<T>
      : T extends {
        type:
          | InternalSerializerType.UINT8
          | InternalSerializerType.UINT16
          | InternalSerializerType.UINT32
          | InternalSerializerType.INT8
          | InternalSerializerType.INT16
          | InternalSerializerType.INT32
          | InternalSerializerType.FLOAT
          | InternalSerializerType.DOUBLE
      }
        ? number

        : T extends {
          type: InternalSerializerType.UINT64
            | InternalSerializerType.INT64
        }
          ? bigint
          : T extends {
            type: InternalSerializerType.MAP
          }
            ? MapProps<T>
            : T extends {
              type: InternalSerializerType.FURY_SET
            }
              ? SetProps<T>
              : T extends {
                type: InternalSerializerType.ARRAY
              }
                ? InnerProps<T>
                : T extends {
                  type: InternalSerializerType.BOOL
                }
                  ? boolean
                  : T extends {
                    type: InternalSerializerType.DATE
                  }
                    ? Date
                    : T extends {
                      type: InternalSerializerType.TIMESTAMP
                    }
                      ? number
                      : T extends {
                        type: InternalSerializerType.BINARY
                      }
                        ? Uint8Array
                        : T extends {
                          type: InternalSerializerType.ANY
                        }
                          ? any
                          : unknown;

export const Type = {
  any() {
    return {
      type: InternalSerializerType.ANY as const,
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
      type: InternalSerializerType.FURY_SET as const,
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
      type: InternalSerializerType.FURY_TYPE_TAG as const,
      options: {
        tag,
        props,
      },
    };
  },
  uint8() {
    return {
      type: InternalSerializerType.UINT8 as const,
    };
  },
  uint16() {
    return {
      type: InternalSerializerType.UINT16 as const,
    };
  },
  uint32() {
    return {
      type: InternalSerializerType.UINT32 as const,
    };
  },
  uint64() {
    return {
      type: InternalSerializerType.UINT64 as const,
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
  int64() {
    return {
      type: InternalSerializerType.INT64 as const,
    };
  },
  float() {
    return {
      type: InternalSerializerType.FLOAT as const,
    };
  },
  double() {
    return {
      type: InternalSerializerType.DOUBLE as const,
    };
  },
  binary() {
    return {
      type: InternalSerializerType.BINARY as const,
    };
  },
  date() {
    return {
      type: InternalSerializerType.DATE as const,
    };
  },
  timestamp() {
    return {
      type: InternalSerializerType.TIMESTAMP as const,
    };
  },
  stringTypedArray() {
    return {
      type: InternalSerializerType.FURY_STRING_ARRAY as const,
    };
  },
  boolTypedArray() {
    return {
      type: InternalSerializerType.FURY_PRIMITIVE_BOOL_ARRAY as const,
    };
  },
  shortTypedArray() {
    return {
      type: InternalSerializerType.FURY_PRIMITIVE_SHORT_ARRAY as const,
    };
  },
  intTypedArray() {
    return {
      type: InternalSerializerType.FURY_PRIMITIVE_INT_ARRAY as const,
    };
  },
  longTypedArray() {
    return {
      type: InternalSerializerType.FURY_PRIMITIVE_LONG_ARRAY as const,
    };
  },
  floatTypedArray() {
    return {
      type: InternalSerializerType.FURY_PRIMITIVE_FLOAT_ARRAY as const,
    };
  },
  doubleTypedArray() {
    return {
      type: InternalSerializerType.FURY_PRIMITIVE_DOUBLE_ARRAY as const,
    };
  },
};
