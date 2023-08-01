/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Fury, { TypeDescription, InternalSerializerType, ObjectTypeDescription, Type } from '@furyjs/fury';
import { describe, expect, test } from '@jest/globals';

describe('array', () => {
  test('should array work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;

    const description = Type.object("example.bar", {
      c: Type.array(Type.object("example.foo", {
        a: Type.string()
      }))
    });
    const fury = new Fury({ refTracking: true, hps });
    const { serialize, deserialize } = fury.registerSerializer(description);
    const o = { a: "123" };
    expect(deserialize(serialize({ c: [o, o] }))).toEqual({ c: [o, o] })
  });
  test('should typedarray work', () => {
    const description = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
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
    const fury = new Fury({ refTracking: true, hps }); const serializer = fury.registerSerializer(description).serializer;
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
    const description: ObjectTypeDescription = {
      type: InternalSerializerType.FURY_TYPE_TAG,
      options: {
        props: {
          a5: {
            type: InternalSerializerType.FURY_PRIMITIVE_FLOAT_ARRAY,
          },
        },
        tag: "example.foo"
      }
    };
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ refTracking: true, hps }); const serialize = fury.registerSerializer(description).serializer;
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


