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

import Fury, { TypeDescription, InternalSerializerType, ObjectTypeDescription, Type } from '../packages/fury/index';
import { describe, expect, test } from '@jest/globals';
import * as beautify from 'js-beautify';

describe('array', () => {
  test('should array work', () => {
    

    const description = Type.object("example.bar", {
      c: Type.array(Type.object("example.foo", {
        a: Type.string()
      }))
    });
    const fury = new Fury({ refTracking: true, hooks: {
      afterCodeGenerated: (code: string) => {
        return beautify.js(code, { indent_size: 2, space_in_empty_paren: true, indent_empty_lines: true });
      }
    } });
    const { serialize, deserialize } = fury.registerSerializer(description);
    const o = { a: "123" };
    expect(deserialize(serialize({ c: [o, o] }))).toEqual({ c: [o, o] })
  });
  test('should typedarray work', () => {
    const description = {
      type: InternalSerializerType.OBJECT,
      options: {
        props: {
          a: {
            type: InternalSerializerType.BOOL_ARRAY,
          },
          a2: {
            type: InternalSerializerType.INT16_ARRAY,
          },
          a3: {
            type: InternalSerializerType.INT32_ARRAY,
          },
          a4: {
            type: InternalSerializerType.INT64_ARRAY,
          },
          a6: {
            type: InternalSerializerType.FLOAT64_ARRAY,
          },
        },
        tag: "example.foo"
      }
    };
    
    const fury = new Fury({ refTracking: true }); const serializer = fury.registerSerializer(description).serializer;
    const input = fury.serialize({
      a: [true, false],
      a2: [1, 2, 3],
      a3: [3, 5, 76],
      a4: [634, 564, 76],
      a6: [234243.555, 55654.6786],
    }, serializer);
    const result = fury.deserialize(
      input
    );
    result.a4 = result.a4.map(x => Number(x));
    expect(result).toEqual({
      a: [true, false],
      a2: [1, 2, 3],
      a3: [3, 5, 76],
      a4: [634, 564, 76],
      a6: [234243.555, 55654.6786],
    })
  });


  test('should floatarray work', () => {
    const description: ObjectTypeDescription = {
      type: InternalSerializerType.OBJECT,
      options: {
        props: {
          a5: {
            type: InternalSerializerType.FLOAT32_ARRAY,
          },
        },
        tag: "example.foo"
      }
    };
    
    const fury = new Fury({ refTracking: true }); const serialize = fury.registerSerializer(description).serializer;
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


