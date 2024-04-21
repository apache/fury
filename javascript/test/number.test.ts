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

import Fury, { TypeDescription, InternalSerializerType, Type } from '../packages/fury/index';
import { describe, expect, test } from '@jest/globals';

describe('number', () => {
  test('should i8 work', () => {
    
    const fury = new Fury({ refTracking: true });    
    const serialize = fury.registerSerializer({
      type: InternalSerializerType.OBJECT,
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
    
    const fury = new Fury({ refTracking: true });    
    const serialize = fury.registerSerializer({
      type: InternalSerializerType.OBJECT,
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
    
    const fury = new Fury({ refTracking: true });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.OBJECT,
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
    
    const fury = new Fury({ refTracking: true });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.OBJECT,
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
    result.a = Number(result.a)
    expect(result).toEqual({ a: 1 })
  });

  test('should float work', () => {
    
    const fury = new Fury({ refTracking: true });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.OBJECT,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.FLOAT32
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
  test('should float64 work', () => {
    
    const fury = new Fury({ refTracking: true });    
    const serializer = fury.registerSerializer({
      type: InternalSerializerType.OBJECT,
      options: {
        tag: "example.foo",
        props: {
          a: {
            type: InternalSerializerType.FLOAT64
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


