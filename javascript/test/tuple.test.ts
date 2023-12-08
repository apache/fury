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

import Fury, { TypeDescription, InternalSerializerType, Type } from '../packages/fury/index';
import { describe, expect, test } from '@jest/globals';

describe('tuple', () => {
  test('should tuple work', () => {
    const description =  Type.object('aasdasd', {
      tuple: Type.tuple( [
        Type.object('example.foo.1',{
          a: Type.object('example.foo.1.1',{
            b: Type.string()
          })
        }),
        Type.object('example.foo.2',{
          a: Type.object('example.foo.2.1',{
            c: Type.string()
          })
        })
      ])
    });

    const fury = new Fury({ refTracking: true });
    const { serialize, deserialize } = fury.registerSerializer(description);
    const tuple = [{a: {b:'1'}}, {a: {c: '2'}}] as [{a: {b: string}}, {a: {c: string}}];
    const raw = {
      tuple,
    };
 
    const input = serialize(raw);
    const result = deserialize(
      input
    );
    expect(result).toEqual(raw)
  });
})
