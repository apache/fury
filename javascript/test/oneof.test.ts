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

const oneOfThree = Type.oneof({
    option1: Type.string(),
    option2: Type.object("foo", {
        a: Type.int32()
    }),
    option3: Type.int32(),
});

describe('oneof', () => {
    test('option1: should oneof work1', () => {
        const fury = new Fury({ refTracking: true });
        const { serialize, deserialize } = fury.registerSerializer(oneOfThree);
        const obj = {
            option1: "hello"
        }
        const input = serialize(obj);
        const result = deserialize(
            input
        );
        expect(result).toEqual(obj.option1)
    });
    test('option2: should oneof work', () => {
        const fury = new Fury({ refTracking: true });
        const { serialize, deserialize } = fury.registerSerializer(oneOfThree);
        const obj = {
            option2: {
                a: 123
            }
        }
        const input = serialize(obj);
        const result = deserialize(
            input
        );
        expect(result).toEqual(obj.option2)
    });
    test('option3: should oneof work', () => {
        const fury = new Fury({ refTracking: true });
        const { serialize, deserialize } = fury.registerSerializer(oneOfThree);
        const obj = {
            option3: 123
        }
        const input = serialize(obj);
        const result = deserialize(
            input
        );
        expect(result).toEqual(obj.option3)
    });
    test('should nested oneof work1', () => {
        const fury = new Fury({ refTracking: true });
        const { serialize, deserialize } = fury.registerSerializer(Type.object("foo2", {
            f: oneOfThree
        }));
        const obj = {
            option1: "hello"
        }
        const input = serialize({
            f: obj
        });
        const result = deserialize(
            input
        );
        expect(result).toEqual({
            f: obj.option1
        })
    });
});


