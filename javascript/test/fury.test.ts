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

import Fury, { TypeDescription, Type } from '../packages/fury/index';
import { describe, expect, test } from '@jest/globals';
import { fromUint8Array } from '../packages/fury/lib/platformBuffer';
import { MAGIC_NUMBER } from '../packages/fury/lib/type';

const hight = MAGIC_NUMBER >> 8;
const low = MAGIC_NUMBER & 0xff;

describe('fury', () => {
    test('should deserialize null work', () => {
        const fury = new Fury();

        expect(fury.deserialize(new Uint8Array([low, hight, 1]))).toBe(null)
    });

    test('should deserialize big endian work', () => {
        const fury = new Fury();
        try {
            fury.deserialize(new Uint8Array([low, hight, 0]))
            throw new Error('unreachable code')
        } catch (error) {
            expect(error.message).toBe('big endian is not supported now');
        }
    });

    test('should deserialize xlang disable work', () => {
        const fury = new Fury();
        try {
            fury.deserialize(new Uint8Array([low, hight, 2]))
            throw new Error('unreachable code')
        } catch (error) {
            expect(error.message).toBe('support crosslanguage mode only');
        }
    });

    test('should deserialize xlang disable work', () => {
        const fury = new Fury();
        try {
            fury.deserialize(new Uint8Array([low, hight, 14]))
            throw new Error('unreachable code')
        } catch (error) {
            expect(error.message).toBe('outofband mode is not supported now');
        }
    });

    test('should register work', () => {
        const fury = new Fury();
        const { serialize, deserialize } = fury.registerSerializer(Type.array(Type.string()));
        const bin = serialize(["hello", "world"]);
        expect(deserialize(bin)).toEqual(["hello", "world"]);
    });

    describe('serializer description should work', () => {
        test('can serialize and deserialize primitive types', () => {
            const description = Type.int8()
            testDescription(description, 123)

            const description2 = Type.int16()
            testDescription(description2, 123)

            const description3 = Type.int32()
            testDescription(description3, 123)

            const description4 = Type.bool()
            testDescription(description4, true)

            // has precision problem
            // const description5 = Type.float()
            // testDescription(description5, 123.456)

            const description6 = Type.float64()
            testDescription(description6, 123.456789)

            const description7 = Type.binary()
            testDescription(description7, new Uint8Array([1, 2, 3]), fromUint8Array(new Uint8Array([1, 2, 3])));

            const description8 = Type.string()
            testDescription(description8, '123')

            const description9 = Type.set(Type.string())
            testDescription(description9, new Set(['123']))
        })

        test('can serialize and deserialize array', () => {
            const description = Type.array(Type.int8())
            testDescription(description, [1, 2, 3])
            testDescription(description, [])
        })

        test('can serialize and deserialize tuple', () => {
            const description = Type.tuple([Type.int8(), Type.int16(), Type.timestamp()])
            testDescription(description, [1, 2, new Date()])
        })


        function testDescription(description: TypeDescription, input: any, expected?: any) {
            const fury = new Fury();
            const serialize = fury.registerSerializer(description);
            const result = serialize.deserialize(
                serialize.serialize(input)
            );
            expect(result).toEqual(expected ?? input)
        }
    })
});
