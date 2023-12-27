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

import { isLatin1, stringCopy } from '../packages/hps/index';
import { describe, expect, test } from '@jest/globals';

describe('hps', () => {
    test('should isLatin1 work', () => {
        for (let index = 0; index < 10000; index++) {
            var is = isLatin1("hello");
            expect(is).toBe(true)

            var is = isLatin1("😁");
            expect(is).toBe(false)
        }

    });

    test('should stringCopy work', () => {
        for (let index = 0; index < 10000; index++) {
            const dist = new Uint8Array(5);
            stringCopy("hello", dist, 0);
            expect([...dist]).toEqual([104, 101, 108, 108, 111])
        }
    });
});


