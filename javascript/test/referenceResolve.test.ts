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

import { fromUint8Array, alloc, BrowserBuffer } from '../packages/fory/lib/platformBuffer';
import { describe, expect, test } from '@jest/globals';
import Fory, { TypeInfo, InternalSerializerType } from '../packages/fory/index';
import { RefFlags } from '../packages/fory/lib/type';
import { BinaryWriter } from '../packages/fory/lib/writer';
import { BinaryReader } from '../packages/fory/lib/reader';
import SerializerResolver from '../packages/fory/lib/classResolver';
import { makeHead } from '../packages/fory/lib/gen/serializer';

describe('referenceResolve', () => {
    test('should write head work', () => {
        const fory = new Fory();
        const bin = fory.serialize(7);
        expect(fory.deserialize(bin)).toBe(7)
    });

    test('should make head work when flag positive', () => {
        const head = makeHead(RefFlags.NotNullValueFlag, InternalSerializerType.STRING);
        const writer = new BinaryWriter({});
        writer.int24(head);
        const ab = writer.dump();
        const reader = new BinaryReader({});
        reader.reset(ab);
        expect(reader.int8()).toBe(RefFlags.NotNullValueFlag);
    });

    test('should make head work when flag is zero', () => {
        const head = makeHead(RefFlags.RefValueFlag, InternalSerializerType.STRING);
        const writer = new BinaryWriter({});
        writer.int24(head);
        const ab = writer.dump();
        const reader = new BinaryReader({});
        reader.reset(ab);
        expect(reader.int8()).toBe(RefFlags.RefValueFlag);
    });
});


