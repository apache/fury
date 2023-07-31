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

import Fury, { TypeDescription, InternalSerializerType, Type } from '@furyjs/fury';
import { describe, expect, test } from '@jest/globals';


describe('protocol', () => {
    test('should py bin work', () => {
        const hps = process.env.enableHps ? require('@furyjs/hps') : null;
        const fury = new Fury({ hps });
        const { deserialize } = fury.registerSerializer({
            type: InternalSerializerType.FURY_TYPE_TAG,
            options: {
                tag: "example.ComplexObject",
                props: {
                    f1: Type.string(),
                    f2: Type.map(),
                    f3: Type.int8(),
                    f4: Type.int16(),
                    f5: Type.int32(),
                    f6: Type.int64(),
                    f7: Type.float(),
                    f8: Type.double(),
                    f9: Type.array(Type.int16()),
                    f10: Type.map(),
                }
            }
        });

        const obj = deserialize(Buffer.from([
            134, 2, 179, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 81, 159, 160, 124, 69, 240, 2, 120, 21, 0,
            101, 120, 97, 109, 112, 108, 101, 46, 67, 111, 109, 112, 108, 101, 120, 79, 98, 106, 101,
            99, 116, 71, 168, 32, 21, 0, 13, 0, 3, 115, 116, 114, 0, 30, 0, 2, 255, 7, 0, 1, 0, 0, 0,
            255, 12, 0, 85, 85, 85, 85, 85, 85, 213, 63, 255, 7, 0, 100, 0, 0, 0, 255, 12, 0, 146, 36,
            73, 146, 36, 73, 210, 63, 0, 30, 0, 2, 0, 13, 0, 2, 107, 49, 255, 3, 0, 255, 0, 13, 0, 2,
            107, 50, 255, 3, 0, 2, 255, 3, 0, 127, 255, 5, 0, 255, 127, 255, 7, 0, 255, 255, 255, 127,
            255, 9, 0, 255, 255, 255, 255, 255, 255, 255, 127, 255, 11, 0, 0, 0, 0, 63, 255, 12, 0, 85,
            85, 85, 85, 85, 85, 229, 63, 0, 25, 0, 2, 255, 5, 0, 1, 0, 255, 5, 0, 2, 0, 134, 2, 98, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 81, 159, 160, 124, 69, 240, 2, 120, 21, 0, 101, 120, 97, 109,
            112, 108, 101, 46, 67, 111, 109, 112, 108, 101, 120, 79, 98, 106, 101, 99, 116, 71, 168,
            32, 21, 253, 253, 253, 255, 3, 0, 0, 255, 5, 0, 0, 0, 255, 7, 0, 0, 0, 0, 0, 255, 9, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 255, 11, 0, 171, 170, 170, 62, 255, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            253,
        ]));

        console.log(obj);
    });
});


