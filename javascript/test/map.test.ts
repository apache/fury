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

import Fury, { TypeDescription, InternalSerializerType } from '@furyjs/fury';
import {describe, expect, test} from '@jest/globals';

describe('map', () => {
  test('should map work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const input = fury.serialize(new Map([["foo", "bar"], ["foo2", "bar2"]]));
    const result = fury.deserialize(
        input
    );
    expect(result).toEqual(new Map([["foo", "bar"],["foo2", "bar2"]]))
  });
});


