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

import { describe, expect, test } from '@jest/globals';
import { tupleObjectDescription, tupleObjectType3Description } from './fixtures/tuple';
import { createFuncFromDescription } from '../packages/fury/lib/codeGen';
import FuryInternal from '../packages/fury/lib/fury';

describe('codeGen', () => {
  test('can generate tuple declaration code', () => {
    const fury = FuryInternal({ refTracking: true });
    const fn = createFuncFromDescription(fury, tupleObjectDescription);
    expect(fn.toString()).toMatchSnapshot();

    const fn2 = createFuncFromDescription(fury, tupleObjectType3Description);
    expect(fn2.toString()).toMatchSnapshot();
  })
})
