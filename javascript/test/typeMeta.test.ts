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

describe('TypeMeta', () => {
    test('should TypeMeta work', () => {
    const c: ObjectTypeDescription = Type.object("hello", {
      a: Type.string(),
      b: Type.int32(),
    });
    const fields = Object.entries(c.options.props).map(([key, value]) => {
      return new FieldInfo(key, value.type);
    });
    const binary = TypeMeta.from_fields(256, fields).to_bytes();
  });
});


