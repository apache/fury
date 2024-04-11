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

import { SetTypeDescription, TypeDescription } from "../description";
import { CodecBuilder } from "./builder";
import { CodegenRegistry } from "./router";
import { InternalSerializerType } from "../type";
import { Scope } from "./scope";
import { CollectionSerializerGenerator } from "./collection";

class SetSerializerGenerator extends CollectionSerializerGenerator {
  description: SetTypeDescription;

  constructor(description: TypeDescription, builder: CodecBuilder, scope: Scope) {
    super(description, builder, scope);
    this.description = <SetTypeDescription>description;
  }

  genericTypeDescriptin(): TypeDescription {
    return this.description.options.key;
  }

  newCollection(): string {
    return `new Set()`;
  }

  sizeProp() {
    return "size";
  }

  putAccessor(result: string, item: string): string {
    return `${result}.add(${item})`;
  }
}

CodegenRegistry.register(InternalSerializerType.SET, SetSerializerGenerator);
