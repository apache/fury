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

import { TypeDescription } from "../description";
import { Builder } from "./builder";
import { BaseSerializerGenerator } from "./serializer";
import { Register } from "./router";
import { InternalSerializerType } from "../type";
import { Scope } from "./scope";

class AnySerializerGenerator extends BaseSerializerGenerator {
  description: TypeDescription;

  constructor(description: TypeDescription, builder: Builder, scope: Scope) {
    super(description, builder, scope);
    this.description = description;
  }

  private addDep() {
    return this.scope.declare(
      "any_ser",
      this.builder.classResolver.getSerializerById(InternalSerializerType.ANY)
    );
  }

  writeStmt(accessor: string): string {
    const name = this.addDep();
    return `${name}.writeInner(${accessor})`;
  }

  readStmt(accessor: (expr: string) => string): string {
    const name = this.addDep();
    return `${name}.readInner(${accessor})`;
  }

  toReadEmbed(accessor: (expr: string) => string): string {
    const name = this.addDep();
    return accessor(`${name}.read()`);
  }

  toWriteEmbed(accessor: string): string {
    const name = this.addDep();
    return `${name}.write(${accessor})`;
  }
}

Register.reg(InternalSerializerType.ANY, AnySerializerGenerator);
