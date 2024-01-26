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

import { OneofTypeDescription, Type, TypeDescription } from "../description";
import { CodecBuilder } from "./builder";
import { BaseSerializerGenerator } from "./serializer";
import { CodegenRegistry } from "./router";
import { InternalSerializerType, RefFlags } from "../type";
import { Scope } from "./scope";

class OneofSerializerGenerator extends BaseSerializerGenerator {
  description: OneofTypeDescription;

  constructor(description: TypeDescription, builder: CodecBuilder, scope: Scope) {
    super(description, builder, scope);
    this.description = <OneofTypeDescription>description;
  }

  writeStmt(): string {
    throw new Error("Type oneof writeStmt can't inline");
  }

  readStmt(): string {
    throw new Error("Type oneof readStmt can't inline");
  }

  toWriteEmbed(accessor: string, excludeHead = false) {
    if (excludeHead) {
      throw new Error("Oneof can't excludeHead");
    }
    if (Object.values(this.description.options.inner).length < 1) {
      throw new Error("Type oneof must contain at least one field");
    }
    const stmts = [
            `if (${accessor} === null || ${accessor} === undefined) {
                ${this.builder.writer.int8(RefFlags.NullFlag)};
            }`,
    ];
    Object.entries(this.description.options.inner).forEach(([key, value]) => {
      const InnerGeneratorClass = CodegenRegistry.get(value.type);
      if (!InnerGeneratorClass) {
        throw new Error(`${value.type} generator not exists`);
      }
      const innerGenerator = new InnerGeneratorClass(value, this.builder, this.scope);
      stmts.push(` if (${CodecBuilder.safeString(key)} in ${accessor}) {
                ${innerGenerator.toWriteEmbed(`${accessor}${CodecBuilder.safePropAccessor(key)}`)}
            }`);
    });
    stmts.push(`
            {
                ${this.builder.writer.int8(RefFlags.NullFlag)};
            }
        `);
    return stmts.join("else");
  }

  toReadEmbed(accessor: (expr: string) => string, excludeHead = false): string {
    const AnyGeneratorClass = CodegenRegistry.get(InternalSerializerType.ANY)!;
    const anyGenerator = new AnyGeneratorClass(Type.any(), this.builder, this.scope);
    return anyGenerator.toReadEmbed(accessor, excludeHead);
  }

  toSerializer() {
    this.scope.assertNameNotDuplicate("read");
    this.scope.assertNameNotDuplicate("readInner");
    this.scope.assertNameNotDuplicate("write");
    this.scope.assertNameNotDuplicate("writeInner");

    const declare = `
          const readInner = (fromRef) => {
            throw new Error("Type oneof readInner can't call directly");
          };
          const read = () => {
            ${this.toReadEmbed(expr => `return ${expr}`)}
          };
          const writeInner = (v) => {
            throw new Error("Type oneof writeInner can't call directly");
          };
          const write = (v) => {
            ${this.toWriteEmbed("v")}
          };
        `;
    return `
            return function (fury, external) {
                ${this.scope.generate()}
                ${declare}
                return {
                  read,
                  readInner,
                  write,
                  writeInner,
                  meta: ${JSON.stringify(this.builder.meta(this.description))}
                };
            }
            `;
  }
}

CodegenRegistry.register(InternalSerializerType.ONEOF, OneofSerializerGenerator);
