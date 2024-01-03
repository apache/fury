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

import { MapTypeDescription, TypeDescription } from "../description";
import { Builder } from "./builder";
import { BaseSerializerGenerator } from "./serializer";
import { Register } from "./router";
import { InternalSerializerType } from "../type";
import { Scope } from "./scope";

class MapSerializerGenerator extends BaseSerializerGenerator {
  description: MapTypeDescription;

  constructor(description: TypeDescription, builder: Builder, scope: Scope) {
    super(description, builder, scope);
    this.description = <MapTypeDescription>description;
  }

  private innerMeta() {
    const key = this.description.options.key;
    const value = this.description.options.value;
    return [this.builder.meta(key), this.builder.meta(value)];
  }

  private innerGenerator() {
    const key = this.description.options.key;
    const value = this.description.options.value;

    const KeyGeneratorClass = Register.get(key.type);
    const ValueGeneratorClass = Register.get(value.type);
    if (!KeyGeneratorClass) {
      throw new Error(`${key.type} generator not exists`);
    }
    if (!ValueGeneratorClass) {
      throw new Error(`${value.type} generator not exists`);
    }
    return [new KeyGeneratorClass(key, this.builder, this.scope), new ValueGeneratorClass(value, this.builder, this.scope)];
  }

  writeStmt(accessor: string): string {
    const [keyMeta, valueMeta] = this.innerMeta();
    const [keyGenerator, valueGenerator] = this.innerGenerator();
    const key = this.scope.uniqueName("key");
    const value = this.scope.uniqueName("value");

    return `
            ${this.builder.writer.varUInt32(`${accessor}.size`)}
            ${this.builder.writer.reserve(`${keyMeta.fixedSize + valueMeta.fixedSize} * ${accessor}.size`)};
            for (const [${key}, ${value}] of ${accessor}.entries()) {
                ${keyGenerator.toWriteEmbed(key)}
                ${valueGenerator.toWriteEmbed(value)}
            }
        `;
  }

  readStmt(accessor: (expr: string) => string): string {
    const [keyGenerator, valueGenerator] = this.innerGenerator();
    const key = this.scope.uniqueName("key");
    const value = this.scope.uniqueName("value");

    const result = this.scope.uniqueName("result");
    const idx = this.scope.uniqueName("idx");
    const len = this.scope.uniqueName("len");

    return `
            const ${result} = new Map();
            ${this.pushReadRefStmt(result)};
            const ${len} = ${this.builder.reader.varUInt32()};
            for (let ${idx} = 0; ${idx} < ${len}; ${idx}++) {
                let ${key};
                let ${value};
                ${keyGenerator.toReadEmbed(x => `${key} = ${x};`)}
                ${valueGenerator.toReadEmbed(x => `${value} = ${x};`)}
                ${result}.set(${key}, ${value});
            }
            ${accessor(result)}
         `;
  }
}

Register.reg(InternalSerializerType.MAP, MapSerializerGenerator);
