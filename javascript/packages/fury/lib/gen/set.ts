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
import { BaseSerializerGenerator } from "./serializer";
import { CodegenRegistry } from "./router";
import { InternalSerializerType } from "../type";
import { Scope } from "./scope";

class SetSerializerGenerator extends BaseSerializerGenerator {
  description: SetTypeDescription;

  constructor(description: TypeDescription, builder: CodecBuilder, scope: Scope) {
    super(description, builder, scope);
    this.description = <SetTypeDescription>description;
  }

  private innerMeta() {
    const inner = this.description.options.key;
    return this.builder.meta(inner);
  }

  private innerGenerator() {
    const inner = this.description.options.key;
    const InnerGeneratorClass = CodegenRegistry.get(inner.type);
    if (!InnerGeneratorClass) {
      throw new Error(`${inner.type} generator not exists`);
    }
    return new InnerGeneratorClass(inner, this.builder, this.scope);
  }

  writeStmt(accessor: string): string {
    const innerMeta = this.innerMeta();
    const innerGenerator = this.innerGenerator();
    const item = this.scope.uniqueName("item");
    return `
            ${this.builder.writer.varUInt32(`${accessor}.size`)}
            ${this.builder.writer.reserve(`${innerMeta.fixedSize} * ${accessor}.size`)};
            for (const ${item} of ${accessor}.values()) {
                ${innerGenerator.toWriteEmbed(item)}
            }
        `;
  }

  readStmt(accessor: (expr: string) => string): string {
    const innerGenerator = this.innerGenerator();
    const result = this.scope.uniqueName("result");
    const idx = this.scope.uniqueName("idx");
    const len = this.scope.uniqueName("len");

    return `
            const ${result} = new Set();
            ${this.pushReadRefStmt(result)}
            const ${len} = ${this.builder.reader.varUInt32()};
            for (let ${idx} = 0; ${idx} < ${len}; ${idx}++) {
                ${innerGenerator.toReadEmbed(x => `${result}.add(${x});`)}
            }
            ${accessor(result)}
         `;
  }
}

CodegenRegistry.register(InternalSerializerType.FURY_SET, SetSerializerGenerator);
