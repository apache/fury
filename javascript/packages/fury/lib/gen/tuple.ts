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

import { TupleTypeDescription, TypeDescription } from "../description";
import { CodecBuilder } from "./builder";
import { BaseSerializerGenerator, RefState } from "./serializer";
import { CodegenRegistry } from "./router";
import { InternalSerializerType } from "../type";
import { Scope } from "./scope";

class TupleSerializerGenerator extends BaseSerializerGenerator {
  description: TupleTypeDescription;

  constructor(description: TypeDescription, builder: CodecBuilder, scope: Scope) {
    super(description, builder, scope);
    this.description = <TupleTypeDescription>description;
  }

  private innerMeta() {
    const inner = this.description.options.inner;
    return inner.map(x => this.builder.meta(x));
  }

  private innerGenerator() {
    const inner = this.description.options.inner;
    return inner.map((x) => {
      const InnerGeneratorClass = CodegenRegistry.get(x.type);
      if (!InnerGeneratorClass) {
        throw new Error(`${x.type} generator not exists`);
      }
      return new InnerGeneratorClass(x, this.builder, this.scope);
    });
  }

  writeStmt(accessor: string): string {
    const innerMeta = this.innerMeta();
    const innerGenerator = this.innerGenerator();
    const fixedSize = innerMeta.reduce((x, y) => x + y.fixedSize, 0);

    return `
            ${this.builder.writer.varUInt32(innerMeta.length)}
            ${this.builder.writer.reserve(fixedSize)};
            ${
                innerGenerator.map((generator, index) => {
                    return generator.toWriteEmbed(`${accessor}[${index}]`);
                }).join("\n")
            }
        `;
  }

  readStmt(accessor: (expr: string) => string, refState: RefState): string {
    const innerGenerator = this.innerGenerator();
    const result = this.scope.uniqueName("result");
    const len = this.scope.uniqueName("len");

    return `
            const ${len} = ${this.builder.reader.varUInt32()};
            const ${result} = new Array(${len});
            ${this.maybeReference(result, refState)}
            ${
                innerGenerator.map((generator, index) => {
                    return `
                    if (${len} > ${index}) {
                        ${generator.toReadEmbed(expr => `${result}[${index}] = ${expr}`)}
                    }
                    `;
                }).join("\n")
            }
            ${accessor(result)}
         `;
  }
}

CodegenRegistry.register(InternalSerializerType.TUPLE, TupleSerializerGenerator);
