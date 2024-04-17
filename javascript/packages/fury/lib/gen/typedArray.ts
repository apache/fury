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

import { Type, TypeDescription } from "../description";
import { CodecBuilder } from "./builder";
import { BaseSerializerGenerator, RefState } from "./serializer";
import { CodegenRegistry } from "./router";
import { InternalSerializerType } from "../type";
import { Scope } from "./scope";

function build(inner: TypeDescription) {
  return class TypedArraySerializerGenerator extends BaseSerializerGenerator {
    description: TypeDescription;

    constructor(description: TypeDescription, builder: CodecBuilder, scope: Scope) {
      super(description, builder, scope);
      this.description = <TypeDescription>description;
    }

    private innerMeta() {
      return this.builder.meta(inner);
    }

    private innerGenerator() {
      const InnerGeneratorClass = CodegenRegistry.get(inner.type);
      if (!InnerGeneratorClass) {
        throw new Error(`${InternalSerializerType[inner.type]} generator not exists`);
      }
      return new InnerGeneratorClass(inner, this.builder, this.scope);
    }

    writeStmt(accessor: string): string {
      const innerMeta = this.innerMeta();
      const innerGenerator = this.innerGenerator();
      const item = this.scope.uniqueName("item");
      return `
                ${this.builder.writer.varUInt32(`${accessor}.length`)}
                ${this.builder.writer.reserve(`${innerMeta.fixedSize} * ${accessor}.length`)};
                for (const ${item} of ${accessor}) {
                    ${innerGenerator.toWriteEmbed(item, true)}
                }
            `;
    }

    readStmt(accessor: (expr: string) => string, refState: RefState): string {
      const innerGenerator = this.innerGenerator();
      const result = this.scope.uniqueName("result");
      const len = this.scope.uniqueName("len");
      const idx = this.scope.uniqueName("idx");

      return `
                const ${len} = ${this.builder.reader.varUInt32()};
                const ${result} = new Array(${len});
                ${this.maybeReference(result, refState)}
                for (let ${idx} = 0; ${idx} < ${len}; ${idx}++) {
                    ${innerGenerator.toReadEmbed(x => `${result}[${idx}] = ${x};`, true, RefState.fromFalse())}
                }
                ${accessor(result)}
             `;
    }
  };
}

CodegenRegistry.register(InternalSerializerType.BOOL_ARRAY, build(Type.bool()));
CodegenRegistry.register(InternalSerializerType.INT8_ARRAY, build(Type.int8()));
CodegenRegistry.register(InternalSerializerType.INT16_ARRAY, build(Type.int16()));
CodegenRegistry.register(InternalSerializerType.INT32_ARRAY, build(Type.int32()));
CodegenRegistry.register(InternalSerializerType.INT64_ARRAY, build(Type.int64()));
CodegenRegistry.register(InternalSerializerType.FLOAT16_ARRAY, build(Type.float16()));
CodegenRegistry.register(InternalSerializerType.FLOAT32_ARRAY, build(Type.float32()));
CodegenRegistry.register(InternalSerializerType.FLOAT64_ARRAY, build(Type.float64()));
