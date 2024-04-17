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

import { InternalSerializerType, MaxInt32 } from "../type";
import { Scope } from "./scope";
import { CodecBuilder } from "./builder";
import { ObjectTypeDescription, TypeDescription } from "../description";
import { fromString } from "../platformBuffer";
import { CodegenRegistry } from "./router";
import { BaseSerializerGenerator, RefState } from "./serializer";
import SerializerResolver from "../classResolver";

function computeFieldHash(hash: number, id: number): number {
  let newHash = (hash) * 31 + (id);
  while (newHash >= MaxInt32) {
    newHash = Math.floor(newHash / 7);
  }
  return newHash;
}

const computeStringHash = (str: string) => {
  const bytes = fromString(str);
  let hash = 17;
  bytes.forEach((b) => {
    hash = hash * 31 + b;
    while (hash >= MaxInt32) {
      hash = Math.floor(hash / 7);
    }
  });
  return hash;
};

const computeStructHash = (description: TypeDescription) => {
  let hash = 17;
  for (const [, value] of Object.entries((<ObjectTypeDescription>description).options.props).sort()) {
    let id = SerializerResolver.getTypeIdByInternalSerializerType(value.type);
    if (value.type === InternalSerializerType.OBJECT) {
      id = computeStringHash((<ObjectTypeDescription>value).options.tag);
    }
    hash = computeFieldHash(hash, id);
  }
  return hash;
};

class ObjectSerializerGenerator extends BaseSerializerGenerator {
  description: ObjectTypeDescription;

  constructor(description: TypeDescription, builder: CodecBuilder, scope: Scope) {
    super(description, builder, scope);
    this.description = <ObjectTypeDescription>description;
  }

  writeStmt(accessor: string): string {
    const options = this.description.options;
    const expectHash = computeStructHash(this.description);

    return `
            ${this.builder.writer.int32(expectHash)};
            ${Object.entries(options.props).sort().map(([key, inner]) => {
            const InnerGeneratorClass = CodegenRegistry.get(inner.type);
            if (!InnerGeneratorClass) {
                throw new Error(`${inner.type} generator not exists`);
            }
            const innerGenerator = new InnerGeneratorClass(inner, this.builder, this.scope);
            return innerGenerator.toWriteEmbed(`${accessor}${CodecBuilder.safePropAccessor(key)}`);
        }).join(";\n")
            }
        `;
  }

  readStmt(accessor: (expr: string) => string, refState: RefState): string {
    const options = this.description.options;
    const expectHash = computeStructHash(this.description);
    const result = this.scope.uniqueName("result");
    return `
        if (${this.builder.reader.int32()} !== ${expectHash}) {
            throw new Error("validate hash failed: ${this.safeTag()}. expect ${expectHash}");
        }
        const ${result} = {
            ${Object.entries(options.props).sort().map(([key]) => {
            return `${CodecBuilder.safePropName(key)}: null`;
        }).join(",\n")}
        };
        ${this.maybeReference(result, refState)}
        ${Object.entries(options.props).sort().map(([key, inner]) => {
            const InnerGeneratorClass = CodegenRegistry.get(inner.type);
            if (!InnerGeneratorClass) {
                throw new Error(`${inner.type} generator not exists`);
            }
            const innerGenerator = new InnerGeneratorClass(inner, this.builder, this.scope);
            return innerGenerator.toReadEmbed(expr => `${result}${CodecBuilder.safePropAccessor(key)} = ${expr}`);
        }).join(";\n")
            }
        ${accessor(result)}
         `;
  }

  private safeTag() {
    return CodecBuilder.replaceBackslashAndQuote(this.description.options.tag);
  }

  toReadEmbed(accessor: (expr: string) => string, excludeHead?: boolean, refState?: RefState): string {
    const name = this.scope.declare(
      "tag_ser",
            `fury.classResolver.getSerializerByTag("${this.safeTag()}")`
    );
    if (!excludeHead) {
      return accessor(`${name}.read()`);
    }
    return accessor(`${name}.readInner(${refState!.toConditionExpr()})`);
  }

  toWriteEmbed(accessor: string, excludeHead?: boolean): string {
    const name = this.scope.declare(
      "tag_ser",
            `fury.classResolver.getSerializerByTag("${this.safeTag()}")`
    );
    if (!excludeHead) {
      return `${name}.write(${accessor})`;
    }
    return `${name}.writeInner(${accessor})`;
  }
}

CodegenRegistry.register(InternalSerializerType.OBJECT, ObjectSerializerGenerator);
