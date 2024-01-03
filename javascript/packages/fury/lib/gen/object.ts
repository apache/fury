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
import { Builder } from "./builder";
import { ObjectTypeDescription, TypeDescription } from "../description";
import { fromString } from "../platformBuffer";
import { Register } from "./router";
import { BaseSerializerGenerator } from "./serializer";

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
    let id = value.type;
    if (value.type === InternalSerializerType.ARRAY || value.type === InternalSerializerType.TUPLE || value.type === InternalSerializerType.MAP) {
      id = Math.floor(value.type); // TODO add map key&value type into schema hash
    } else if (value.type === InternalSerializerType.FURY_TYPE_TAG) {
      id = computeStringHash((<ObjectTypeDescription>value).options.tag);
    }
    hash = computeFieldHash(hash, id);
  }
  return hash;
};

class ObjectSerializerGenerator extends BaseSerializerGenerator {
  description: ObjectTypeDescription;

  constructor(description: TypeDescription, builder: Builder, scope: Scope) {
    super(description, builder, scope);
    this.description = <ObjectTypeDescription>description;
  }

  writeStmt(accessor: string): string {
    const options = this.description.options;
    const expectHash = computeStructHash(this.description);
    const tagWriter = this.scope.declare("tagWriter", `${this.builder.classResolver.createTagWriter(this.safeTag())}`);

    return `
            ${tagWriter}.write(${this.builder.writer.ownName()});
            ${this.builder.writer.int32(expectHash)};
            ${Object.entries(options.props).sort().map(([key, inner]) => {
            const InnerGeneratorClass = Register.get(inner.type);
            if (!InnerGeneratorClass) {
                throw new Error(`${inner.type} generator not exists`);
            }
            const innerGenerator = new InnerGeneratorClass(inner, this.builder, this.scope);
            return innerGenerator.toWriteEmbed(`${accessor}${Builder.safePropAccessor(key)}`);
        }).join(";\n")
            }
        `;
  }

  readStmt(accessor: (expr: string) => string): string {
    const options = this.description.options;
    const expectHash = computeStructHash(this.description);
    const result = this.scope.uniqueName("result");
    return `
        if (${this.builder.reader.int32()} !== ${expectHash}) {
            throw new Error("validate hash failed: ${this.safeTag()}. expect ${expectHash}");
        }
        const ${result} = {
            ${Object.entries(options.props).sort().map(([key]) => {
            return `${Builder.safePropName(key)}: null`;
        }).join(",\n")}
        };
        ${this.pushReadRefStmt(result)}
        ${Object.entries(options.props).sort().map(([key, inner]) => {
            const InnerGeneratorClass = Register.get(inner.type);
            if (!InnerGeneratorClass) {
                throw new Error(`${inner.type} generator not exists`);
            }
            const innerGenerator = new InnerGeneratorClass(inner, this.builder, this.scope);
            return innerGenerator.toReadEmbed(expr => `${result}${Builder.safePropAccessor(key)} = ${expr}`);
        }).join(";\n")
            }
        ${accessor(result)}
         `;
  }

  safeTag() {
    return Builder.replaceBackslashAndQuote(this.description.options.tag);
  }

  toReadEmbed(accessor: (expr: string) => string): string {
    const name = this.scope.declare(
      "tag_ser",
            `fury.classResolver.getSerializerByTag("${this.safeTag()}")`
    );
    return accessor(`${name}.read()`);
  }

  toWriteEmbed(accessor: string): string {
    const name = this.scope.declare(
      "tag_ser",
            `fury.classResolver.getSerializerByTag("${this.safeTag()}")`
    );
    return `${name}.write(${accessor})`;
  }
}

Register.reg(InternalSerializerType.FURY_TYPE_TAG, ObjectSerializerGenerator);
