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

import { getTypeIdByInternalSerializerType, InternalSerializerType, MaxInt32, Mode } from "../type";
import { Scope } from "./scope";
import { CodecBuilder } from "./builder";
import { ObjectTypeDescription, TypeDescription } from "../description";
import { fromString } from "../platformBuffer";
import { CodegenRegistry } from "./router";
import { BaseSerializerGenerator, RefState } from "./serializer";
import ClassResolver from "../classResolver";
import { FieldInfo, TypeMeta } from "../meta/TypeMeta";

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
    let id = getTypeIdByInternalSerializerType(value.type);
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
    // const metaInformation = Buffer.from(computeMetaInformation(this.description));
    const fields = Object.entries(this.description).map(([key, value]) => {
      return new FieldInfo(key, value.type);
    });
    const typeMetaBinary = new Uint8Array(TypeMeta.fromFields(256, fields).toBytes());
    const typeMetaDeclare = this.scope.declare("typeMeta", `new Uint8Array([${typeMetaBinary.toString()}])`);

    return `
      ${this.builder.writer.int32(expectHash)};
      
      ${
        this.builder.fury.config.mode === Mode.Compatible
          ? this.builder.writer.buffer(typeMetaDeclare)
          : ""
      }

      ${Object.entries(options.props).sort().map(([key, inner]) => {
        const InnerGeneratorClass = CodegenRegistry.get(inner.type);
        if (!InnerGeneratorClass) {
            throw new Error(`${inner.type} generator not exists`);
        }
        const innerGenerator = new InnerGeneratorClass(inner, this.builder, this.scope);
        return innerGenerator.toWriteEmbed(`${accessor}${CodecBuilder.safePropAccessor(key)}`);
      }).join(";\n")}
    `;
  }

  readStmt(accessor: (expr: string) => string, refState: RefState): string {
    const options = this.description.options;
    const expectHash = computeStructHash(this.description);
    const result = this.scope.uniqueName("result");

    return `
      if (${this.builder.reader.int32()} !== ${expectHash}) {
          throw new Error("got ${this.builder.reader.int32()} validate hash failed: ${this.safeTag()}. expect ${expectHash}");
      }
      ${
        this.description.options.withConstructor
? `
          const ${result} = new ${this.builder.getOptions("constructor")}();
        `
: `
          const ${result} = {
            ${Object.entries(options.props).sort().map(([key]) => {
              return `${CodecBuilder.safePropName(key)}: null`;
            }).join(",\n")}
          };
        `
      }
      

      ${this.maybeReference(result, refState)}
      ${
        this.builder.fury.config.mode === Mode.Compatible
          ? this.builder.typeMeta.fromBytes(this.builder.reader.ownName())
          : ""
      }
      ${Object.entries(options.props).sort().map(([key, inner]) => {
        const InnerGeneratorClass = CodegenRegistry.get(inner.type);
        if (!InnerGeneratorClass) {
          throw new Error(`${inner.type} generator not exists`);
        }
        const innerGenerator = new InnerGeneratorClass(inner, this.builder, this.scope);
        return innerGenerator.toReadEmbed(expr => `${result}${CodecBuilder.safePropAccessor(key)} = ${expr}`);
      }).join(";\n")}
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

  getFixedSize(): number {
    const options = (<ObjectTypeDescription> this.description).options;
    let fixedSize = ClassResolver.tagBuffer(options.tag).byteLength + 8;
    if (options.props) {
      Object.values(options.props).forEach((x) => {
        const propGenerator = new (CodegenRegistry.get(x.type)!)(x, this.builder, this.scope);
        fixedSize += propGenerator.getFixedSize();
      });
    } else {
      fixedSize += this.builder.fury.classResolver.getSerializerByTag(options.tag).fixedSize;
    }
    return fixedSize;
  }

  needToWriteRef(): boolean {
    return Boolean(this.builder.fury.config.refTracking);
  }
}

CodegenRegistry.register(InternalSerializerType.OBJECT, ObjectSerializerGenerator);
