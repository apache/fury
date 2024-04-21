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
import { BaseSerializerGenerator } from "./serializer";
import { CodegenRegistry } from "./router";
import { InternalSerializerType, RefFlags, Serializer } from "../type";
import { Scope } from "./scope";
import Fury from "../fury";
import { Meta, getMeta } from "../meta";
import SerializerResolver from "../classResolver";

export class AnySerializer {
  meta: Meta;

  constructor(private fury: Fury) {
    this.meta = getMeta(Type.any(), fury);
  }

  readInner() {
    throw new Error("Anonymous serializer can't call directly");
  }

  writeInner() {
    throw new Error("Anonymous serializer can't call directly");
  }

  detectSerializer() {
    const typeId = this.fury.binaryReader.int16();
    let serializer: Serializer;
    if (typeId === SerializerResolver.getTypeIdByInternalSerializerType(InternalSerializerType.OBJECT)) {
      const tag = this.fury.classResolver.readTag(this.fury.binaryReader)();
      serializer = this.fury.classResolver.getSerializerByTag(tag);
    } else {
      serializer = this.fury.classResolver.getSerializerById(typeId);
    }
    if (!serializer) {
      throw new Error(`cant find implements of typeId: ${typeId}`);
    }
    return serializer;
  }

  read() {
    const flag = this.fury.referenceResolver.readRefFlag();
    switch (flag) {
      case RefFlags.RefValueFlag:
        return this.detectSerializer().readInner(true);
      case RefFlags.RefFlag:
        return this.fury.referenceResolver.getReadObject(this.fury.binaryReader.varUInt32());
      case RefFlags.NullFlag:
        return null;
      case RefFlags.NotNullValueFlag:
        return this.detectSerializer().readInner(false);
    }
  }

  write(v: any) {
    if (v === null || v === undefined) {
      this.fury.binaryWriter.reserve(1);
      this.fury.binaryWriter.int8(RefFlags.NullFlag); // null
      return;
    }

    const serializer = this.fury.classResolver.getSerializerByData(v);
    if (!serializer) {
      throw new Error(`Failed to detect the Fury serializer from JavaScript type: ${typeof v}`);
    }
    this.fury.binaryWriter.reserve(serializer.meta.fixedSize);
    serializer.write(v);
  }
}

class AnySerializerGenerator extends BaseSerializerGenerator {
  description: TypeDescription;

  constructor(description: TypeDescription, builder: CodecBuilder, scope: Scope) {
    super(description, builder, scope);
    this.description = description;
  }

  writeStmt(): string {
    throw new Error("Type Any writeStmt can't inline");
  }

  readStmt(): string {
    throw new Error("Type Any readStmt can't inline");
  }

  toReadEmbed(accessor: (expr: string) => string, excludeHead = false): string {
    if (excludeHead) {
      throw new Error("Anonymous can't excludeHead");
    }
    return accessor(`${this.builder.furyName()}.anySerializer.read()`);
  }

  toWriteEmbed(accessor: string, excludeHead = false): string {
    if (excludeHead) {
      throw new Error("Anonymous can't excludeHead");
    }
    return `${this.builder.furyName()}.anySerializer.write(${accessor})`;
  }

  toSerializer() {
    this.scope.assertNameNotDuplicate("read");
    this.scope.assertNameNotDuplicate("readInner");
    this.scope.assertNameNotDuplicate("write");
    this.scope.assertNameNotDuplicate("writeInner");

    const declare = `
      const readInner = (fromRef) => {
        throw new Error("Type Any readInner can't call directly");
      };
      const read = () => {
        ${this.toReadEmbed(expr => `return ${expr}`)}
      };
      const writeInner = (v) => {
        throw new Error("Type Any writeInner can't call directly");
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

CodegenRegistry.register(InternalSerializerType.ANY, AnySerializerGenerator);
CodegenRegistry.registerExternal(AnySerializer);
