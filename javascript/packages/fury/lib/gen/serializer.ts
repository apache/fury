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

import { InternalSerializerType } from "../type";
import { CodecBuilder } from "./builder";
import { makeHead } from "../referenceResolver";
import { RefFlags } from "../type";
import { Scope } from "./scope";
import { TypeDescription, ObjectTypeDescription } from "../description";

export interface SerializerGenerator {
  writeStmt(accessor: string): string
  readStmt(accessor: (expr: string) => string): string
  toSerializer(): string
  toWriteEmbed(accessor: string, excludeHead?: boolean): string
  toReadEmbed(accessor: (expr: string) => string, excludeHead?: boolean): string
}

export abstract class BaseSerializerGenerator implements SerializerGenerator {
  constructor(
    protected description: TypeDescription,
    protected builder: CodecBuilder,
    protected scope: Scope,
  ) {

  }

  abstract writeStmt(accessor: string): string;

  abstract readStmt(accessor: (expr: string) => string): string;

  protected pushReadRefStmt(accessor: string) {
    if (!this.builder.config().refTracking) {
      return "";
    }
    return this.builder.referenceResolver.reference(accessor);
  }

  safeTag() {
    return CodecBuilder.replaceBackslashAndQuote((<ObjectTypeDescription>this.description).options.tag);
  }

  protected wrapWriteHead(accessor: string, stmt: (accessor: string) => string) {
    const meta = this.builder.meta(this.description);

    const maybeTag = () => {
      if (this.description.type !== InternalSerializerType.FURY_TYPE_TAG) {
        return "";
      }
      const tagWriter = this.scope.declare("tagWriter", `${this.builder.classResolver.createTagWriter(this.safeTag())}`);
      return `${tagWriter}.write(${this.builder.writer.ownName()})`;
    };

    if (meta.needToWriteRef) {
      const head = makeHead(RefFlags.RefValueFlag, this.description.type);
      const existsId = this.scope.uniqueName("existsId");
      return `
                if (${accessor} !== null && ${accessor} !== undefined) {
                    const ${existsId} = ${this.builder.referenceResolver.existsWriteObject(accessor)};
                    if (typeof ${existsId} === "number") {
                        ${this.builder.writer.int8(RefFlags.RefFlag)}
                        ${this.builder.writer.varUInt32(existsId)}
                    } else {
                        ${this.builder.referenceResolver.writeRef(accessor)}
                        ${this.builder.writer.int24(head)};
                        ${maybeTag()}
                        ${stmt(accessor)};
                    }
                } else {
                    ${this.builder.writer.int8(RefFlags.NullFlag)};
                }
                `;
    } else {
      const head = makeHead(RefFlags.NotNullValueFlag, this.description.type);
      return `
            if (${accessor} !== null && ${accessor} !== undefined) {
                ${this.builder.writer.int24(head)};
                ${maybeTag()}
                ${stmt(accessor)};
            } else {
                ${this.builder.writer.int8(RefFlags.NullFlag)};
            }`;
    }
  }

  protected wrapReadHead(accessor: (expr: string) => string, stmt: (accessor: (expr: string) => string) => string) {
    return `
      switch (${this.builder.reader.int8()}) {
          case ${RefFlags.NotNullValueFlag}:
          case ${RefFlags.RefValueFlag}:
              if (${this.builder.reader.int16()} === ${InternalSerializerType.FURY_TYPE_TAG}) {
                  ${this.builder.classResolver.readTag(this.builder.reader.ownName())};
              }
              ${stmt(accessor)}
              break;
          case ${RefFlags.RefFlag}:
              ${accessor(this.builder.referenceResolver.getReadObject(this.builder.reader.varUInt32()))}
              break;
          case ${RefFlags.NullFlag}:
              ${accessor("null")}
              break;
      }
      `;
  }

  toWriteEmbed(accessor: string, excludeHead = false) {
    if (excludeHead) {
      return this.writeStmt(accessor);
    }
    return this.wrapWriteHead(accessor, accessor => this.writeStmt(accessor));
  }

  toReadEmbed(accessor: (expr: string) => string, excludeHead = false) {
    if (excludeHead) {
      return this.readStmt(accessor);
    }
    return this.wrapReadHead(accessor, accessor => this.readStmt(accessor));
  }

  toSerializer() {
    this.scope.assertNameNotDuplicate("read");
    this.scope.assertNameNotDuplicate("readInner");
    this.scope.assertNameNotDuplicate("write");
    this.scope.assertNameNotDuplicate("writeInner");

    const declare = `
      const readInner = () => {
        ${this.readStmt(x => `return ${x}`)}
      };
      const read = () => {
        ${this.wrapReadHead(x => `return ${x}`, accessor => accessor(`readInner()`))}
      };
      const writeInner = (v) => {
        ${this.writeStmt("v")}
      };
      const write = (v) => {
        ${this.wrapWriteHead("v", accessor => `writeInner(${accessor})`)}
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
