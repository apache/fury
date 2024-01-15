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
import { TypeDescription } from "../description";

export interface SerializerGenerator {
  writeStmt(accessor: string): string
  readStmt(accessor: (expr: string) => string): string
  toSerializer(): string
  toWriteEmbed(accessor: string, withHead?: boolean): string
  toReadEmbed(accessor: (expr: string) => string, withHead?: boolean): string
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
    return this.builder.referenceResolver.pushReadObject(accessor);
  }

  protected wrapWriteHead(accessor: string, stmt: (accessor: string) => string) {
    const meta = this.builder.meta(this.description);
    const noneable = meta.noneable;
    if (noneable) {
      const head = makeHead(RefFlags.RefValueFlag, this.description.type);

      const normaStmt = `
            if (${accessor} !== null && ${accessor} !== undefined) {
                ${this.builder.writer.int24(head)};
                ${stmt(accessor)};
            } else {
                ${this.builder.writer.int8(RefFlags.NullFlag)};
            }
            `;
      if (this.builder.config().refTracking) {
        const existsId = this.scope.uniqueName("existsId");
        return `
                const ${existsId} = ${this.builder.referenceResolver.existsWriteObject(accessor)};
                if (typeof ${existsId} === "number") {
                    ${this.builder.writer.int8(RefFlags.RefFlag)}
                    ${this.builder.writer.varUInt32(existsId)}
                } else {
                    ${this.builder.referenceResolver.pushWriteObject(accessor)}
                    ${normaStmt}
                }
                `;
      } else {
        return normaStmt;
      }
    } else {
      const head = makeHead(RefFlags.NotNullValueFlag, this.description.type);
      return `
            ${this.builder.writer.int24(head)};
            if (${accessor} !== null && ${accessor} !== undefined) {
                ${stmt(accessor)};
            } else {
                ${typeof meta.default === "string" ? stmt(`"${meta.default}"`) : stmt(meta.default)};
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
              ${accessor(this.builder.referenceResolver.getReadObjectByRefId(this.builder.reader.varUInt32()))}
              break;
          case ${RefFlags.NullFlag}:
              ${accessor("null")}
              break;
      }
      `;
  }

  toWriteEmbed(accessor: string, withHead = true) {
    if (!withHead) {
      return this.writeStmt(accessor);
    }
    return this.wrapWriteHead(accessor, accessor => this.writeStmt(accessor));
  }

  toReadEmbed(accessor: (expr: string) => string, withHead = true) {
    if (!withHead) {
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
