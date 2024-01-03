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

import { TypeDescription } from "../description";
import { Builder } from "./builder";
import { BaseSerializerGenerator } from "./serializer";
import { Register } from "./router";
import { InternalSerializerType } from "../type";
import { Scope } from "./scope";

function buildNumberSerializer(writeFun: (builder: Builder, accessor: string) => string, read: (builder: Builder) => string) {
  return class NumberSerializerGenerator extends BaseSerializerGenerator {
    description: TypeDescription;

    constructor(description: TypeDescription, builder: Builder, scope: Scope) {
      super(description, builder, scope);
      this.description = description;
    }

    writeStmt(accessor: string): string {
      return writeFun(this.builder, accessor);
    }

    readStmt(accessor: (expr: string) => string): string {
      return accessor(read(this.builder));
    }
  };
}

Register.reg(InternalSerializerType.UINT8,
  buildNumberSerializer(
    (builder, accessor) => builder.writer.uint8(accessor),
    builder => builder.reader.uint8()
  )
);
Register.reg(InternalSerializerType.INT8,
  buildNumberSerializer(
    (builder, accessor) => builder.writer.int8(accessor),
    builder => builder.reader.int8()
  )
);
Register.reg(InternalSerializerType.UINT16,
  buildNumberSerializer(
    (builder, accessor) => builder.writer.uint16(accessor),
    builder => builder.reader.uint16()
  )
);
Register.reg(InternalSerializerType.INT16,
  buildNumberSerializer(
    (builder, accessor) => builder.writer.int16(accessor),
    builder => builder.reader.int16()
  )
);
Register.reg(InternalSerializerType.UINT32,
  buildNumberSerializer(
    (builder, accessor) => builder.writer.uint32(accessor),
    builder => builder.reader.uint32()
  )
);
Register.reg(InternalSerializerType.INT32,
  buildNumberSerializer(
    (builder, accessor) => builder.writer.int32(accessor),
    builder => builder.reader.int32()
  )
);
Register.reg(InternalSerializerType.UINT64,
  buildNumberSerializer(
    (builder, accessor) => builder.writer.varUInt64(accessor),
    builder => builder.reader.varUInt64()
  )
);
Register.reg(InternalSerializerType.INT64,
  buildNumberSerializer(
    (builder, accessor) => builder.writer.sliLong(accessor),
    builder => builder.reader.sliLong()
  )
);
Register.reg(InternalSerializerType.FLOAT,
  buildNumberSerializer(
    (builder, accessor) => builder.writer.float(accessor),
    builder => builder.reader.float()
  )
);
Register.reg(InternalSerializerType.DOUBLE,
  buildNumberSerializer(
    (builder, accessor) => builder.writer.double(accessor),
    builder => builder.reader.double()
  )
);
