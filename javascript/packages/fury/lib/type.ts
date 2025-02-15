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

import { ObjectTypeDescription } from "./description";

export const getTypeIdByInternalSerializerType = (type: InternalSerializerType) => {
  switch (type) {
    case InternalSerializerType.BOOL:
      return 1;
    case InternalSerializerType.INT8:
      return 2;
    case InternalSerializerType.INT16:
      return 3;
    case InternalSerializerType.INT32:
      return 4;
    case InternalSerializerType.VAR_INT32:
      return 5;
    case InternalSerializerType.INT64:
      return 6;
    case InternalSerializerType.VAR_INT64:
      return 7;
    case InternalSerializerType.SLI_INT64:
      return 8;
    case InternalSerializerType.FLOAT16:
      return 9;
    case InternalSerializerType.FLOAT32:
      return 10;
    case InternalSerializerType.FLOAT64:
      return 11;
    case InternalSerializerType.STRING:
      return 12;
    case InternalSerializerType.ENUM:
      return 13;
    case InternalSerializerType.LIST:
      return 14;
    case InternalSerializerType.SET:
      return 15;
    case InternalSerializerType.MAP:
      return 16;
    case InternalSerializerType.DURATION:
      return 17;
    case InternalSerializerType.TIMESTAMP:
      return 18;
    case InternalSerializerType.DECIMAL:
      return 19;
    case InternalSerializerType.BINARY:
      return 20;
    case InternalSerializerType.TUPLE:
    case InternalSerializerType.ARRAY:
      return 21;
    case InternalSerializerType.BOOL_ARRAY:
      return 22;
    case InternalSerializerType.INT8_ARRAY:
      return 23;
    case InternalSerializerType.INT16_ARRAY:
      return 24;
    case InternalSerializerType.INT32_ARRAY:
      return 25;
    case InternalSerializerType.INT64_ARRAY:
      return 26;
    case InternalSerializerType.FLOAT16_ARRAY:
      return 27;
    case InternalSerializerType.FLOAT32_ARRAY:
      return 28;
    case InternalSerializerType.FLOAT64_ARRAY:
      return 29;
    case InternalSerializerType.OBJECT: // todo
      return 256;
    default:
      throw new Error(`typeId is not assigned to type ${InternalSerializerType[type]}`);
  }
};

export enum InternalSerializerType {
  // primitive type
  BOOL,
  INT8,
  INT16,
  INT32,
  VAR_INT32,
  INT64,
  VAR_INT64,
  SLI_INT64,
  FLOAT16,
  FLOAT32,
  FLOAT64,
  STRING,
  ENUM,
  LIST,
  SET,
  MAP,
  DURATION,
  TIMESTAMP,
  DECIMAL,
  BINARY,
  ARRAY,
  BOOL_ARRAY,
  INT8_ARRAY,
  INT16_ARRAY,
  INT32_ARRAY,
  INT64_ARRAY,
  FLOAT16_ARRAY,
  FLOAT32_ARRAY,
  FLOAT64_ARRAY,
  OBJECT,

  // alias type, only use by javascript
  ANY,
  ONEOF,
  TUPLE,
}

export enum ConfigFlags {
  isNullFlag = 1 << 0,
  isLittleEndianFlag = 2,
  isCrossLanguageFlag = 4,
  isOutOfBandFlag = 8,
}

// read, write
export type Serializer<T = any, T2 = any> = {
  read: () => T2;
  write: (v: T2) => T;
  readInner: (refValue?: boolean) => T2;
  writeInner: (v: T2) => T;
  fixedSize: number;
  needToWriteRef: () => boolean;
  getTypeId: () => number;
};

export enum RefFlags {
  NullFlag = -3,
  // RefFlag indicates that object is a not-null value.
  // We don't use another byte to indicate REF, so that we can save one byte.
  RefFlag = -2,
  // NotNullValueFlag indicates that the object is a non-null value.
  NotNullValueFlag = -1,
  // RefValueFlag indicates that the object is a referencable and first read.
  RefValueFlag = 0,
}

export const MaxInt32 = 2147483647;
export const MinInt32 = -2147483648;
export const MaxUInt32 = 0xFFFFFFFF;
export const MinUInt32 = 0;
export const HalfMaxInt32 = MaxInt32 / 2;
export const HalfMinInt32 = MinInt32 / 2;

export const LATIN1 = 0;
export const UTF8 = 1;
export const UTF16 = 2;
export interface Hps {
  serializeString: (str: string, dist: Uint8Array, offset: number) => number;
}

export enum Mode {
  SchemaConsistent,
  Compatible,
}

export interface Config {
  hps?: Hps;
  refTracking?: boolean;
  useSliceString?: boolean;
  hooks?: {
    afterCodeGenerated?: (code: string) => string;
  };
  mode?: Mode;
}

export enum Language {
  XLANG = 0,
  JAVA = 1,
  PYTHON = 2,
  CPP = 3,
  GO = 4,
  JAVASCRIPT = 5,
  RUST = 6,
}

export const MAGIC_NUMBER = 0x62D4;

export interface ObjectFuryClsInfo {
  constructor: new () => any;
  toObjectDescription(): ObjectTypeDescription;
}

export const FuryClsInfoSymbol = Symbol("furyClsInfo");
