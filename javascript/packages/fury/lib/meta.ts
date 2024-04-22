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

import Fury from "./fury";
import ClassResolver from "./classResolver";
import { ObjectTypeDescription, TypeDescription } from "./description";
import { InternalSerializerType } from "./type";

export type Meta = {
  fixedSize: number;
  needToWriteRef: boolean;
  type: InternalSerializerType;
  typeId: number | null;
};

export const getMeta = (description: TypeDescription, fury: Fury): Meta => {
  const type = description.type;
  switch (type) {
    case InternalSerializerType.STRING:
      return {
        fixedSize: 8,
        needToWriteRef: false,
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.ARRAY:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking),
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.TUPLE:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking),
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.MAP:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking),
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.BOOL:
    case InternalSerializerType.INT8:
      return {
        fixedSize: 4,
        needToWriteRef: false,
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.INT16:
    case InternalSerializerType.FLOAT16:
      return {
        fixedSize: 5,
        needToWriteRef: false,
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.VAR_INT32:
    case InternalSerializerType.INT32:
    case InternalSerializerType.FLOAT32:
      return {
        fixedSize: 7,
        needToWriteRef: false,
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.SLI_INT64:
    case InternalSerializerType.INT64:
    case InternalSerializerType.FLOAT64:
      return {
        fixedSize: 11,
        needToWriteRef: false,
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.BINARY:
      return {
        fixedSize: 8,
        needToWriteRef: Boolean(fury.config.refTracking),
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.DURATION:
      return {
        fixedSize: 7,
        needToWriteRef: false,
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.TIMESTAMP:
      return {
        fixedSize: 11,
        needToWriteRef: false,
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.OBJECT:
    {
      const options = (<ObjectTypeDescription>description).options;
      let fixedSize = ClassResolver.tagBuffer(options.tag).byteLength + 8;
      if (options.props) {
        Object.values(options.props).forEach(x => fixedSize += getMeta(x, fury).fixedSize);
      } else {
        fixedSize += fury.classResolver.getSerializerByTag(options.tag).meta.fixedSize;
      }
      return {
        fixedSize,
        needToWriteRef: Boolean(fury.config.refTracking),
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    }

    case InternalSerializerType.SET:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking),
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.BOOL_ARRAY:
    case InternalSerializerType.INT8_ARRAY:
    case InternalSerializerType.INT16_ARRAY:
    case InternalSerializerType.INT32_ARRAY:
    case InternalSerializerType.INT64_ARRAY:
    case InternalSerializerType.FLOAT16_ARRAY:
    case InternalSerializerType.FLOAT32_ARRAY:
    case InternalSerializerType.FLOAT64_ARRAY:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking),
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    case InternalSerializerType.ONEOF:
    case InternalSerializerType.ANY:
      return {
        fixedSize: 11,
        needToWriteRef: Boolean(fury.config.refTracking),
        type,
        typeId: null,
      };
    case InternalSerializerType.ENUM:
      return {
        fixedSize: 7,
        needToWriteRef: false,
        type,
        typeId: ClassResolver.getTypeIdByInternalSerializerType(type),
      };
    default:
      throw new Error(`Meta of ${description.type} not exists`);
  }
};
