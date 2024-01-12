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
  fixedSize: number
  needToWriteRef: boolean
  type: InternalSerializerType
};

export const getMeta = (description: TypeDescription, fury: Fury): Meta => {
  const type = description.type;
  switch (type) {
    case InternalSerializerType.STRING:
      return {
        fixedSize: 8,
        needToWriteRef: Boolean(fury.config.refTracking) && false,
        type,
      };
    case InternalSerializerType.ARRAY:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking) && true,
        type,
      };
    case InternalSerializerType.TUPLE:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking) && true,
        type,
      };
    case InternalSerializerType.MAP:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking) && true,
        type,
      };
    case InternalSerializerType.BOOL:
    case InternalSerializerType.UINT8:
    case InternalSerializerType.INT8:
      return {
        fixedSize: 4,
        needToWriteRef: Boolean(fury.config.refTracking) && false,
        type,
      };
    case InternalSerializerType.UINT16:
    case InternalSerializerType.INT16:
      return {
        fixedSize: 5,
        needToWriteRef: Boolean(fury.config.refTracking) && false,
        type,
      };
    case InternalSerializerType.UINT32:
    case InternalSerializerType.INT32:
    case InternalSerializerType.FLOAT:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking) && false,
        type,
      };
    case InternalSerializerType.UINT64:
    case InternalSerializerType.INT64:
    case InternalSerializerType.DOUBLE:
      return {
        fixedSize: 11,
        needToWriteRef: Boolean(fury.config.refTracking) && false,
        type,
      };
    case InternalSerializerType.BINARY:
      return {
        fixedSize: 8,
        needToWriteRef: Boolean(fury.config.refTracking) && true,
        type,
      };
    case InternalSerializerType.DATE:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking) && false,
        type,
      };
    case InternalSerializerType.TIMESTAMP:
      return {
        fixedSize: 11,
        needToWriteRef: Boolean(fury.config.refTracking) && false,
        type,
      };
    case InternalSerializerType.FURY_TYPE_TAG:
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
        needToWriteRef: Boolean(fury.config.refTracking) && true,
        type,
      };
    }

    case InternalSerializerType.FURY_SET:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking) && true,
        type,
      };
    case InternalSerializerType.FURY_PRIMITIVE_BOOL_ARRAY:
    case InternalSerializerType.FURY_PRIMITIVE_SHORT_ARRAY:
    case InternalSerializerType.FURY_PRIMITIVE_INT_ARRAY:
    case InternalSerializerType.FURY_PRIMITIVE_LONG_ARRAY:
    case InternalSerializerType.FURY_PRIMITIVE_FLOAT_ARRAY:
    case InternalSerializerType.FURY_PRIMITIVE_DOUBLE_ARRAY:
    case InternalSerializerType.FURY_STRING_ARRAY:
      return {
        fixedSize: 7,
        needToWriteRef: Boolean(fury.config.refTracking) && true,
        type,
      };
    case InternalSerializerType.ANY:
      return {
        fixedSize: 11,
        needToWriteRef: Boolean(fury.config.refTracking) && true,
        type,
      };
    default:
      throw new Error(`Meta of ${description.type} not exists`);
  }
};
