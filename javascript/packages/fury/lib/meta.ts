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

import ClassResolver from "./classResolver";
import { ObjectTypeDescription, TypeDescription } from "./description";
import { Fury, InternalSerializerType } from "./type";

export type Meta<T> = {
  fixedSize: number
  noneable: boolean
  default?: T
};

const epochDate = new Date("1970/01/01 00:00");

export const getMeta = (description: TypeDescription, fury: Fury): Meta<any> => {
  const type = description.type;
  switch (type) {
    case InternalSerializerType.STRING:
      return {
        fixedSize: 8,
        noneable: false,
        default: "",
      };
    case InternalSerializerType.ARRAY:
      return {
        fixedSize: 7,
        noneable: true,
      };
    case InternalSerializerType.TUPLE:
      return {
        fixedSize: 7,
        noneable: true,
      };
    case InternalSerializerType.MAP:
      return {
        fixedSize: 7,
        noneable: true,
      };
    case InternalSerializerType.BOOL:
    case InternalSerializerType.UINT8:
    case InternalSerializerType.INT8:
      return {
        fixedSize: 4,
        noneable: false,
        default: 0,
      };
    case InternalSerializerType.UINT16:
    case InternalSerializerType.INT16:
      return {
        fixedSize: 5,
        noneable: false,
        default: 0,
      };
    case InternalSerializerType.UINT32:
    case InternalSerializerType.INT32:
    case InternalSerializerType.FLOAT:
      return {
        fixedSize: 7,
        noneable: false,
        default: 0,
      };
    case InternalSerializerType.UINT64:
    case InternalSerializerType.INT64:
    case InternalSerializerType.DOUBLE:
      return {
        fixedSize: 11,
        noneable: false,
        default: 0,
      };
    case InternalSerializerType.BINARY:
      return {
        fixedSize: 8,
        noneable: true,
      };
    case InternalSerializerType.DATE:
      return {
        fixedSize: 7,
        noneable: false,
        default: epochDate.getTime(),
      };
    case InternalSerializerType.TIMESTAMP:
      return {
        fixedSize: 11,
        noneable: false,
        default: epochDate.getTime(),
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
        noneable: true,
      };
    }

    case InternalSerializerType.FURY_SET:
      return {
        fixedSize: 7,
        noneable: true,
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
        noneable: true,
      };
    case InternalSerializerType.ANY:
      return {
        fixedSize: 11,
        noneable: true,
      };
    default:
      throw new Error(`Meta of ${description.type} not exists`);
  }
};
