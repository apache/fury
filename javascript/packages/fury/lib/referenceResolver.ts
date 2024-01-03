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

import {
  RefFlags,
  BinaryReader,
  BinaryWriter,
  InternalSerializerType,
} from "./type";

export const makeHead = (flag: RefFlags, type: InternalSerializerType) => {
  return (((Math.floor(type) << 16) >>> 16) << 8) | ((flag << 24) >>> 24);
};

export const ReferenceResolver = (
  config: {
    refTracking?: boolean
  },
  binaryWriter: BinaryWriter,
  binaryReader: BinaryReader,
) => {
  let readObjects: any[] = [];
  let writeObjects: any[] = [];

  function reset() {
    readObjects = [];
    writeObjects = [];
  }

  function getReadObjectByRefId(refId: number) {
    return readObjects[refId];
  }

  function readRefFlag() {
    return binaryReader.int8() as RefFlags;
  }

  function pushReadObject(object: any) {
    readObjects.push(object);
  }

  function pushWriteObject(object: any) {
    writeObjects.push(object);
  }

  function existsWriteObject(obj: any) {
    for (let index = 0; index < writeObjects.length; index++) {
      if (writeObjects[index] === obj) {
        return index;
      }
    }
  }

  return {
    existsWriteObject,
    pushWriteObject,
    pushReadObject,
    readRefFlag,
    getReadObjectByRefId,
    reset,
  };
};
