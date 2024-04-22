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

import { MapTypeDescription, TypeDescription } from "../description";
import { CodecBuilder } from "./builder";
import { BaseSerializerGenerator, RefState } from "./serializer";
import { CodegenRegistry } from "./router";
import { InternalSerializerType, RefFlags, Serializer } from "../type";
import { Scope } from "./scope";
import Fury from "../fury";

const MapFlags = {
  /** Whether track elements ref. */
  TRACKING_REF: 0b1,

  /** Whether collection has null. */
  HAS_NULL: 0b10,

  /** Whether collection elements type is not declare type. */
  NOT_DECL_ELEMENT_TYPE: 0b100,

  /** Whether collection elements type different. */
  NOT_SAME_TYPE: 0b1000,
};

class TypeInfo {
  private static IS_NULL = 0b10;
  private static TRACKING_REF = 0b01;
  static elementInfo(typeId: number, isNull: 0 | 1, trackRef: 0 | 1) {
    return typeId << 16 | isNull << 1 | trackRef;
  }

  static isNull(info: number) {
    return info & this.IS_NULL;
  }

  static trackingRef(info: number) {
    return info & this.TRACKING_REF;
  }
}

class MapChunkWriter {
  private preKeyInfo = 0;
  private preValueInfo = 0;

  private chunkSize = 0;
  private chunkOffset = 0;
  private header = 0;

  constructor(private fury: Fury) {

  }

  private getHead(keyInfo: number, valueInfo: number) {
    let flag = 0;
    if (TypeInfo.isNull(keyInfo)) {
      flag |= MapFlags.HAS_NULL;
    }
    if (TypeInfo.trackingRef(keyInfo)) {
      flag |= MapFlags.TRACKING_REF;
    }
    flag <<= 4;
    if (TypeInfo.isNull(valueInfo)) {
      flag |= MapFlags.HAS_NULL;
    }
    if (TypeInfo.trackingRef(valueInfo)) {
      flag |= MapFlags.TRACKING_REF;
    }
    return flag;
  }

  private writeHead(keyInfo: number, valueInfo: number) {
    // KV header
    const header = this.getHead(keyInfo, valueInfo);
    this.fury.binaryWriter.uint8(header);
    // chunkSize, max 255
    this.chunkOffset = this.fury.binaryWriter.getCursor();
    this.fury.binaryWriter.uint8(0);
    this.fury.binaryWriter.uint32((keyInfo >> 16) | (valueInfo & 0xFFFF0000));
    return header;
  }

  next(keyInfo: number, valueInfo: number) {
    // max size of chunk is 255
    if (this.chunkSize == 255
      || this.chunkOffset == 0
      || this.preKeyInfo !== keyInfo
      || this.preValueInfo !== valueInfo
    ) {
      // new chunk
      this.endChunk();
      this.chunkSize++;
      this.preKeyInfo = keyInfo;
      this.preValueInfo = valueInfo;
      return this.header = this.writeHead(keyInfo, valueInfo);
    }
    this.chunkSize++;
    return this.header;
  }

  endChunk() {
    if (this.chunkOffset > 0) {
      this.fury.binaryWriter.setUint8Position(this.chunkOffset, this.chunkSize);
      this.chunkSize = 0;
    }
  }
}

class MapAnySerializer {
  private keySerializer: Serializer | null = null;
  private valueSerializer: Serializer | null = null;

  constructor(private fury: Fury, keySerializerId: null | number, valueSerializerId: null | number) {
    if (keySerializerId !== null) {
      fury.classResolver.getSerializerById(keySerializerId);
    }
    if (valueSerializerId !== null) {
      fury.classResolver.getSerializerById(valueSerializerId);
    }
  }

  private writeHead(header: number, v: any) {
    if (header !== 0) {
      if (header & MapFlags.HAS_NULL) {
        if (v === null || v === undefined) {
          this.fury.binaryWriter.uint8(RefFlags.NullFlag);
        }
      }
      if (header & MapFlags.TRACKING_REF) {
        const keyRef = this.fury.referenceResolver.existsWriteObject(v);
        if (keyRef !== undefined) {
          this.fury.binaryWriter.uint8(RefFlags.RefFlag);
          this.fury.binaryWriter.uint16(keyRef);
        } else {
          this.fury.binaryWriter.uint8(RefFlags.RefValueFlag);
        }
      } else {
        this.fury.binaryWriter.uint8(RefFlags.NotNullValueFlag);
      }
    }
  }

  write(value: Map<any, any>) {
    const mapChunkWriter = new MapChunkWriter(this.fury);
    this.fury.binaryWriter.varInt32(value.size);
    for (const [k, v] of value.entries()) {
      const keySerializer = this.keySerializer !== null ? this.keySerializer : this.fury.classResolver.getSerializerByData(k);
      const valueSerializer = this.valueSerializer !== null ? this.valueSerializer : this.fury.classResolver.getSerializerByData(v);

      const header = mapChunkWriter.next(
        TypeInfo.elementInfo(keySerializer!.meta.typeId!, k == null ? 1 : 0, keySerializer!.meta.needToWriteRef ? 1 : 0),
        TypeInfo.elementInfo(valueSerializer!.meta.typeId!, v == null ? 1 : 0, valueSerializer!.meta.needToWriteRef ? 1 : 0)
      );

      this.writeHead(header >> 4, k);
      keySerializer!.writeInner(k);
      this.writeHead(header & 0b00001111, v);
      valueSerializer!.writeInner(v);
    }
    mapChunkWriter.endChunk();
  }

  private readElement(header: number, serializer: Serializer | null) {
    if (header === 0) {
      return serializer!.readInner(false);
    }
    const isSame = !(header & MapFlags.NOT_SAME_TYPE);
    const includeNone = header & MapFlags.HAS_NULL;
    const trackingRef = header & MapFlags.TRACKING_REF;

    let flag = 0;
    if (trackingRef || includeNone) {
      flag = this.fury.binaryReader.uint8();
    }
    if (!isSame) {
      serializer = this.fury.classResolver.getSerializerByType(this.fury.binaryReader.int16());
    }
    switch (flag) {
      case RefFlags.RefValueFlag:
        return serializer!.readInner(true);
      case RefFlags.RefFlag:
        return this.fury.referenceResolver.getReadObject(this.fury.binaryReader.varUInt32());
      case RefFlags.NullFlag:
        return null;
      case RefFlags.NotNullValueFlag:
        return serializer!.readInner(false);
    }
  }

  read(fromRef: boolean): any {
    let count = this.fury.binaryReader.varInt32();
    const result = new Map();
    if (fromRef) {
      this.fury.referenceResolver.reference(result);
    }
    while (count > 0) {
      const header = this.fury.binaryReader.uint16();
      const chunkSize = header >> 8;
      const keyHeader = header >> 12;
      const valueHeader = header & 0b00001111;

      let keySerializer = null;
      let valueSerializer = null;

      if (!(keyHeader & MapFlags.NOT_SAME_TYPE)) {
        keySerializer = this.fury.classResolver.getSerializerById(this.fury.binaryReader.uint16());
      }
      if (!(valueHeader & MapFlags.NOT_SAME_TYPE)) {
        valueSerializer = this.fury.classResolver.getSerializerById(this.fury.binaryReader.uint16());
      }
      for (let index = 0; index < chunkSize; index++) {
        result.set(
          this.readElement(keyHeader, keySerializer),
          this.readElement(valueHeader, valueSerializer)
        );
        count--;
      }
    }
    return result;
  }
}

export class MapSerializerGenerator extends BaseSerializerGenerator {
  description: MapTypeDescription;

  constructor(description: TypeDescription, builder: CodecBuilder, scope: Scope) {
    super(description, builder, scope);
    this.description = <MapTypeDescription>description;
  }

  private innerMeta() {
    const inner = this.description;
    return [this.builder.meta(inner.options.key), this.builder.meta(inner.options.value)];
  }

  writeStmt(accessor: string): string {
    const [keyMeta, valueMeta] = this.innerMeta();
    const anySerializer = this.builder.getExternal(MapAnySerializer.name);
    return `
        new (${anySerializer})(${this.builder.furyName()}, ${keyMeta.typeId}, ${valueMeta.typeId}).write(${accessor})
    `;
  }

  readStmt(accessor: (expr: string) => string, refState: RefState): string {
    const anySerializer = this.builder.getExternal(MapAnySerializer.name);
    const [keyMeta, valueMeta] = this.innerMeta();
    return accessor(`new (${anySerializer})(${this.builder.furyName()}, ${keyMeta.typeId}, ${valueMeta.typeId}).read(${refState.toConditionExpr()})
      `);
  }
}

CodegenRegistry.registerExternal(MapAnySerializer);
CodegenRegistry.register(InternalSerializerType.MAP, MapSerializerGenerator);
