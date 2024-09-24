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

import { BinaryWriter } from "../writer";
import { BinaryReader } from "../reader";
import { MetaString } from "./MetaString";

enum Encoding {
  Utf8,
  AllToLowerSpecial,
  LowerUpperDigitSpecial,
}

export class FieldInfo {
  constructor(private field_name: string, private field_id: number) {
  }

  static u8_to_encoding(value: number) {
    switch (value) {
      case 0x00:
        return Encoding.Utf8;
      case 0x01:
        return Encoding.AllToLowerSpecial;
      case 0x02:
        return Encoding.LowerUpperDigitSpecial;
    }
  }

  static from_bytes(reader: BinaryReader) {
    const header = reader.uint8();
    let size = (header & 0b11100000) >> 5;
    size = (size === 0b111) ? reader.varInt32() + 7 : size;
    const type_id = reader.int16();
    // reader.skip(size);
    const field_name = MetaString.decode(reader.buffer(size)); // now we commentd this line , the code work well
    return new FieldInfo(field_name, type_id);
  }

  to_bytes() {
    const writer = new BinaryWriter({});
    const meta_string = MetaString.encode(this.field_name);
    let header = 1 << 2;
    const size = meta_string.byteLength;
    const big_size = size >= 7;
    if (big_size) {
      header |= 0b11100000;
      writer.uint8(header);
      writer.varInt32(size - 7);
    } else {
      header |= size << 5;
      writer.uint8(header);
    }
    writer.int16(this.field_id);
    writer.buffer(meta_string);
    return writer.dump();
  }
}

// Using classes to emulate struct methods in Rust
class TypeMetaLayer {
  constructor(private type_id: number, private field_info: FieldInfo[]) {
  }

  get_type_id() {
    return this.type_id;
  }

  get_field_info() {
    return this.field_info;
  }

  to_bytes() {
    const writer = new BinaryWriter({});
    writer.varInt32(this.field_info.length);
    writer.varInt32(this.type_id);
    for (const field of this.field_info) {
      writer.buffer(field.to_bytes());
    }
    return writer.dump();
  }

  static from_bytes(reader: BinaryReader) {
    const field_num = reader.varInt32();
    const type_id = reader.varInt32();
    const field_info = [];
    for (let i = 0; i < field_num; i++) {
      field_info.push(FieldInfo.from_bytes(reader));
    }
    return new TypeMetaLayer(type_id, field_info);
  }
}

export class TypeMeta {
  constructor(private hash: bigint, private layers: TypeMetaLayer[]) {
  }

  get_field_info() {
    return this.layers[0].get_field_info();
  }

  get_type_id() {
    return this.layers[0].get_type_id();
  }

  static from_fields(type_id: number, field_info: FieldInfo[]) {
    return new TypeMeta(BigInt(0), [new TypeMetaLayer(type_id, field_info)]);
  }

  static from_bytes(reader: BinaryReader) {
    const header = reader.uint64();
    const hash = header >> BigInt(8);
    const layer_count = header & BigInt(0b1111);
    const layers = [];
    for (let i = 0; i < layer_count; i++) {
      layers.push(TypeMetaLayer.from_bytes(reader));
    }
    return new TypeMeta(hash, layers);
  }

  to_bytes() {
    const writer = new BinaryWriter({});
    writer.uint64(BigInt((this.hash << BigInt(8)) | BigInt((this.layers.length & 0b1111))));
    for (const layer of this.layers) {
      writer.buffer(layer.to_bytes());
    }
    return writer.dump();
  }
}
