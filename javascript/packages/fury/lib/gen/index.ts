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

import { InternalSerializerType, Serializer } from "../type";
import { ArrayTypeInfo, MapTypeInfo, StructTypeInfo, OneofTypeInfo, SetTypeInfo, TupleTypeInfo, TypeInfo } from "../typeInfo";
import { CodegenRegistry } from "./router";
import { CodecBuilder } from "./builder";
import { Scope } from "./scope";
import "./array";
import "./struct";
import "./string";
import "./binary";
import "./bool";
import "./datetime";
import "./map";
import "./number";
import "./set";
import "./struct";
import "./tuple";
import "./typedArray";
import "./enum";
import Fury from "../fury";

export { AnySerializer } from "./any";

export class Gen {
  static external = CodegenRegistry.getExternal();

  constructor(private fury: Fury, private replace = false, private regOptions: { [key: string]: any } = {}) {

  }

  private generate(typeInfo: TypeInfo) {
    const InnerGeneratorClass = CodegenRegistry.get(typeInfo.type);
    if (!InnerGeneratorClass) {
      throw new Error(`${typeInfo.type} generator not exists`);
    }
    const scope = new Scope();
    const generator = new InnerGeneratorClass(typeInfo, new CodecBuilder(scope, this.fury), scope);

    const funcString = generator.toSerializer();
    if (this.fury.config && this.fury.config.hooks) {
      const afterCodeGenerated = this.fury.config.hooks.afterCodeGenerated;
      if (typeof afterCodeGenerated === "function") {
        return new Function(afterCodeGenerated(funcString));
      }
    }
    return new Function(funcString);
  }

  private register(typeInfo: StructTypeInfo, serializer?: Serializer) {
    this.fury.classResolver.registerSerializer(typeInfo, serializer);
  }

  private isRegistered(typeInfo: TypeInfo) {
    return !!this.fury.classResolver.typeInfoExists(typeInfo);
  }

  private traversalContainer(typeInfo: TypeInfo) {
    if (typeInfo.type === InternalSerializerType.STRUCT) {
      if (this.isRegistered(typeInfo) && !this.replace) {
        return;
      }
      const options = (<StructTypeInfo>typeInfo).options;
      if (options.props) {
        this.register(<StructTypeInfo>typeInfo);
        Object.values(options.props).forEach((x) => {
          this.traversalContainer(x);
        });
        const func = this.generate(typeInfo);
        this.register(<StructTypeInfo>typeInfo, func()(this.fury, Gen.external, typeInfo, this.regOptions));
      }
    }
    if (typeInfo.type === InternalSerializerType.ARRAY) {
      this.traversalContainer((<ArrayTypeInfo>typeInfo).options.inner);
    }
    if (typeInfo.type === InternalSerializerType.SET) {
      this.traversalContainer((<SetTypeInfo>typeInfo).options.key);
    }
    if (typeInfo.type === InternalSerializerType.MAP) {
      this.traversalContainer((<MapTypeInfo>typeInfo).options.key);
      this.traversalContainer((<MapTypeInfo>typeInfo).options.value);
    }
    if (typeInfo.type === InternalSerializerType.TUPLE) {
      (<TupleTypeInfo>typeInfo).options.inner.forEach((x) => {
        this.traversalContainer(x);
      });
    }
    if (typeInfo.type === InternalSerializerType.ONEOF) {
      const options = (<OneofTypeInfo>typeInfo).options;
      if (options.inner) {
        Object.values(options.inner).forEach((x) => {
          this.traversalContainer(x);
        });
      }
    }
  }

  generateSerializer(typeInfo: TypeInfo) {
    this.traversalContainer(typeInfo);
    const exists = this.isRegistered(typeInfo);
    if (exists) {
      return this.fury.classResolver.getSerializerByTypeInfo(typeInfo);
    }
    const func = this.generate(typeInfo);
    return func()(this.fury, Gen.external, typeInfo, this.regOptions);
  }
}
