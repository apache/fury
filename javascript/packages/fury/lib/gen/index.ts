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

import { InternalSerializerType, Fury } from "../type";
import { ArrayTypeDescription, MapTypeDescription, ObjectTypeDescription, SetTypeDescription, TupleTypeDescription, TypeDescription } from "../description";
import { Register } from "./router";
import { Builder } from "./builder";
import { Scope } from "./scope";
import "./array";
import "./object";
import "./string";
import "./binary";
import "./bool";
import "./datetime";
import "./map";
import "./number";
import "./set";
import "./any";
import "./tuple";
import "./typedArray";

export const generate = (fury: Fury, description: TypeDescription) => {
  const InnerGeneratorClass = Register.get(description.type);
  if (!InnerGeneratorClass) {
    throw new Error(`${description.type} generator not exists`);
  }
  const scope = new Scope();
  const generator = new InnerGeneratorClass(description, new Builder(scope, fury), scope);

  const funcString = generator.toSerializer();
  return new Function(funcString);
};

function regDependences(fury: Fury, description: TypeDescription) {
  if (description.type === InternalSerializerType.FURY_TYPE_TAG) {
    const options = (<ObjectTypeDescription>description).options;
    if (options.props) {
      fury.classResolver.registerSerializerByTag(options.tag);
      Object.values(options.props).forEach((x) => {
        regDependences(fury, x);
      });
      const func = generate(fury, description);
      fury.classResolver.registerSerializerByTag(options.tag, func()(fury, {}));
    }
  }
  if (description.type === InternalSerializerType.ARRAY) {
    regDependences(fury, (<ArrayTypeDescription>description).options.inner);
  }
  if (description.type === InternalSerializerType.FURY_SET) {
    regDependences(fury, (<SetTypeDescription>description).options.key);
  }
  if (description.type === InternalSerializerType.MAP) {
    regDependences(fury, (<MapTypeDescription>description).options.key);
    regDependences(fury, (<MapTypeDescription>description).options.value);
  }
  if (description.type === InternalSerializerType.TUPLE) {
    (<TupleTypeDescription>description).options.inner.forEach((x) => {
      regDependences(fury, x);
    });
  }
}

export const generateSerializer = (fury: Fury, description: TypeDescription) => {
  regDependences(fury, description);
  if (description.type === InternalSerializerType.FURY_TYPE_TAG) {
    return fury.classResolver.getSerializerByTag((<ObjectTypeDescription>description).options.tag);
  }
  const func = generate(fury, description);
  return func()(fury, {});
};
