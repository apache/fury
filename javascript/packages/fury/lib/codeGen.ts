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

import { InternalSerializerType, MaxInt32, RefFlags, Fury } from "./type";
import { replaceBackslashAndQuote, safePropAccessor, safePropName } from "./util";
import mapSerializer from "./internalSerializer/map";
import setSerializer from "./internalSerializer/set";
import { arraySerializer } from "./internalSerializer/array";
import { tupleSerializer } from "./internalSerializer/tuple";
import { ArrayTypeDescription, Cast, MapTypeDescription, ObjectTypeDescription, SetTypeDescription, TupleTypeDescription, TypeDescription } from "./description";
import { fromString } from "./platformBuffer";

function computeFieldHash(hash: number, id: number): number {
  let newHash = (hash) * 31 + (id);
  while (newHash >= MaxInt32) {
    newHash = Math.floor(newHash / 7);
  }
  return newHash;
}

const computeStringHash = (str: string) => {
  const bytes = fromString(str);
  let hash = 17;
  bytes.forEach((b) => {
    hash = hash * 31 + b;
    while (hash >= MaxInt32) {
      hash = Math.floor(hash / 7);
    }
  });
  return hash;
};

const computeStructHash = (description: TypeDescription) => {
  let hash = 17;
  for (const [, value] of Object.entries(Cast<ObjectTypeDescription>(description).options.props).sort()) {
    let id = value.type;
    if (value.type === InternalSerializerType.ARRAY || value.type === InternalSerializerType.MAP) {
      id = value.type; // TODO add map key&value type into schema hash
    } else if (value.type === InternalSerializerType.FURY_TYPE_TAG) {
      id = computeStringHash(Cast<ObjectTypeDescription>(value).options.tag);
    }
    hash = computeFieldHash(hash, id);
  }
  return hash;
};

function typeHandlerDeclaration(fury: Fury) {
  let declarations: string[] = [];
  let count = 0;
  const exists = new Map<string, string>();
  function addDeclar(name: string, declar: string, uniqueKey?: string) {
    const unique = uniqueKey || name;
    if (exists.has(unique)) {
      return exists.get(unique)!;
    }
    declarations.push(declar);
    exists.set(unique, name);
    return name;
  }

  const genBuiltinDeclaration = (type: number) => {
    const name = `type_${type}`.replace("-", "_");
    return addDeclar(name, `
    const ${name} = classResolver.getSerializerById(${type})`);
  };

  const genTagDeclaration = (tag: string) => {
    const name = `tag_${count++}`;
    return addDeclar(name, `
    const ${name} = classResolver.getSerializerByTag("${replaceBackslashAndQuote(tag)}")`, tag);
  };

  const genDeclaration = (description: TypeDescription): string => {
    if (description.type === InternalSerializerType.FURY_TYPE_TAG) {
      genTagSerializer(fury, Cast<ObjectTypeDescription>(description));
      return genTagDeclaration(Cast<ObjectTypeDescription>(description).options.tag);
    }
    if (description.type === InternalSerializerType.ARRAY) {
      const tupleOptions = Cast<TupleTypeDescription>(description).options;
      if (tupleOptions && tupleOptions.isTuple) {
        const names = [] as string[];
        Cast<TupleTypeDescription>(description).options.inner.forEach((v) => {
          names.push(genDeclaration(v));
        });

        const name = `tuple_${names.join("_")}`;
        return addDeclar(name, `
    const ${name} = tupleSerializer(fury, [${names.join(", ")}])`
        );
      }

      const inner = genDeclaration(Cast<ArrayTypeDescription>(description).options.inner);
      const name = `array_${inner}`;
      return addDeclar(name, `
    const ${name} = arraySerializer(fury, ${inner})`
      );
    }
    if (description.type === InternalSerializerType.FURY_SET) {
      const inner = genDeclaration(Cast<SetTypeDescription>(description).options.key);
      const name = `set_${inner}`;
      return addDeclar(name, `
    const ${name} = setSerializer(fury, ${inner})`
      );
    }
    if (description.type === InternalSerializerType.MAP) {
      const key = genDeclaration(Cast<MapTypeDescription>(description).options.key);
      const value = genDeclaration(Cast<MapTypeDescription>(description).options.value);

      const name = `map_${key}_${value}`;
      return addDeclar(name, `
    const ${name} = mapSerializer(fury, ${key}, ${value})`
      );
    }
    return genBuiltinDeclaration(description.type);
  };
  return {
    genDeclaration,
    finish() {
      const result = {
        declarations,
        names: [...exists.values()],
      };
      declarations = [];
      exists.clear();
      return result;
    },
  };
}

export const generateTagInlineCode = (fury: Fury, description: TypeDescription) => {
  const options = Cast<ObjectTypeDescription>(description).options;
  const tag = options?.tag;
  const { genDeclaration, finish } = typeHandlerDeclaration(fury);
  const expectHash = computeStructHash(description);
  const read = `
    // relation tag: ${tag}
    const result = {
        ${Object.entries(options.props).sort().map(([key]) => {
        return `${safePropName(key)}: null`;
    }).join(",\n")}
    };
    pushReadObject(result);
    ${Object.entries(options.props).sort().map(([key, value]) => {
        return `result${safePropAccessor(key)} = ${genDeclaration(value)}.read()`;
    }).join(";\n")
        }
    return result;
`;
  const write = Object.entries(options.props).sort().map(([key, value]) => {
    return `${genDeclaration(value)}.write(v${safePropAccessor(key)})`;
  }).join(";\n");
  const { names, declarations } = finish();
  const validTag = replaceBackslashAndQuote(tag);
  return new Function(
        `
return function (fury, scope) {
  const { referenceResolver, binaryWriter, classResolver, binaryReader } = fury;
  const { writeNullOrRef, pushReadObject } = referenceResolver;
  const { RefFlags, InternalSerializerType, arraySerializer, tupleSerializer, mapSerializer, setSerializer } = scope;
  ${declarations.join("")}
  const tagWriter = classResolver.createTagWriter("${validTag}");

  const reserves = ${names.map(x => `${x}.config().reserve`).join(" + ")};
  return {
    ...referenceResolver.deref(() => {
      const hash = binaryReader.int32();
      if (hash !== ${expectHash}) {
          throw new Error("validate hash failed: ${validTag}. expect ${expectHash}, but got" + hash);
      }
      {
          ${read}
      }
    }),
    write: referenceResolver.withNullableOrRefWriter(InternalSerializerType.FURY_TYPE_TAG, (v) => {
      tagWriter.write(binaryWriter);
      binaryWriter.int32(${expectHash});
      binaryWriter.reserve(reserves);
      ${write}
    }),
    config() {
      return {
        reserve: tagWriter.bufferLen + 8,
      }
    }
  }
}
`
  );
};

export const genTagSerializer = (fury: Fury, description: ObjectTypeDescription) => {
  const tag = description.options.tag;

  if (fury.classResolver.getSerializerByTag(tag)) {
    return fury.classResolver.getSerializerByTag(tag);
  }

  fury.classResolver.registerSerializerByTag(tag, fury.classResolver.getSerializerById(InternalSerializerType.ANY));

  const func = generateTagInlineCode(fury, description);
  return fury.classResolver.registerSerializerByTag(tag, func()(fury, {
    InternalSerializerType,
    RefFlags,
    arraySerializer,
    tupleSerializer,
    mapSerializer,
    setSerializer,
  }));
};

export const generateInlineCode = (fury: Fury, description: TypeDescription) => {
  const { genDeclaration, finish } = typeHandlerDeclaration(fury);

  const name = genDeclaration(description);
  const { declarations } = finish();
  return new Function(
        `
return function (fury, scope) {
  const { referenceResolver, binaryWriter, classResolver, binaryReader } = fury;
  const { writeNullOrRef, pushReadObject } = referenceResolver;
  const { RefFlags, InternalSerializerType, arraySerializer, tupleSerializer, mapSerializer, setSerializer } = scope;
${declarations.join("")}
  return ${name};
}
`
  );
};

export const genSerializer = (fury: Fury, description: TypeDescription) => {
  const func = generateInlineCode(fury, description);
  return func()(fury, {
    InternalSerializerType,
    RefFlags,
    arraySerializer,
    tupleSerializer,
    mapSerializer,
    setSerializer,
  });
};
