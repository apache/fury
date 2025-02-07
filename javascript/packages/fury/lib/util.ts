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

import { Type, TypeDescription } from "./description";

export const isNodeEnv: boolean
  = typeof process !== "undefined"
  && process.versions != null
  && process.env.ECMA_ONLY !== "true"
  && process.versions.node != null;

export const hasBuffer = isNodeEnv && typeof Buffer !== "undefined";

export function isUint8Array(obj: any): obj is Uint8Array {
  return obj instanceof Uint8Array || Object.prototype.toString.call(obj) === "[object Uint8Array]";
}

export const data2Description = (
  data: any,
  tag: string,
): TypeDescription | null => {
  if (data === null || data === undefined) {
    return null;
  }
  if (Array.isArray(data)) {
    const item = data2Description(data[0], tag);
    if (!item) {
      throw new Error("empty array can't convert");
    }
    return {
      ...Type.array(item),
      label: "array",
    };
  }
  if (data instanceof Date) {
    return {
      ...Type.timestamp(),
      label: "timestamp",
    };
  }
  if (typeof data === "string") {
    return {
      ...Type.string(),
      label: "string",
    };
  }
  if (data instanceof Set) {
    return {
      ...Type.set(data2Description([...data.values()][0], tag)!),
      label: "set",
    };
  }
  if (data instanceof Map) {
    return {
      ...Type.map(
        data2Description([...data.keys()][0], tag)!,
        data2Description([...data.values()][0], tag)!,
      ),
      label: "map",
    };
  }
  if (typeof data === "boolean") {
    return {
      ...Type.bool(),
      label: "boolean",
    };
  }
  if (typeof data === "number") {
    if (data > Number.MAX_SAFE_INTEGER || data < Number.MIN_SAFE_INTEGER) {
      return {
        ...Type.int64(),
        label: "int64",
      };
    }
    return {
      ...Type.int32(),
      label: "int32",
    };
  }

  if (typeof data === "object") {
    if (isUint8Array(data)) {
      return Type.binary();
    }

    return Type.object(
      tag,
      Object.fromEntries(
        Object.entries(data)
          .map(([key, value]) => {
            return [key, data2Description(value, `${tag}.${key}`)];
          })
          .filter(([, v]) => Boolean(v)),
      ),
    );
  }

  throw new Error(`unkonw data type ${typeof data}`);
};
