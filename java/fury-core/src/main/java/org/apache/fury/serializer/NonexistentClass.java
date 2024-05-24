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

package org.apache.fury.serializer;

import org.apache.fury.collection.LazyMap;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Config;
import org.apache.fury.meta.ClassDef;

/**
 * A class for hold deserialization data when the class doesn't exist in this process. When {@link
 * CompatibleMode#COMPATIBLE} is enabled
 *
 * @see Config#shareMetaContext()
 */
public interface NonexistentClass {
  enum NonexistentEnumClass implements NonexistentClass {}

  /** Ensure no fields here to avoid conflicts with peer class fields. */
  class NonexistentSkipClass implements NonexistentClass {}

  class NonexistentMetaSharedClass extends LazyMap implements NonexistentClass {
    final ClassDef classDef;

    public NonexistentMetaSharedClass(ClassDef classDef) {
      this.classDef = classDef;
    }
  }

  class NonexistentArrayClass implements NonexistentClass {}

  class NonexistentEnumArrayClass implements NonexistentClass {}

  Class<?> NonexistentEnum1DArray = NonexistentEnumClass[].class;
  Class<?> NonexistentEnum2DArray = NonexistentEnumClass[][].class;
  Class<?> NonexistentEnum3DArray = NonexistentEnumClass[][][].class;
  Class<?> NonexistentSkip1DArray = NonexistentSkipClass[].class;
  Class<?> NonexistentSkip2DArray = NonexistentSkipClass[][].class;
  Class<?> NonexistentSkip3DArray = NonexistentSkipClass[][][].class;
  Class<?> Nonexistent1DArray = NonexistentMetaSharedClass[].class;
  Class<?> Nonexistent2DArray = NonexistentMetaSharedClass[][].class;
  Class<?> Nonexistent3DArray = NonexistentMetaSharedClass[][][].class;

  static Class<?> getUnexistentClass(boolean isEnum, int arrayDims, boolean shareMeta) {
    return getUnexistentClass("Unknown", isEnum, arrayDims, shareMeta);
  }

  static Class<?> getUnexistentClass(
      String className, boolean isEnum, int arrayDims, boolean shareMeta) {
    if (arrayDims != 0) {
      if (isEnum) {
        switch (arrayDims) {
          case 1:
            return NonexistentEnum1DArray;
          case 2:
            return NonexistentEnum2DArray;
          case 3:
            return NonexistentEnum3DArray;
          default:
            throw new UnsupportedOperationException(
                String.format(
                    "Unsupported array dimensions %s for nonexistent class %s",
                    arrayDims, className));
        }
      } else {
        switch (arrayDims) {
          case 1:
            return shareMeta ? Nonexistent1DArray : NonexistentSkip1DArray;
          case 2:
            return shareMeta ? Nonexistent2DArray : NonexistentSkip2DArray;
          case 3:
            return shareMeta ? Nonexistent3DArray : NonexistentSkip3DArray;
          default:
            throw new UnsupportedOperationException(
                String.format(
                    "Unsupported array dimensions %s for nonexistent class %s",
                    arrayDims, className));
        }
      }
    } else if (isEnum) {
      return NonexistentEnumClass.class;
    } else {
      return shareMeta ? NonexistentMetaSharedClass.class : NonexistentSkipClass.class;
    }
  }
}
