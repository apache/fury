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

package org.apache.fury.resolver;

import java.util.function.Function;
import org.apache.fury.util.Preconditions;

/** Responsible for allocating ClassId and maintaining built-in ClassId. */
public class ClassIdAllocator {
  public static class BuiltinClassId {
    // preserve 0 as flag for class id not set in ClassInfo`
    public static final short NO_CLASS_ID = (short) 0;
    public static final short LAMBDA_STUB_ID = 1;
    public static final short JDK_PROXY_STUB_ID = 2;
    public static final short REPLACE_STUB_ID = 3;
    // Note: following pre-defined class id should be continuous, since they may be used based
    // range.
    public static final short PRIMITIVE_VOID_CLASS_ID = (short) (REPLACE_STUB_ID + 1);
    public static final short PRIMITIVE_BOOLEAN_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 1);
    public static final short PRIMITIVE_BYTE_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 2);
    public static final short PRIMITIVE_CHAR_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 3);
    public static final short PRIMITIVE_SHORT_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 4);
    public static final short PRIMITIVE_INT_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 5);
    public static final short PRIMITIVE_FLOAT_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 6);
    public static final short PRIMITIVE_LONG_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 7);
    public static final short PRIMITIVE_DOUBLE_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 8);
    public static final short VOID_CLASS_ID = (short) (PRIMITIVE_DOUBLE_CLASS_ID + 1);
    public static final short BOOLEAN_CLASS_ID = (short) (VOID_CLASS_ID + 1);
    public static final short BYTE_CLASS_ID = (short) (VOID_CLASS_ID + 2);
    public static final short CHAR_CLASS_ID = (short) (VOID_CLASS_ID + 3);
    public static final short SHORT_CLASS_ID = (short) (VOID_CLASS_ID + 4);
    public static final short INTEGER_CLASS_ID = (short) (VOID_CLASS_ID + 5);
    public static final short FLOAT_CLASS_ID = (short) (VOID_CLASS_ID + 6);
    public static final short LONG_CLASS_ID = (short) (VOID_CLASS_ID + 7);
    public static final short DOUBLE_CLASS_ID = (short) (VOID_CLASS_ID + 8);
    public static final short STRING_CLASS_ID = (short) (VOID_CLASS_ID + 9);
    public static final short PRIMITIVE_BOOLEAN_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 1);
    public static final short PRIMITIVE_BYTE_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 2);
    public static final short PRIMITIVE_CHAR_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 3);
    public static final short PRIMITIVE_SHORT_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 4);
    public static final short PRIMITIVE_INT_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 5);
    public static final short PRIMITIVE_FLOAT_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 6);
    public static final short PRIMITIVE_LONG_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 7);
    public static final short PRIMITIVE_DOUBLE_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 8);
    public static final short STRING_ARRAY_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 1);
    public static final short OBJECT_ARRAY_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 2);
    public static final short ARRAYLIST_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 3);
    public static final short HASHMAP_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 4);
    public static final short HASHSET_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 5);
    public static final short CLASS_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 6);
    public static final short EMPTY_OBJECT_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 7);
  }

  // class id of last default registered class.
  private short innerEndClassId;
  // Here we set it to 1 because `NO_CLASS_ID` is 0 to avoid calculating it again in
  // `register(Class<?> cls)`.
  private short classIdGenerator = 1;

  private final Function<Class<?>, Boolean> classRegisteredFactory;

  private final Function<Short, Boolean> classIdRegisteredFactory;

  public ClassIdAllocator(
      Function<Class<?>, Boolean> classRegisteredFactory,
      Function<Short, Boolean> classIdRegisteredFactory) {
    Preconditions.checkNotNull(classRegisteredFactory);
    Preconditions.checkNotNull(classIdRegisteredFactory);
    this.classRegisteredFactory = classRegisteredFactory;
    this.classIdRegisteredFactory = classIdRegisteredFactory;
  }

  public short allocateClassId(Class<?> cls) {
    if (!classRegisteredFactory.apply(cls)) {
      while (classIdRegisteredFactory.apply(classIdGenerator)) {
        classIdGenerator++;
      }
    }
    return classIdGenerator;
  }

  public void notifyRegistrationEnd() {
    classIdGenerator++;
  }

  public void markInternalRegistrationEnd() {
    innerEndClassId = classIdGenerator;
  }

  public boolean isInnerClass(Short classId) {
    return classId != null && classId != BuiltinClassId.NO_CLASS_ID && classId < innerEndClassId;
  }

  public boolean isPrimitive(short classId) {
    return classId >= BuiltinClassId.PRIMITIVE_VOID_CLASS_ID
        && classId <= BuiltinClassId.PRIMITIVE_DOUBLE_CLASS_ID;
  }
}
