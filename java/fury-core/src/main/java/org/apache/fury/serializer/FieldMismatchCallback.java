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

import java.lang.reflect.Field;

/** Callback interface for handling field mismatch during deserialization. */
public interface FieldMismatchCallback {

  /**
   * Called when a field mismatch is detected during deserialization.
   *
   * @param modifiedClass The class that is being deserialized
   * @param deserializedTypeName The name of the type that was deserialized
   * @param deserializedFieldName The name of the field that was deserialized
   * @return A FieldAdjustment that contains the target Field and a method to map the deserialized
   *     value to the target field.
   */
  FieldAdjustment onMismatch(
      Class<?> modifiedClass, String deserializedTypeName, String deserializedFieldName);

  abstract class FieldAdjustment {
    private final Field targetField;

    public FieldAdjustment(Field targetField) {
      this.targetField = targetField;
    }

    public Field getTargetField() {
      return targetField;
    }

    public abstract Object adjustValue(Object deserializedValue);
  }
}
