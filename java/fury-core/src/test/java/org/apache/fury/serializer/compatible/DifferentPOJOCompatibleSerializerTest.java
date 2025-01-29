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

package org.apache.fury.serializer.compatible;

import org.apache.fury.Fury;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
import org.apache.fury.serializer.compatible.classes.ClassCompleteField;
import org.apache.fury.serializer.compatible.classes.ClassMissingField;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test COMPATIBILITY mode that supports - same field type and name can be deserialized to other
 * class with different name - scrambled field order to make sure it could handle different field
 * order - missing or extra field from source class to target class - generic class
 */
public class DifferentPOJOCompatibleSerializerTest extends Assert {

  Fury getFury(Class<?>... classes) {
    Fury instance =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withMetaShare(true)
            .withScopedMetaShare(true)
            .requireClassRegistration(false)
            .withAsyncCompilation(true)
            .serializeEnumByName(true)
            .build();
    if (classes != null) {
      for (Class<?> clazz : classes) {
        instance.register(clazz);
      }
    }
    ;
    return instance;
  }

  @Test
  void testTargetHasLessFieldComparedToSourceClass() throws InterruptedException {

    ClassCompleteField<String> subclass = new ClassCompleteField<>("subclass", "subclass2");
    ClassCompleteField<ClassCompleteField<String>> classCompleteField =
        new ClassCompleteField<>(subclass, subclass);
    byte[] serialized = getFury().serializeJavaObject(classCompleteField);
    ClassMissingField<ClassMissingField<String>> classMissingField =
        getFury().deserializeJavaObject(serialized, ClassMissingField.class);

    assertEq(classCompleteField, classMissingField);
  }

  @Test
  void testTargetHasMoreFieldComparedToSourceClass() throws InterruptedException {

    ClassMissingField<String> subclass = new ClassMissingField<>("subclass");
    ClassMissingField classMissingField = new ClassMissingField(subclass);
    byte[] serialized = getFury().serializeJavaObject(classMissingField);

    ClassCompleteField classCompleteField =
        getFury()
            .deserializeJavaObject(serialized, ClassCompleteField.class);

    assertEq(classCompleteField, classMissingField);
  }

  void assertEq(ClassCompleteField classCompleteField, ClassMissingField classMissingField) {
    assertEqSubClass(
        (ClassCompleteField) classCompleteField.getPrivateFieldSubClass(),
        (ClassMissingField) classMissingField.getPrivateFieldSubClass());
    assertEquals(classCompleteField.getPrivateMap(), classMissingField.getPrivateMap());
    assertEquals(classCompleteField.getPrivateList(), classMissingField.getPrivateList());
    assertEquals(classCompleteField.getPrivateString(), classMissingField.getPrivateString());
    assertEquals(classCompleteField.getPrivateInt(), classMissingField.getPrivateInt());
  }

  void assertEqSubClass(
      ClassCompleteField classCompleteField, ClassMissingField classMissingField) {
    assertEquals(
        classCompleteField.getPrivateFieldSubClass(), classMissingField.getPrivateFieldSubClass());
    assertEquals(classCompleteField.getPrivateMap(), classMissingField.getPrivateMap());
    assertEquals(classCompleteField.getPrivateList(), classMissingField.getPrivateList());
    assertEquals(classCompleteField.getPrivateString(), classMissingField.getPrivateString());
    assertEquals(classCompleteField.getPrivateInt(), classMissingField.getPrivateInt());
  }
}
