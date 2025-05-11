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

package org.apache.fury.meta;

import static org.testng.Assert.assertEquals;

import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.meta.ClassDefTest.TestFieldsOrderClass1;
import org.apache.fury.meta.ClassDefTest.TestFieldsOrderClass2;
import org.apache.fury.reflect.ReflectionUtils;
import org.testng.annotations.Test;

public class TypeDefTest extends FuryTestBase {

  @Test
  public void testClassDefSerialization() {
    Fury fury = builder().withLanguage(Language.XLANG).withMetaShare(true).build();
    fury.register(TestFieldsOrderClass1.class, "demo.Class1");
    ClassDef classDef = ClassDef.buildClassDef(fury, TestFieldsOrderClass1.class, true);
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    classDef.writeClassDef(buffer);
    ClassDef classDef1 = ClassDef.readClassDef(fury, buffer);
    assertEquals(classDef1.getClassName(), classDef.getClassName());
    assertEquals(classDef1, classDef);
  }

  @Test
  public void testClassDefInheritanceDuplicatedFields() {
    Fury fury = builder().withLanguage(Language.XLANG).withMetaShare(true).build();
    fury.register(TestFieldsOrderClass2.class, "demo.Class2");
    ClassDef classDef = ClassDef.buildClassDef(fury, TestFieldsOrderClass2.class);
    assertEquals(classDef.getClassName(), TestFieldsOrderClass2.class.getName());
    // xtype ignore duplicate fields from parent class.
    assertEquals(
        classDef.getFieldsInfo().size(),
        ReflectionUtils.getFields(TestFieldsOrderClass2.class, true).size() - 1);
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    classDef.writeClassDef(buffer);
    ClassDef classDef1 = ClassDef.readClassDef(fury, buffer);
    assertEquals(classDef1.getClassName(), classDef.getClassName());
    assertEquals(classDef1, classDef);
  }
}
