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

package org.apache.fury.type;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.meta.ClassDef;
import org.apache.fury.util.ReflectionUtils;
import org.testng.annotations.Test;

public class ClassDefTest extends FuryTestBase {
  private static class TestFieldsOrderClass1 {
    private int intField2;
    private boolean booleanField;
    private Object objField;
    private long longField;
  }

  private static class TestFieldsOrderClass2 extends TestFieldsOrderClass1 {
    private int intField1;
    private boolean booleanField;
    private int childIntField2;
    private boolean childBoolField1;
    private byte childByteField;
    private short childShortField;
    private long childLongField;
  }

  private static class DuplicateFieldClass extends TestFieldsOrderClass1 {
    private int intField1;
    private boolean booleanField;
    private Object objField;
    private long longField;
  }

  private static class ContainerClass extends TestFieldsOrderClass1 {
    private int intField1;
    private long longField;
    private Collection<String> collection;
    private List<Integer> list1;
    private List<Object> list2;
    private List list3;
    private Map<String, Object> map1;
    private Map<String, Integer> map2;
    private Map map3;
  }

  @Test
  public void testFieldsOrder() {
    List<Field> fieldList = new ArrayList<>();
    Collections.addAll(fieldList, TestFieldsOrderClass1.class.getDeclaredFields());
    Collections.addAll(fieldList, TestFieldsOrderClass2.class.getDeclaredFields());
    TreeSet<Field> sorted = new TreeSet<>(ClassDef.FIELD_COMPARATOR);
    sorted.addAll(fieldList);
    assertEquals(fieldList.size(), sorted.size());
    fieldList.sort(ClassDef.FIELD_COMPARATOR);
  }

  @Test
  public void testClassDefSerialization() throws NoSuchFieldException {
    Fury fury = Fury.builder().withMetaContextShare(true).build();
    {
      ClassDef classDef =
          ClassDef.buildClassDef(
              fury.getClassResolver(),
              TestFieldsOrderClass1.class,
              ImmutableList.of(TestFieldsOrderClass1.class.getDeclaredField("longField")));
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
      classDef.writeClassDef(buffer);
      ClassDef classDef1 = ClassDef.readClassDef(buffer);
      assertEquals(classDef1.getClassName(), classDef.getClassName());
      assertEquals(classDef1, classDef);
    }
    {
      ClassDef classDef =
          ClassDef.buildClassDef(
              fury.getClassResolver(),
              TestFieldsOrderClass1.class,
              ReflectionUtils.getFields(TestFieldsOrderClass1.class, true));
      assertEquals(classDef.getClassName(), TestFieldsOrderClass1.class.getName());
      assertEquals(
          classDef.getFieldsInfo().size(),
          ReflectionUtils.getFields(TestFieldsOrderClass1.class, true).size());
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
      classDef.writeClassDef(buffer);
      ClassDef classDef1 = ClassDef.readClassDef(buffer);
      assertEquals(classDef1.getClassName(), classDef.getClassName());
      assertEquals(classDef1, classDef);
    }
    {
      ClassDef classDef =
          ClassDef.buildClassDef(
              fury.getClassResolver(),
              TestFieldsOrderClass2.class,
              ReflectionUtils.getFields(TestFieldsOrderClass2.class, true));
      assertEquals(classDef.getClassName(), TestFieldsOrderClass2.class.getName());
      assertEquals(
          classDef.getFieldsInfo().size(),
          ReflectionUtils.getFields(TestFieldsOrderClass2.class, true).size());
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
      classDef.writeClassDef(buffer);
      ClassDef classDef1 = ClassDef.readClassDef(buffer);
      assertEquals(classDef1.getClassName(), classDef.getClassName());
      assertEquals(classDef1, classDef);
    }
  }

  @Test
  public void testDuplicateFieldsClass() {
    Fury fury = Fury.builder().withMetaContextShare(true).build();
    {
      ClassDef classDef =
          ClassDef.buildClassDef(
              fury.getClassResolver(),
              DuplicateFieldClass.class,
              ReflectionUtils.getFields(DuplicateFieldClass.class, true));
      assertEquals(classDef.getClassName(), DuplicateFieldClass.class.getName());
      assertEquals(
          classDef.getFieldsInfo().size(),
          ReflectionUtils.getFields(DuplicateFieldClass.class, true).size());
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
      classDef.writeClassDef(buffer);
      ClassDef classDef1 = ClassDef.readClassDef(buffer);
      assertEquals(classDef1.getClassName(), classDef.getClassName());
      assertEquals(classDef1, classDef);
    }
  }

  @Test
  public void testContainerClass() {
    Fury fury = Fury.builder().withMetaContextShare(true).build();
    {
      ClassDef classDef =
          ClassDef.buildClassDef(
              fury.getClassResolver(),
              ContainerClass.class,
              ReflectionUtils.getFields(ContainerClass.class, true));
      assertEquals(classDef.getClassName(), ContainerClass.class.getName());
      assertEquals(
          classDef.getFieldsInfo().size(),
          ReflectionUtils.getFields(ContainerClass.class, true).size());
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
      classDef.writeClassDef(buffer);
      ClassDef classDef1 = ClassDef.readClassDef(buffer);
      assertEquals(classDef1.getClassName(), classDef.getClassName());
      assertEquals(classDef1, classDef);
    }
  }
}
