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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassDefEncoderTest {

  @Test
  public void testBasicClassDef() throws Exception {
    Fury fury = Fury.builder().withMetaContextShare(true).build();
    Class<ClassDefTest.TestFieldsOrderClass1> type = ClassDefTest.TestFieldsOrderClass1.class;
    List<ClassDef.FieldInfo> fieldsInfo =
        ClassDefEncoder.buildFieldsInfo(fury.getClassResolver(), type);
    Map<String, List<ClassDef.FieldInfo>> classLayers =
        ClassDefEncoder.getClassFields(type, fieldsInfo);
    MemoryBuffer buffer =
        ClassDefEncoder.encodeClassDef(fury.getClassResolver(), type, classLayers, new HashMap<>());
    ClassDef classDef = ClassDef.readClassDef(fury.getClassResolver(), buffer);
    Assert.assertEquals(classDef.getClassName(), type.getName());
    Assert.assertEquals(classDef.getFieldsInfo().size(), type.getDeclaredFields().length);
    Assert.assertEquals(classDef.getFieldsInfo(), fieldsInfo);
  }
}
