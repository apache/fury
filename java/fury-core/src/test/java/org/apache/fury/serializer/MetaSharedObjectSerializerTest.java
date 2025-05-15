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

import org.apache.fury.FuryTestBase;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.codegen.JaninoUtils;
import org.apache.fury.config.CompatibleMode;
import org.testng.annotations.Test;

public class MetaSharedObjectSerializerTest extends FuryTestBase {

  @Test
  public void testIgnoreTypeInconsistentSerializer()
      throws InstantiationException, IllegalAccessException {
    String codeA =
        "public class TestA {"
            + "    private int a = 1;"
            + "    private Long b = 2L;"
            + "    private String c = \"test\";"
            + "    private int d;"
            + "}";

    String codeB =
        "public class TestA {"
            + "    private Integer a ;"
            + "    private int b = 30;"
            + "    private String c = \"test\";"
            + "    private String d;"
            + "}";

    Class<?> cls1 = JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestA", codeA);
    Class<?> cls2 = JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestA", codeB);
    ThreadSafeFury fury1 =
        builder()
            .withRefTracking(true)
            .requireClassRegistration(false)
            .withDeserializeNonexistentClass(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .deserializeNonexistentEnumValueAsNull(true)
            .withScopedMetaShare(true)
            .withCodegen(false)
            .withClassLoader(cls1.getClassLoader())
            .buildThreadSafeFury();
    ThreadSafeFury fury2 =
        builder()
            .withRefTracking(true)
            .requireClassRegistration(false)
            .withDeserializeNonexistentClass(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .deserializeNonexistentEnumValueAsNull(true)
            .withScopedMetaShare(true)
            .withCodegen(false)
            .withClassLoader(cls2.getClassLoader())
            .buildThreadSafeFury();
    Object data = cls1.newInstance();
    System.out.println(fury2.deserialize(fury1.serialize(data)));
  }
}
