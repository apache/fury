/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.codegen;

import com.google.common.reflect.TypeToken;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CodegenContextTest {

  public static class A {
    public String f1;
  }

  @Test
  public void type() {
    TypeToken<List<List<String>>> typeToken = new TypeToken<List<List<String>>>() {};
    {
      CodegenContext ctx = new CodegenContext();
      ctx.addImport(List.class);
      Assert.assertEquals("List", ctx.type(List.class));
    }
    CodegenContext ctx = new CodegenContext();
    String type = ctx.type(typeToken);
    Assert.assertEquals("java.util.List", type);
    Assert.assertEquals("int[][]", ctx.type(int[][].class));
  }

  @Test
  public void testTypeForInnerClass() {
    CodegenContext ctx = new CodegenContext();
    Assert.assertEquals(ctx.type(A.class), A.class.getCanonicalName());
    ctx.addImport(getClass());
    Assert.assertEquals(ctx.type(A.class), A.class.getCanonicalName());
    ctx.addImport(A.class);
    Assert.assertEquals(ctx.type(A.class), A.class.getSimpleName());
  }

  @Test
  public void testNewName() {
    {
      CodegenContext ctx = new CodegenContext();
      Assert.assertEquals(ctx.newName("serializer"), "serializer");
      Assert.assertEquals(ctx.newName("serializer"), "serializer1");
      Assert.assertEquals(ctx.newName("serializer"), "serializer2");
    }
    {
      CodegenContext ctx = new CodegenContext();
      Assert.assertEquals(ctx.newName("serializer"), "serializer");
      // Assert.assertEquals(
      //     ctx.newNames(Serializer.class, "isNull"), new String[] {"serializer1", "isNull1"});
      Assert.assertEquals(ctx.newName("serializer"), "serializer1");
    }
    {
      CodegenContext ctx = new CodegenContext();
      Assert.assertEquals(ctx.newName("isNull"), "isNull");
      Assert.assertEquals(
          ctx.newNames("serializer", "isNull"), new String[] {"serializer1", "isNull1"});
      Assert.assertEquals(ctx.newName("serializer"), "serializer2");
    }
  }
}
