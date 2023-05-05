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

import io.fury.test.bean.Foo;
import io.fury.util.ClassLoaderUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CodeGeneratorTest {

  @Test
  public void classFilepath() {
    String p =
        CodeGenerator.classFilepath(
            new CompileUnit(Foo.class.getPackage().getName(), Foo.class.getSimpleName(), ""));
    Assert.assertEquals(
        p,
        String.format(
            "%s/%s.class",
            Foo.class.getPackage().getName().replace(".", "/"), Foo.class.getSimpleName()));
  }

  @Test
  public void fullClassName() {
    CompileUnit unit =
        new CompileUnit(Foo.class.getPackage().getName(), Foo.class.getSimpleName(), "");
    Assert.assertEquals(CodeGenerator.fullClassName(unit), Foo.class.getName());
  }

  @Test
  public void testCompile() throws Exception {
    CodeGenerator codeGenerator = CodeGenerator.getSharedCodeGenerator(getClass().getClassLoader());
    CompileUnit unit1 =
        new CompileUnit(
            "demo.pkg1",
            "A",
            (""
                + "package demo.pkg1;\n"
                + "public class A {\n"
                + "  public static String hello() { return \"HELLO\"; }\n"
                + "}"));
    ClassLoader classLoader = codeGenerator.compile(unit1);
    Assert.assertEquals(classLoader.loadClass("demo.pkg1.A").getSimpleName(), "A");
    Assert.assertNotEquals(classLoader, getClass().getClassLoader());
    Assert.assertEquals(classLoader.getClass(), ClassLoaderUtils.ByteArrayClassLoader.class);
  }

  @Test
  public void testMultiCompile() throws Exception {
    CodeGenerator codeGenerator = new CodeGenerator(getClass().getClassLoader());
    CompileUnit unit1 =
        new CompileUnit(
            "demo.pkg1",
            "A",
            (""
                + "package demo.pkg1;\n"
                + "import demo.pkg2.*;\n"
                + "public class A {\n"
                + "  public static String main() { return B.hello(); }\n"
                + "  public static String hello() { return \"HELLO\"; }\n"
                + "}"));
    CompileUnit unit2 =
        new CompileUnit(
            "demo.pkg2",
            "B",
            (""
                + "package demo.pkg2;\n"
                + "import demo.pkg1.*;\n"
                + "public class B {\n"
                + "  public static String hello() { return A.hello(); }\n"
                + "}"));
    ClassLoader classLoader = codeGenerator.compile(unit1, unit2);
    Assert.assertEquals(
        "HELLO", classLoader.loadClass("demo.pkg1.A").getMethod("main").invoke(null));
    ClassLoader classLoader2 = codeGenerator.compile(unit1, unit2);
    Assert.assertSame(classLoader, classLoader2);
  }
}
