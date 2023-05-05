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

import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ByteArrayClassLoader;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.ClassLoaderIClassLoader;
import org.codehaus.janino.Compiler;
import org.codehaus.janino.ScriptEvaluator;
import org.codehaus.janino.SimpleCompiler;
import org.codehaus.janino.util.resource.MapResourceCreator;
import org.codehaus.janino.util.resource.MapResourceFinder;
import org.codehaus.janino.util.resource.Resource;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JaninoUtilsTest {

  @Test
  public void compile() throws Exception {
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
    ClassLoader classLoader =
        JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), unit1, unit2);
    Assert.assertEquals(
        "HELLO", classLoader.loadClass("demo.pkg1.A").getMethod("main").invoke(null));
  }

  // For 3.0.11: Total cost 98.523273 ms, average time is 1.970465 ms
  // For 3.1.2: Total cost 15863.328650 ms, average time is 317.266573 ms
  @Test
  public void benchmark() {
    CompileUnit unit =
        new CompileUnit(
            "demo.pkg1",
            "A",
            (""
                + "package demo.pkg1;\n"
                + "public class A {\n"
                + "  public static String hello() { return \"HELLO\"; }\n"
                + "}"));
    // Since janino is not called frequently, we test only 50 times.
    int iterNums = 50;
    for (int i = 0; i < iterNums; i++) {
      JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), unit);
    }
    long startTime = System.nanoTime();
    for (int i = 0; i < iterNums; i++) {
      JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), unit);
    }
    long duration = System.nanoTime() - startTime;
    System.out.printf(
        "Total cost %f ms, average time is %f ms",
        (double) duration / 1000_000, (double) duration / iterNums / 1000_000);
  }

  @Test
  public void testJaninoScript() throws CompileException, InvocationTargetException {
    ScriptEvaluator se = new ScriptEvaluator();
    se.cook(
        ""
            + "static void method1() {\n"
            + "  int i = 2;\n"
            + "  int j = i * 3;\n"
            + "}\n"
            + "\n"
            + "method1();\n"
            + "method2();\n"
            + "\n"
            + "static void method2() {\n"
            + "  int x = 3;\n"
            + "  int y = x * 3;\n"
            + "}\n");
    se.evaluate(new Object[0]);
  }

  @Test
  public void testJaninoClassBody()
      throws CompileException, IllegalAccessException, InstantiationException {
    ClassBodyEvaluator evaluator = new ClassBodyEvaluator();
    // default to context class loader. set if only use another class loader
    evaluator.setParentClassLoader(Thread.currentThread().getContextClassLoader());
    evaluator.setDefaultImports(List.class.getName());
    evaluator.setImplementedInterfaces(new Class[] {Function.class});
    String code =
        ""
            + "@Override\n"
            + "public Object apply(Object str) {\n"
            + "  return ((String)str).length();\n"
            + "}";
    evaluator.cook(code);
    Class<?> clazz = evaluator.getClazz();
    @SuppressWarnings("unchecked")
    Function<Object, Object> function = (Function<Object, Object>) clazz.newInstance();
    Assert.assertEquals(function.apply("test class body"), "test class body".length());
  }

  @Test
  public void testJaninoClass()
      throws CompileException, ClassNotFoundException, IllegalAccessException,
          InstantiationException {
    SimpleCompiler compiler = new SimpleCompiler();
    String code =
        ""
            + "import java.util.function.Function;\n"
            + "public class A implements Function {\n"
            + "  @Override\n"
            + "  public Object apply(Object o) {\n"
            + "    return o;\n"
            + "  }\n"
            + "}";
    // default to context class loader. set if only use another class loader
    compiler.setParentClassLoader(Thread.currentThread().getContextClassLoader());
    compiler.cook(code);
    Class<? extends Function> aCLass =
        compiler.getClassLoader().loadClass("A").asSubclass(Function.class);
    @SuppressWarnings("unchecked")
    Function<Object, Object> function = aCLass.newInstance();
    Assert.assertEquals(function.apply("test class"), "test class");
  }

  @Test
  public void testJaninoCompiler() throws Exception {
    WeakReference<? extends Class<?>> clsRef = new WeakReference<>(compileClassByJaninoCompiler());
    while (clsRef.get() != null) {
      System.gc();
      Thread.sleep(1000);
      System.out.printf("Wait cls %s gc.\n", clsRef.get());
    }
  }

  public Class<?> compileClassByJaninoCompiler() throws Exception {
    MapResourceFinder sourceFinder = new MapResourceFinder();
    String stubFileName = "A.java";
    String code =
        ""
            + "import java.util.function.Function;\n"
            + "public class A implements Function {\n"
            + "  @Override\n"
            + "  public Object apply(Object o) {\n"
            + "    return o;\n"
            + "  }\n"
            + "}";
    sourceFinder.addResource(stubFileName, code);
    // Storage for generated bytecode
    final Map<String, byte[]> classes = new HashMap<>();
    // Set up the compiler.
    ClassLoaderIClassLoader classLoader =
        new ClassLoaderIClassLoader(Thread.currentThread().getContextClassLoader());
    Compiler compiler = new Compiler(sourceFinder, classLoader);
    compiler.setClassFileCreator(new MapResourceCreator(classes));
    compiler.setClassFileFinder(new MapResourceFinder(classes));

    // set debug flag to get source file names and line numbers for debug and stacktrace.
    // this is also the default behaviour for javac.
    compiler.setDebugSource(true);
    compiler.setDebugLines(true);

    // Compile all sources
    compiler.compile(sourceFinder.resources().toArray(new Resource[0]));
    ByteArrayClassLoader byteArrayClassLoader =
        new ByteArrayClassLoader(classes, Thread.currentThread().getContextClassLoader());
    Class<?> cls = byteArrayClassLoader.loadClass("A");
    Assert.assertEquals(cls.getSimpleName(), "A");
    return cls;
  }

  @Test
  public void testGetClassStats() {
    CompileUnit unit =
        new CompileUnit(
            "demo.pkg1",
            "A",
            (""
                + "package demo.pkg1;\n"
                + "public class A {\n"
                + "  public static String hello() { return \"HELLO\"; }\n"
                + "}"));
    // Since janino is not called frequently, we test only 50 times.
    byte[] bytecodes =
        JaninoUtils.toBytecode(Thread.currentThread().getContextClassLoader(), unit)
            .values()
            .iterator()
            .next();
    JaninoUtils.CodeStats classStats = JaninoUtils.getClassStats(bytecodes);
    System.out.println(classStats);
    Assert.assertTrue(classStats.methodsSize.containsKey("hello"));
  }

  @Test(timeOut = 60000)
  public void testJaninoGeneratedClassGC() throws InterruptedException {
    WeakReference<Class<?>> clsRef = janinoGenerateClass();
    ;
    while (clsRef.get() != null) {
      System.gc();
      Thread.sleep(1000);
      System.out.printf("Wait cls %s gc.\n", clsRef.get());
    }
  }

  private WeakReference<Class<?>> janinoGenerateClass() {
    CompileUnit unit =
        new CompileUnit(
            "demo.pkg1",
            "A",
            (""
                + "package demo.pkg1;\n"
                + "public class A {\n"
                + "  public static String hello() { return \"HELLO\"; }\n"
                + "}"));
    ByteArrayClassLoader classLoader = JaninoUtils.compile(getClass().getClassLoader(), unit);
    try {
      Class<?> cls = classLoader.loadClass("demo.pkg1.A");
      return new WeakReference<>(cls);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
