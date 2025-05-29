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

package org.apache.fory.util.unsafe;

import java.util.Collections;
import org.apache.fory.codegen.CompileUnit;
import org.apache.fory.codegen.JaninoUtils;
import org.apache.fory.memory.Platform;
import org.apache.fory.util.ClassLoaderUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DefineClassTest {

  @Test
  public void testDefineClass() throws ClassNotFoundException {
    String pkg = DefineClassTest.class.getPackage().getName();
    CompileUnit unit =
        new CompileUnit(
            pkg,
            "A",
            ("package "
                + pkg
                + ";\n"
                + "public class A {\n"
                + "  public static String hello() { return \"HELLO\"; }\n"
                + "}"));
    byte[] bytecodes =
        JaninoUtils.toBytecode(Thread.currentThread().getContextClassLoader(), unit)
            .values()
            .iterator()
            .next();
    String className = pkg + ".A";
    ClassLoaderUtils.ByteArrayClassLoader loader =
        new ClassLoaderUtils.ByteArrayClassLoader(Collections.singletonMap(className, bytecodes));
    loader.loadClass(className);

    loader =
        new ClassLoaderUtils.ByteArrayClassLoader(Collections.singletonMap(className, bytecodes));
    DefineClass.defineClass(className, DefineClassTest.class, loader, null, bytecodes);
    Class<?> clz = loader.loadClass(className);
    if (Platform.JAVA_VERSION >= 9) {
      Assert.assertEquals(clz.getClassLoader(), DefineClassTest.class.getClassLoader());
      Assert.assertThrows(
          Exception.class,
          () ->
              DefineClass.defineClass(
                  className, null, DefineClassTest.class.getClassLoader(), null, bytecodes));
    } else {
      Assert.assertEquals(clz.getClassLoader(), loader);
      DefineClass.defineClass(
          className, null, DefineClassTest.class.getClassLoader(), null, bytecodes);
    }
  }
}
