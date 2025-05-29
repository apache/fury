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

package org.apache.fory.test.bean;

import java.io.Serializable;
import java.util.Random;
import lombok.Data;
import org.codehaus.janino.SimpleCompiler;

/** Test struct for primitive fields. */
@Data
public class Foo implements Serializable {
  int f1;
  int f2;
  long f3;
  long f4;
  float f5;
  double f6;
  double f7;
  long f8;
  long f9;
  long f10;
  long f11;
  long f12;
  long f13;
  long f14;
  long f15;

  /** Create Object. */
  public static Foo create() {
    Random random = new Random(31);
    Foo foo = new Foo();
    foo.f1 = random.nextInt();
    foo.f2 = random.nextInt();
    foo.f3 = random.nextLong();
    foo.f4 = random.nextLong();
    foo.f5 = random.nextFloat();
    foo.f6 = random.nextDouble();
    foo.f7 = random.nextDouble();
    foo.f8 = random.nextLong();
    foo.f9 = random.nextLong();
    foo.f10 = random.nextLong();
    foo.f11 = random.nextLong();
    foo.f12 = random.nextLong();
    foo.f13 = random.nextLong();
    foo.f14 = random.nextLong();
    foo.f15 = random.nextLong();
    return foo;
  }

  /** Create Class. */
  public static Class<?> createCompatibleClass1() {
    String pkg = Foo.class.getPackage().getName();
    String code =
        ""
            + "package "
            + pkg
            + ";\n"
            + "public class Foo {\n"
            + "int f1;\n"
            + "  int f2;\n"
            + "  long f3;\n"
            + "  long f4;\n"
            + "  float f5;\n"
            + "}";
    return loadFooClass(pkg, code);
  }

  /** Create class. */
  public static Class<?> createCompatibleClass2() {
    String pkg = Foo.class.getPackage().getName();
    String code =
        ""
            + "package "
            + pkg
            + ";\n"
            + "public class Foo {\n"
            + "  long f13;\n"
            + "  long f14;\n"
            + "  long f15;\n"
            + "}";
    return loadFooClass(pkg, code);
  }

  /** Create class. */
  public static Class<?> createCompatibleClass3() {
    String pkg = Foo.class.getPackage().getName();
    String code =
        ""
            + "package "
            + pkg
            + ";\n"
            + "public class Foo {\n"
            + "  int f2;\n"
            + "  long f4;\n"
            + "  float f5;\n"
            + "  double f6;\n"
            + "  long f8;\n"
            + "  long f14;\n"
            + "}";
    return loadFooClass(pkg, code);
  }

  private static Class<?> loadFooClass(String pkg, String code) {
    SimpleCompiler compiler = new SimpleCompiler();
    compiler.setParentClassLoader(Foo.class.getClassLoader().getParent());
    try {
      compiler.cook(code);
      Class<?> cls = compiler.getClassLoader().loadClass(pkg + ".Foo");
      if (cls == Foo.class) {
        throw new RuntimeException();
      }
      return cls;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
