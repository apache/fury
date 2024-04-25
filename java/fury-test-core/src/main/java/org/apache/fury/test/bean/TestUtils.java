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

package org.apache.fury.test.bean;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

public class TestUtils {
  public static String random(int size, int rand) {
    return random(size, new Random(rand));
  }

  public static String random(int size, Random random) {
    char[] chars = new char[size];
    char start = ' ';
    char end = 'z' + 1;
    int gap = end - start;
    for (int i = 0; i < size; i++) {
      chars[i] = (char) (start + random.nextInt(gap));
    }
    return new String(chars);
  }

  /** Create class. */
  public static Class<?> compile(String classname, String classCode) {
    try {
      Path dir = Files.createTempDirectory("compile");
      Path path = dir.resolve(classname + ".java");
      try {
        Files.deleteIfExists(path);
        Files.write(path, classCode.toString().getBytes());
        // Use JavaCompiler because janino doesn't support generics.
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        int result =
            compiler.run(
                null,
                System.out, // ignore output
                System.err, // ignore output
                "-classpath",
                System.getProperty("java.class.path"),
                path.toString());
        if (result != 0) {
          throw new RuntimeException(String.format("Couldn't compile code:\n %s.", classCode));
        }
        Class<?> clz =
            new URLClassLoader(new URL[] {dir.toUri().toURL()}, Struct.class.getClassLoader())
                .loadClass(classname);
        Files.delete(path);
        Files.delete(dir.resolve(classname + ".class"));
        return clz;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
