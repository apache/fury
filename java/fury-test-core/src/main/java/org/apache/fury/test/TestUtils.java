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

package org.apache.fury.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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

  public static boolean executeCommand(
      List<String> command, int waitTimeoutSeconds, Map<String, String> env) {
    try {
      System.out.println("Executing command: " + String.join(" ", command));
      // redirectOutput doesn't work for forked jvm such as in maven sure.
      ProcessBuilder processBuilder = new ProcessBuilder(command);
      for (Map.Entry<String, String> entry : env.entrySet()) {
        processBuilder.environment().put(entry.getKey(), entry.getValue());
      }
      Process process = processBuilder.start();
      // Capture output to log
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      BufferedReader errorReader =
          new BufferedReader(new InputStreamReader(process.getErrorStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
      }
      while ((line = errorReader.readLine()) != null) {
        System.err.println(line);
      }
      boolean finished = process.waitFor(waitTimeoutSeconds, TimeUnit.SECONDS);
      if (finished) {
        return process.exitValue() == 0;
      } else {
        process.destroy(); // ensure the process is terminated
        return false;
      }
    } catch (Exception e) {
      throw new RuntimeException("Error executing command " + String.join(" ", command), e);
    }
  }
}
