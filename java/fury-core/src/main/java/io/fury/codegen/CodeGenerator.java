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

import io.fury.util.LoggerFactory;
import io.fury.util.StringUtils;
import org.slf4j.Logger;

/**
 * Code generator to generate class from {@link CompileUnit}.
 *
 * @author chaokunyang
 */
public class CodeGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(CodeGenerator.class);

  private static final String CODE_DIR_KEY = "FURY_CODE_DIR";
  private static final String DELETE_CODE_ON_EXIT_KEY = "FURY_DELETE_CODE_ON_EXIT";

  // This is the default value of HugeMethodLimit in the OpenJDK HotSpot JVM,
  // beyond which methods will be rejected from JIT compilation
  static final int DEFAULT_JVM_HUGE_METHOD_LIMIT = 8000;

  static final int DEFAULT_JVM_INLINE_METHOD_LIMIT = 325;

  // The max valid length of method parameters in JVM.
  static final int MAX_JVM_METHOD_PARAMS_LENGTH = 255;

  public static String getCodeDir() {
    return System.getProperty(CODE_DIR_KEY, System.getenv(CODE_DIR_KEY));
  }

  static boolean deleteCodeOnExit() {
    boolean deleteCodeOnExit = StringUtils.isBlank(getCodeDir());
    String deleteCodeOnExitStr =
        System.getProperty(DELETE_CODE_ON_EXIT_KEY, System.getenv(DELETE_CODE_ON_EXIT_KEY));
    if (deleteCodeOnExitStr != null) {
      deleteCodeOnExit = Boolean.parseBoolean(deleteCodeOnExitStr);
    }
    return deleteCodeOnExit;
  }
}
