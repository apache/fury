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

package io.fury.util;

/**
 * Misc common utils.
 *
 * @author chaokunyang
 */
public class Utils {

  public static void ignore(Object... args) {}

  public static void checkArgument(boolean b, String errorMessage) {
    if (!b) {
      throw new IllegalArgumentException(errorMessage);
    }
  }

  /**
   * Ensures the truth of an expression involving one or more parameters to the calling method.
   * Workaround for guava before 20.0.
   *
   * <p>See {@link com.google.common.base.Preconditions#checkArgument(boolean, String, Object...)}
   * for details.
   */
  public static void checkArgument(
      boolean expression,
      String errorMessageTemplate,
      Object errorMessageArg0,
      Object... errorMessageArgs) {
    if (!expression) {
      Object[] args;
      if (errorMessageArgs != null) {
        args = new Object[errorMessageArgs.length + 1];
        args[0] = errorMessageArg0;
        System.arraycopy(errorMessageArgs, 0, args, 1, errorMessageArgs.length);
      } else {
        args = new Object[] {errorMessageArg0};
      }
      throw new IllegalArgumentException(String.format(errorMessageTemplate, args));
    }
  }
}
