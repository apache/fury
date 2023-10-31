/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
 * Util for java exceptions.
 *
 * @author chaokunyang
 */
public class ExceptionUtils {
  private static final long detailMessageOffset =
      ReflectionUtils.getFieldOffset(Throwable.class, "detailMessage");

  /**
   * Try to set `StackOverflowError` exception message. Returns passed exception if set succeed, or
   * null if failed.
   */
  public static StackOverflowError trySetStackOverflowErrorMessage(
      StackOverflowError e, String message) {
    if (detailMessageOffset != 0) {
      Platform.putObject(e, detailMessageOffset, message);
      return e;
    } else {
      return null;
    }
  }
}
