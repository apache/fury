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

package io.fury.exception;

import io.fury.config.CompatibleMode;

/**
 * Exception for class compatibility. If {@link CompatibleMode#COMPATIBLE} is not enabled, and the
 * class when serializing is different from deserialization, for example, the class add/delete some
 * fields, this exception will be thrown.
 *
 * @author chaokunyang
 */
public class ClassNotCompatibleException extends FuryException {
  public ClassNotCompatibleException(String message) {
    super(message);
  }

  public ClassNotCompatibleException(String message, Throwable cause) {
    super(message, cause);
  }
}
