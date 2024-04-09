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

package org.apache.fury.logging;

import org.apache.fury.util.GraalvmSupport;

/**
 * A logger factory which can be used to disable fury logging more easily than configure logging.
 */
public class LoggerFactory {
  private static volatile boolean disableLogging;
  private static volatile boolean useSlf4jlogger;

  public static void disableLogging() {
    disableLogging = true;
  }

  public static void enableLogging() {
    disableLogging = false;
  }

  public static void useSlf4jLogging(boolean useSlf4jLogging) {
    LoggerFactory.useSlf4jlogger = useSlf4jLogging;
  }

  public static Logger getLogger(Class<?> clazz) {
    if (disableLogging) {
      return new NilLogger();
    } else {
      if (GraalvmSupport.IN_GRAALVM_NATIVE_IMAGE) {
        return new FuryLogger(clazz);
      }
      if (useSlf4jlogger) {
        return createSlf4jLogger(clazz);
      } else {
        return new FuryLogger(clazz);
      }
    }
  }

  private static Logger createSlf4jLogger(Class<?> clazz) {
    return new Slf4jLogger(clazz);
  }
}
