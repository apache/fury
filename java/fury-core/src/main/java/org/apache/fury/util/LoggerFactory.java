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

package org.apache.fury.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

/**
 * A logger factory which can be used to disable fury logging more easily than configure logging.
 */
public class LoggerFactory {
  private static boolean disableLogging;

  public static void disableLogging() {
    disableLogging = true;
  }

  public static void enableLogging() {
    disableLogging = false;
  }

  public static Logger getLogger(Class<?> clazz) {
    if (disableLogging) {
      return NOPLogger.NOP_LOGGER;
    } else {
      if (GraalvmSupport.IN_GRAALVM_NATIVE_IMAGE) {
        return (Logger)
            Proxy.newProxyInstance(
                clazz.getClassLoader(), new Class[] {Logger.class}, new GraalvmLogger(clazz));
      }
      return org.slf4j.LoggerFactory.getLogger(clazz);
    }
  }

  private static final class GraalvmLogger implements InvocationHandler {
    private static final DateTimeFormatter dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
    private final Class<?> targetClass;

    private GraalvmLogger(Class<?> targetClass) {
      this.targetClass = targetClass;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      if (name.equals("info")) {
        log("INFO", false, args);
      }
      if (name.equals("warn")) {
        log("WARN", false, args);
      }
      if (name.equals("error")) {
        log("ERROR", true, args);
      }
      return null;
    }

    private void log(String level, boolean mayPrintTrace, Object[] args) {
      StringBuilder builder = new StringBuilder(dateTimeFormatter.format(LocalDateTime.now()));
      builder.append(" ").append(level);
      builder.append(" ").append(targetClass.getSimpleName());
      builder.append(" ").append(Thread.currentThread().getName());
      builder.append(" -");
      for (Object arg : args) {
        builder.append(" ").append(arg);
      }
      System.out.println(builder);
      int length = args.length;
      if (mayPrintTrace && length > 0) {
        Object o = args[length - 1];
        if (o instanceof Throwable) {
          ((Throwable) o).printStackTrace();
        }
      }
    }
  }
}
