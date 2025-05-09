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

package org.apache.fury.serializer.shim;

import org.apache.fury.exception.FuryException;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.util.ExceptionUtils;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ProtobufDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufDispatcher.class);

  private static Class<?> pbByteStringClass;
  private static Class<? extends Serializer> pbByteStringSerializerClass;
  private static Class<?> pbMessageClass;
  private static Class<? extends Serializer> pbMessageSerializerClass;

  static {
    try {
      pbMessageClass = ReflectionUtils.loadClass("com.google.protobuf.Message");
      pbByteStringClass = ReflectionUtils.loadClass("com.google.protobuf.ByteString");
    } catch (Exception e) {
      ExceptionUtils.ignore(e);
    }
    try {
      pbMessageSerializerClass =
          (Class<? extends Serializer>)
              ReflectionUtils.loadClass(
                  Serializer.class.getPackage().getName() + "." + "ProtobufSerializer");
      pbByteStringSerializerClass =
          (Class<? extends Serializer>)
              ReflectionUtils.loadClass(
                  Serializer.class.getPackage().getName() + "." + "ByteStringSerializer");
    } catch (Exception e) {
      ExceptionUtils.ignore(e);
      if (pbMessageClass != null) {
        LOG.warn("ProtobufSerializer not loaded, please add fury-extensions dependency.");
      }
    }
  }

  public static Class<? extends Serializer> getSerializerClass(Class<?> type) {
    if (pbMessageClass == null) {
      return null;
    }
    if (pbMessageSerializerClass == null) {
      throw new FuryException(
          "ProtobufSerializer can't be loaded, please add fury-extensions dependencies");
    }
    if (pbMessageClass.isAssignableFrom(type)) {
      return pbMessageSerializerClass;
    }
    if (pbByteStringClass.isAssignableFrom(type)) {
      return pbByteStringSerializerClass;
    }
    return null;
  }
}
