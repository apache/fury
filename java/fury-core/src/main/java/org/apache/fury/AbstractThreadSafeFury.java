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

package org.apache.fury;

import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.fury.annotation.Internal;
import org.apache.fury.resolver.ClassChecker;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.SerializerFactory;

public abstract class AbstractThreadSafeFury implements ThreadSafeFury {
  @Override
  public void register(Class<?> clz) {
    registerCallback(fury -> fury.register(clz));
  }

  @Override
  public void register(Class<?> cls, boolean createSerializer) {
    registerCallback(fury -> fury.register(cls, createSerializer));
  }

  @Override
  public void register(Class<?> cls, int id) {
    registerCallback(fury -> fury.register(cls, id));
  }

  @Override
  public void register(Class<?> cls, int id, boolean createSerializer) {
    registerCallback(fury -> fury.register(cls, id, createSerializer));
  }

  @Override
  public void register(Class<?> cls, String typeName) {
    registerCallback(fury -> fury.register(cls, typeName));
  }

  @Override
  public void register(Class<?> cls, String namespace, String typeName) {
    registerCallback(fury -> fury.register(cls, namespace, typeName));
  }

  @Override
  public <T> void registerSerializer(Class<T> type, Class<? extends Serializer> serializerClass) {
    registerCallback(fury -> fury.registerSerializer(type, serializerClass));
  }

  @Override
  public void registerSerializer(Class<?> type, Serializer<?> serializer) {
    registerCallback(fury -> fury.registerSerializer(type, serializer));
  }

  @Override
  public void registerSerializer(Class<?> type, Function<Fury, Serializer<?>> serializerCreator) {
    registerCallback(fury -> fury.registerSerializer(type, serializerCreator.apply(fury)));
  }

  @Override
  public void setSerializerFactory(SerializerFactory serializerFactory) {
    registerCallback(fury -> fury.setSerializerFactory(serializerFactory));
  }

  @Override
  public void setClassChecker(ClassChecker classChecker) {
    registerCallback(fury -> fury.getClassResolver().setClassChecker(classChecker));
  }

  @Internal
  public abstract void registerCallback(Consumer<Fury> callback);
}
