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
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.SerializerFactory;

public abstract class AbstractThreadSafeFury implements ThreadSafeFury {
  @Override
  public void register(Class<?> clz) {
    processCallback(fury -> fury.register(clz));
  }

  @Override
  public void register(Class<?> cls, boolean createSerializer) {
    processCallback(fury -> fury.register(cls, createSerializer));
  }

  @Override
  public void register(Class<?> cls, Short id) {
    processCallback(fury -> fury.register(cls, id));
  }

  @Override
  public void register(Class<?> cls, Short id, boolean createSerializer) {
    processCallback(fury -> fury.register(cls, id, createSerializer));
  }

  @Override
  public <T> void registerSerializer(Class<T> type, Class<? extends Serializer> serializerClass) {
    processCallback(fury -> fury.registerSerializer(type, serializerClass));
  }

  @Override
  public void registerSerializer(Class<?> type, Serializer<?> serializer) {
    processCallback(fury -> fury.registerSerializer(type, serializer));
  }

  @Override
  public void setSerializerFactory(SerializerFactory serializerFactory) {
    processCallback(fury -> fury.setSerializerFactory(serializerFactory));
  }

  protected abstract void processCallback(Consumer<Fury> callback);
}
