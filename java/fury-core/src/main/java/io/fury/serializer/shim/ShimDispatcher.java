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

package io.fury.serializer.shim;

import io.fury.Fury;
import io.fury.collection.FuryObjectMap;
import io.fury.collection.ObjectMap;
import io.fury.serializer.Serializer;
import io.fury.serializer.Serializers;
import io.fury.serializer.collection.CollectionSerializer;
import io.fury.serializer.collection.MapSerializers;
import io.fury.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A shim serializer dispatcher to resolve compatibility problems for common used classes.
 *
 * @author fengjian
 */
@SuppressWarnings("rawtypes")
public class ShimDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(ShimDispatcher.class);

  private final FuryObjectMap<String, Class<? extends Serializer>> className2ShimSerializerClass =
      new ObjectMap<>(4, 0.75f);

  private final Fury fury;

  public ShimDispatcher(Fury fury) {
    this.fury = fury;
  }

  public void initialize() {
    register("com.alibaba.fastjson.JSONObject", MapSerializers.StringKeyMapSerializer.class);
    register("com.alibaba.fastjson.JSONArray", CollectionSerializer.class);
  }

  public boolean contains(Class<?> clazz) {
    return className2ShimSerializerClass.containsKey(clazz.getName());
  }

  public void register(String className, Class<? extends Serializer> serializerClass) {
    Preconditions.checkArgument(className != null, "Class name cannot be null");
    Preconditions.checkArgument(serializerClass != null, "Serializer class cannot be null");

    if (className2ShimSerializerClass.containsKey(className)) {
      throw new IllegalArgumentException(
          "Class "
              + className
              + " has already been registered with serializer:"
              + serializerClass.getName());
    }

    className2ShimSerializerClass.put(className, serializerClass);
  }

  public Serializer<?> getSerializer(Class<?> clazz) {
    String className = clazz.getName();
    if (!className2ShimSerializerClass.containsKey(className)) {
      return null;
    }
    Class<? extends Serializer> serializerClass = className2ShimSerializerClass.get(className);
    try {
      return Serializers.newSerializer(fury, clazz, serializerClass);
    } catch (Throwable e) {
      LOG.warn(
          "Construct shim serializer failed for class [{}] with serializer class [{}]",
          className,
          serializerClass);
      return null;
    }
  }
}
