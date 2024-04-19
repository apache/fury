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

package org.apache.fury.builder;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.meta.ClassDef;
import org.apache.fury.serializer.CompatibleSerializerBase;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;

/**
 * Since janino doesn't support generics, we use {@link Object} to represent object type rather
 * generic type.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public interface Generated {

  /** Base class for all generated serializers. */
  abstract class GeneratedSerializer extends Serializer implements Generated {
    public GeneratedSerializer(Fury fury, Class<?> cls) {
      super(fury, cls);
    }

    /**
     * Register notify callback to update final field type serializers if it's jit-able.
     *
     * @param serializerFieldInfos fields name and types whose field value will be updated when jit
     *     succeed fot these field types.
     * @see BaseObjectCodecBuilder#getOrCreateSerializer(Class)
     * @see BaseObjectCodecBuilder#genCode
     */
    // Invoked in jit constructor.
    public void registerJITNotifyCallback(
        Serializer subclassSerializer, Object... serializerFieldInfos) {
      if (serializerFieldInfos.length > 0) {
        Map<String, Field> fieldsMap = new HashMap<>(serializerFieldInfos.length / 2);
        for (Field field : getClass().getDeclaredFields()) {
          if (field.getType() == Serializer.class) {
            fieldsMap.put(field.getName(), field);
          }
        }
        for (int i = 0; i < serializerFieldInfos.length; i += 2) {
          String serializerFieldName = (String) serializerFieldInfos[i];
          Class<?> beanFieldType = (Class<?>) serializerFieldInfos[i + 1];
          Field field = Objects.requireNonNull(fieldsMap.get(serializerFieldName));
          fury.getJITContext()
              .registerJITNotifyCallback(
                  beanFieldType,
                  new JITContext.NotifyCallback() {
                    @Override
                    public void onNotifyResult(Object result) {
                      Serializer<?> fieldSerializer =
                          fury.getClassResolver().getSerializer(beanFieldType);
                      Preconditions.checkState(beanFieldType == fieldSerializer.getType());
                      Preconditions.checkState(result == fieldSerializer.getClass());
                      ReflectionUtils.setObjectFieldValue(
                          subclassSerializer, field, fieldSerializer);
                    }

                    @Override
                    public void onNotifyMissed() {
                      Serializer<?> fieldSerializer =
                          fury.getClassResolver().getSerializer(beanFieldType);
                      ReflectionUtils.setObjectFieldValue(
                          subclassSerializer, field, fieldSerializer);
                    }
                  });
        }
      }
    }
  }

  /** Base class for all type consist serializers. */
  abstract class GeneratedObjectSerializer extends GeneratedSerializer implements Generated {
    public GeneratedObjectSerializer(Fury fury, Class<?> cls) {
      super(fury, cls);
    }
  }

  /** Base class for all serializers with meta shared by {@link ClassDef}. */
  abstract class GeneratedMetaSharedSerializer extends GeneratedSerializer implements Generated {
    public static final String SERIALIZER_FIELD_NAME = "serializer";

    /** Will be set in generated constructor by {@link MetaSharedCodecBuilder}. */
    public Serializer serializer;

    public GeneratedMetaSharedSerializer(Fury fury, Class<?> cls) {
      super(fury, cls);
    }

    @Override
    public void write(MemoryBuffer buffer, Object value) {
      serializer.write(buffer, value);
    }
  }

  /** Base class for all compatible serializers. */
  abstract class GeneratedCompatibleSerializer extends CompatibleSerializerBase
      implements Generated {

    public GeneratedCompatibleSerializer(Fury fury, Class<?> type) {
      super(fury, type);
    }
  }
}
