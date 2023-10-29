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

package io.fury.serializer;

import static io.fury.util.Utils.checkArgument;

import io.fury.Fury;
import io.fury.builder.CodecUtils;
import io.fury.builder.Generated;
import io.fury.memory.MemoryBuffer;
import java.lang.reflect.Modifier;

/**
 * Util for JIT Serialization.
 *
 * @author chaokunyang
 */
@SuppressWarnings("UnstableApiUsage")
public final class CodegenSerializer {

  public static boolean supportCodegenForJavaSerialization(Class<?> cls) {
    // bean class can be static nested class, but can't be a non-static inner class
    // If a class is a static class, the enclosing class must not be null.
    // If enclosing class is null, it must not be a static class.
    return cls.getEnclosingClass() == null || Modifier.isStatic(cls.getModifiers());
  }

  @SuppressWarnings("unchecked")
  public static <T> Class<Serializer<T>> loadCodegenSerializer(Fury fury, Class<T> cls) {
    try {
      return (Class<Serializer<T>>) CodecUtils.loadOrGenObjectCodecClass(cls, fury);
    } catch (Exception e) {
      String msg = String.format("Create sequential serializer failed, \nclass: %s", cls);
      throw new RuntimeException(msg, e);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> Class<Serializer<T>> loadCompatibleCodegenSerializer(Fury fury, Class<T> cls) {
    try {
      return (Class<Serializer<T>>) CodecUtils.loadOrGenCompatibleCodecClass(cls, fury);
    } catch (Exception e) {
      String msg = String.format("Create compatible serializer failed, \nclass: %s", cls);
      throw new RuntimeException(msg, e);
    }
  }

  /**
   * A bean serializer which initializes lazily on first call read/write method.
   *
   * <p>This class is used by {@link io.fury.builder.BaseObjectCodecBuilder} to avoid potential
   * recursive bean serializer creation when there is a circular reference in class children fields.
   */
  public static final class LazyInitBeanSerializer<T> extends Serializer<T> {
    private Serializer<T> serializer;
    private Serializer<T> interpreterSerializer;

    public LazyInitBeanSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      getOrCreateGeneratedSerializer().write(buffer, value);
    }

    @Override
    public T read(MemoryBuffer buffer) {
      return getOrCreateGeneratedSerializer().read(buffer);
    }

    @SuppressWarnings({"rawtypes"})
    private Serializer<T> getOrCreateGeneratedSerializer() {
      if (serializer == null) {
        Serializer<T> jitSerializer = fury.getClassResolver().getSerializer(type);
        // Just be defensive for `getSerializer`/other call in Codec Builder to make
        // LazyInitBeanSerializer as serializer for `type`.
        if (jitSerializer instanceof LazyInitBeanSerializer) {
          // jit not finished, avoid recursive call this serializer.
          if (interpreterSerializer != null) {
            return interpreterSerializer;
          }
          if (fury.getConfig().isAsyncCompilationEnabled()) {
            // jit not finished, avoid recursive call current serializer.
            Class<? extends Serializer> sc =
                fury.getClassResolver().getSerializerClass(type, false);
            return interpreterSerializer = Serializers.newSerializer(fury, type, sc);
          } else {
            Class<? extends Serializer> sc = fury.getClassResolver().getSerializerClass(type);
            checkArgument(
                Generated.GeneratedSerializer.class.isAssignableFrom(sc),
                "Expect jit serializer but got %s for class %s",
                sc,
                type);
            serializer = Serializers.newSerializer(fury, type, sc);
            fury.getClassResolver().setSerializer(type, serializer);
            return serializer;
          }
        } else {
          serializer = jitSerializer;
        }
      }
      return serializer;
    }
  }
}
