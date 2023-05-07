/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.resolver;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Primitives;
import io.fury.Fury;
import io.fury.Language;
import io.fury.annotation.Internal;
import io.fury.collection.IdentityMap;
import io.fury.collection.LongMap;
import io.fury.collection.ObjectMap;
import io.fury.exception.InsecureException;
import io.fury.memory.MemoryBuffer;
import io.fury.serializer.Serializer;
import io.fury.serializer.SerializerFactory;
import io.fury.serializer.Serializers;
import io.fury.type.TypeUtils;
import io.fury.util.Functions;
import io.fury.util.LoggerFactory;
import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;
import io.fury.util.StringUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;

/**
 * Class registry for types of serializing objects, responsible for reading/writing types, setting
 * up relations between serializer and types.
 *
 * @author chaokunyang
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClassResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ClassResolver.class);

  public static final byte USE_CLASS_VALUE = 0;
  public static final byte USE_STRING_ID = 1;
  // preserve 0 as flag for class id not set in ClassInfo`
  public static final short NO_CLASS_ID = (short) 0;
  public static final short LAMBDA_STUB_ID = 1;
  public static final short JDK_PROXY_STUB_ID = 2;
  public static final short REPLACE_STUB_ID = 3;
  // Note: following pre-defined class id should be continuous, since they may be used based range.
  public static final short PRIMITIVE_VOID_CLASS_ID = (short) (REPLACE_STUB_ID + 1);
  public static final short PRIMITIVE_BOOLEAN_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 1);
  public static final short PRIMITIVE_BYTE_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 2);
  public static final short PRIMITIVE_CHAR_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 3);
  public static final short PRIMITIVE_SHORT_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 4);
  public static final short PRIMITIVE_INT_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 5);
  public static final short PRIMITIVE_FLOAT_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 6);
  public static final short PRIMITIVE_LONG_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 7);
  public static final short PRIMITIVE_DOUBLE_CLASS_ID = (short) (PRIMITIVE_VOID_CLASS_ID + 8);
  public static final short VOID_CLASS_ID = (short) (PRIMITIVE_DOUBLE_CLASS_ID + 1);
  public static final short BOOLEAN_CLASS_ID = (short) (VOID_CLASS_ID + 1);
  public static final short BYTE_CLASS_ID = (short) (VOID_CLASS_ID + 2);
  public static final short CHAR_CLASS_ID = (short) (VOID_CLASS_ID + 3);
  public static final short SHORT_CLASS_ID = (short) (VOID_CLASS_ID + 4);
  public static final short INTEGER_CLASS_ID = (short) (VOID_CLASS_ID + 5);
  public static final short FLOAT_CLASS_ID = (short) (VOID_CLASS_ID + 6);
  public static final short LONG_CLASS_ID = (short) (VOID_CLASS_ID + 7);
  public static final short DOUBLE_CLASS_ID = (short) (VOID_CLASS_ID + 8);
  public static final short STRING_CLASS_ID = (short) (VOID_CLASS_ID + 9);
  public static final short PRIMITIVE_BOOLEAN_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 1);
  public static final short PRIMITIVE_BYTE_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 2);
  public static final short PRIMITIVE_CHAR_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 3);
  public static final short PRIMITIVE_SHORT_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 4);
  public static final short PRIMITIVE_INT_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 5);
  public static final short PRIMITIVE_FLOAT_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 6);
  public static final short PRIMITIVE_LONG_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 7);
  public static final short PRIMITIVE_DOUBLE_ARRAY_CLASS_ID = (short) (STRING_CLASS_ID + 8);
  public static final short STRING_ARRAY_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 1);
  public static final short OBJECT_ARRAY_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 2);
  public static final short ARRAYLIST_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 3);
  public static final short HASHMAP_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 4);
  public static final short HASHSET_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 5);
  public static final short CLASS_CLASS_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 6);
  private static final int initialCapacity = 128;
  // use a lower load factor to minimize hash collision
  private static final float loadFactor = 0.25f;
  private static final float furyMapLoadFactor = 0.25f;
  private static final String META_SHARE_FIELDS_INFO_KEY = "shareFieldsInfo";
  private static final ClassInfo NIL_CLASS_INFO =
      new ClassInfo(null, null, null, null, false, null, null, ClassResolver.NO_CLASS_ID);

  private final Fury fury;
  private ClassInfo[] registeredId2ClassInfo = new ClassInfo[] {};

  // IdentityMap has better lookup performance, when loadFactor is 0.05f, performance is better
  private final IdentityMap<Class<?>, ClassInfo> classInfoMap =
      new IdentityMap<>(initialCapacity, furyMapLoadFactor);
  private ClassInfo classInfoCache;
  private final ObjectMap<EnumStringBytes, Class<?>> classNameBytes2Class =
      new ObjectMap<>(initialCapacity, furyMapLoadFactor);
  // Every deserialization for unregistered class will query it, performance is important.
  private final ObjectMap<ClassNameBytes, Class<?>> compositeClassNameBytes2Class =
      new ObjectMap<>(initialCapacity, furyMapLoadFactor);
  private final HashMap<Short, Class<?>> typeIdToClassXLangMap =
      new HashMap<>(initialCapacity, loadFactor);
  private final HashMap<String, Class<?>> typeTagToClassXLangMap =
      new HashMap<>(initialCapacity, loadFactor);
  private final EnumStringResolver enumStringResolver;
  private final boolean metaContextShareEnabled;
  private Class<?> currentReadClass;
  // class id of last default registered class.
  private short innerEndClassId;
  private final ExtRegistry extRegistry;

  private static class ExtRegistry {
    private short registeredClassIdCounter = 0;
    private LongMap<Class<?>> registeredId2Classes = new LongMap<>(initialCapacity);
    private SerializerFactory serializerFactory;
    private final IdentityMap<Class<?>, Short> registeredClassIdMap =
        new IdentityMap<>(initialCapacity);
    private final Map<String, Class<?>> registeredClasses = new HashMap<>(initialCapacity);
    // avoid potential recursive call for seq codec generation.
    // ex. A->field1: B, B.field1: A
    private final Set<Class<?>> getClassCtx = new HashSet<>();
  }

  public ClassResolver(Fury fury) {
    this.fury = fury;
    enumStringResolver = fury.getEnumStringResolver();
    classInfoCache = NIL_CLASS_INFO;
    metaContextShareEnabled = false;
    extRegistry = new ExtRegistry();
  }

  public void initialize() {
    registerWithCheck(void.class, PRIMITIVE_VOID_CLASS_ID);
    registerWithCheck(boolean.class, PRIMITIVE_BOOLEAN_CLASS_ID);
    registerWithCheck(byte.class, PRIMITIVE_BYTE_CLASS_ID);
    registerWithCheck(char.class, PRIMITIVE_CHAR_CLASS_ID);
    registerWithCheck(short.class, PRIMITIVE_SHORT_CLASS_ID);
    registerWithCheck(int.class, PRIMITIVE_INT_CLASS_ID);
    registerWithCheck(float.class, PRIMITIVE_FLOAT_CLASS_ID);
    registerWithCheck(long.class, PRIMITIVE_LONG_CLASS_ID);
    registerWithCheck(double.class, PRIMITIVE_DOUBLE_CLASS_ID);
    registerWithCheck(Void.class, VOID_CLASS_ID);
    registerWithCheck(Boolean.class, BOOLEAN_CLASS_ID);
    registerWithCheck(Byte.class, BYTE_CLASS_ID);
    registerWithCheck(Character.class, CHAR_CLASS_ID);
    registerWithCheck(Short.class, SHORT_CLASS_ID);
    registerWithCheck(Integer.class, INTEGER_CLASS_ID);
    registerWithCheck(Float.class, FLOAT_CLASS_ID);
    registerWithCheck(Long.class, LONG_CLASS_ID);
    registerWithCheck(Double.class, DOUBLE_CLASS_ID);
    registerWithCheck(String.class, STRING_CLASS_ID);
    registerWithCheck(boolean[].class, PRIMITIVE_BOOLEAN_ARRAY_CLASS_ID);
    registerWithCheck(byte[].class, PRIMITIVE_BYTE_ARRAY_CLASS_ID);
    registerWithCheck(char[].class, PRIMITIVE_CHAR_ARRAY_CLASS_ID);
    registerWithCheck(short[].class, PRIMITIVE_SHORT_ARRAY_CLASS_ID);
    registerWithCheck(int[].class, PRIMITIVE_INT_ARRAY_CLASS_ID);
    registerWithCheck(float[].class, PRIMITIVE_FLOAT_ARRAY_CLASS_ID);
    registerWithCheck(long[].class, PRIMITIVE_LONG_ARRAY_CLASS_ID);
    registerWithCheck(double[].class, PRIMITIVE_DOUBLE_ARRAY_CLASS_ID);
    registerWithCheck(String[].class, STRING_ARRAY_CLASS_ID);
    registerWithCheck(Object[].class, OBJECT_ARRAY_CLASS_ID);
    registerWithCheck(ArrayList.class, ARRAYLIST_CLASS_ID);
    registerWithCheck(HashMap.class, HASHMAP_CLASS_ID);
    registerWithCheck(HashSet.class, HASHSET_CLASS_ID);
    registerWithCheck(Class.class, CLASS_CLASS_ID);
    addDefaultSerializers();
    registerDefaultClasses();
    innerEndClassId = extRegistry.registeredClassIdCounter;
  }

  private void addDefaultSerializers() {
    // primitive types will be boxed.
    addDefaultSerializer(boolean.class, new Serializers.BooleanSerializer(fury, boolean.class));
    addDefaultSerializer(byte.class, new Serializers.ByteSerializer(fury, byte.class));
    addDefaultSerializer(short.class, new Serializers.ShortSerializer(fury, short.class));
    addDefaultSerializer(char.class, new Serializers.CharSerializer(fury, char.class));
    addDefaultSerializer(int.class, new Serializers.IntSerializer(fury, int.class));
    addDefaultSerializer(long.class, new Serializers.LongSerializer(fury, long.class));
    addDefaultSerializer(float.class, new Serializers.FloatSerializer(fury, float.class));
    addDefaultSerializer(double.class, new Serializers.DoubleSerializer(fury, double.class));
    addDefaultSerializer(Boolean.class, new Serializers.BooleanSerializer(fury, Boolean.class));
    addDefaultSerializer(Byte.class, new Serializers.ByteSerializer(fury, Byte.class));
    addDefaultSerializer(Short.class, new Serializers.ShortSerializer(fury, Short.class));
    addDefaultSerializer(Character.class, new Serializers.CharSerializer(fury, Character.class));
    addDefaultSerializer(Integer.class, new Serializers.IntSerializer(fury, Integer.class));
    addDefaultSerializer(Long.class, new Serializers.LongSerializer(fury, Long.class));
    addDefaultSerializer(Float.class, new Serializers.FloatSerializer(fury, Float.class));
    addDefaultSerializer(Double.class, new Serializers.DoubleSerializer(fury, Double.class));
  }

  private void addDefaultSerializer(Class<?> type, Class<? extends Serializer> serializerClass) {
    addDefaultSerializer(type, Serializers.newSerializer(fury, type, serializerClass));
  }

  private void addDefaultSerializer(Class type, Serializer serializer) {
    registerSerializer(type, serializer);
    register(type);
  }

  private void registerDefaultClasses() {
    register(Object.class, Object[].class, Void.class);
    register(ByteBuffer.allocate(1).getClass());
    register(ByteBuffer.allocateDirect(1).getClass());
    register(Comparator.naturalOrder().getClass());
    register(Comparator.reverseOrder().getClass());
    register(ConcurrentHashMap.class);
    register(ArrayBlockingQueue.class);
    register(LinkedBlockingQueue.class);
    register(Boolean[].class, Byte[].class, Short[].class, Character[].class);
    register(Integer[].class, Float[].class, Long[].class, Double[].class);
    register(AtomicBoolean.class);
    register(AtomicInteger.class);
    register(AtomicLong.class);
    register(AtomicReference.class);
    register(EnumSet.allOf(Language.class).getClass());
    register(EnumSet.of(Language.JAVA).getClass());
  }

  /** register class. */
  public void register(Class<?> cls) {
    if (!extRegistry.registeredClassIdMap.containsKey(cls)) {
      while (extRegistry.registeredId2Classes.containsKey(extRegistry.registeredClassIdCounter)) {
        extRegistry.registeredClassIdCounter++;
      }
      register(cls, extRegistry.registeredClassIdCounter);
    }
  }

  public void register(Class<?>... classes) {
    for (Class<?> cls : classes) {
      register(cls);
    }
  }

  /** register class with given type tag which will be used for cross-language serialization. */
  public void register(Class<?> cls, String typeTag) {
    Preconditions.checkArgument(!typeTagToClassXLangMap.containsKey(typeTag));
    throw new UnsupportedOperationException();
  }

  public void register(Class<?> cls, short id) {
    Preconditions.checkArgument(id >= 0);
    if (!extRegistry.registeredClassIdMap.containsKey(cls)) {
      if (extRegistry.registeredClasses.containsKey(cls.getName())) {
        throw new IllegalArgumentException(
            String.format(
                "Class %s with name %s has been registered, registering classes with same name are not allowed.",
                extRegistry.registeredClasses.get(cls.getName()), cls.getName()));
      }
      extRegistry.registeredClassIdMap.put(cls, id);
      if (registeredId2ClassInfo.length <= id) {
        ClassInfo[] tmp = new ClassInfo[(id + 1) * 2];
        System.arraycopy(registeredId2ClassInfo, 0, tmp, 0, registeredId2ClassInfo.length);
        registeredId2ClassInfo = tmp;
      }
      ClassInfo classInfo = classInfoMap.get(cls);
      if (classInfo != null) {
        classInfo.classId = id;
      } else {
        classInfo = new ClassInfo(this, cls, null, null, id);
        // make `extRegistry.registeredClassIdMap` and `classInfoMap` share same classInfo
        // instances.
        classInfoMap.put(cls, classInfo);
      }
      // serializer will be set lazily in `addSerializer` method if it's null.
      registeredId2ClassInfo[id] = classInfo;
      extRegistry.registeredClasses.put(cls.getName(), cls);
      extRegistry.registeredClassIdCounter++;
      extRegistry.registeredId2Classes.put(id, cls);
    }
  }

  /** register class with given id. */
  public void registerWithCheck(Class<?> cls, short id) {
    if (extRegistry.registeredClassIdMap.containsKey(cls)) {
      throw new IllegalArgumentException(
          String.format(
              "" + "Class %s already registered with id %s.",
              cls, extRegistry.registeredClassIdMap.get(cls)));
    }
    register(cls, id);
  }

  public Short getRegisteredClassId(Class<?> cls) {
    return extRegistry.registeredClassIdMap.get(cls);
  }

  public Class<?> getRegisteredClass(short id) {
    if (id < registeredId2ClassInfo.length) {
      ClassInfo classInfo = registeredId2ClassInfo[id];
      if (classInfo != null) {
        return classInfo.cls;
      }
    }
    return null;
  }

  public List<Class<?>> getRegisteredClasses() {
    return Arrays.stream(registeredId2ClassInfo)
        .filter(Objects::nonNull)
        .map(info -> info.cls)
        .collect(Collectors.toList());
  }

  /** Returns true if <code>cls</code> is fury inner registered class. */
  boolean isInnerClass(Class<?> cls) {
    Short classId = extRegistry.registeredClassIdMap.get(cls);
    if (classId == null) {
      ClassInfo classInfo = getClassInfo(cls, false);
      if (classInfo != null) {
        classId = classInfo.getClassId();
      }
    }
    return classId != null && classId != NO_CLASS_ID && classId < innerEndClassId;
  }

  /**
   * Register a Serializer.
   *
   * @param type class needed to be serialized/deserialized
   * @param serializerClass serializer class can be created with {@link Serializers#newSerializer}
   * @param <T> type of class
   */
  public <T> void registerSerializer(Class<T> type, Class<? extends Serializer> serializerClass) {
    registerSerializer(type, Serializers.newSerializer(fury, type, serializerClass));
  }

  /**
   * If a serializer exists before, it will be replaced by new serializer.
   *
   * @param type class needed to be serialized/deserialized
   * @param serializer serializer for object of {@code type}
   */
  public void registerSerializer(Class<?> type, Serializer<?> serializer) {
    if (!extRegistry.registeredClassIdMap.containsKey(type)
        && fury.getLanguage() == Language.JAVA) {
      register(type);
    }
    addSerializer(type, serializer);
  }

  public void setSerializerFactory(SerializerFactory serializerFactory) {
    this.extRegistry.serializerFactory = serializerFactory;
  }

  public SerializerFactory getSerializerFactory() {
    return extRegistry.serializerFactory;
  }

  /**
   * Set the serializer for <code>cls</code>, overwrite serializer if exists. Note if class info is
   * already related with a class, this method should try to reuse that class info, otherwise jit
   * callback to update serializer won't take effect in some cases since it can't change that
   * classinfo.
   */
  public <T> void setSerializer(Class<T> cls, Serializer<T> serializer) {
    addSerializer(cls, serializer);
  }

  /**
   * Reset serializer if <code>serializer</code> is not null, otherwise clear serializer for <code>
   * cls</code>.
   *
   * @see #setSerializer
   * @see #clearSerializer
   * @see #createSerializerSafe
   */
  public <T> void resetSerializer(Class<T> cls, Serializer<T> serializer) {
    if (serializer == null) {
      clearSerializer(cls);
    } else {
      setSerializer(cls, serializer);
    }
  }

  /**
   * Set serializer to avoid circular error when there is a serializer query for fields by {@link
   * #getClassInfo} and {@link #getSerializer(Class)} which access current creating serializer. This
   * method is used to avoid overwriting existing serializer for class when creating a data
   * serializer for serialization of parts fields of a class.
   */
  public <T> void setSerializerIfAbsent(Class<T> cls, Serializer<T> serializer) {
    Serializer<T> s = getSerializer(cls, false);
    if (s == null) {
      setSerializer(cls, serializer);
    }
  }

  /** Clear serializer associated with <code>cls</code> if not null. */
  public void clearSerializer(Class<?> cls) {
    ClassInfo classInfo = classInfoMap.get(cls);
    if (classInfo != null) {
      classInfo.serializer = null;
    }
  }

  /** Ass serializer for specified class. */
  private void addSerializer(Class<?> type, Serializer<?> serializer) {
    Preconditions.checkNotNull(serializer);
    String typeTag = null;
    short typeId = serializer.getCrossLanguageTypeId();
    if (typeId != Fury.NOT_SUPPORT_CROSS_LANGUAGE) {
      if (typeId > Fury.NOT_SUPPORT_CROSS_LANGUAGE) {
        typeIdToClassXLangMap.put(typeId, type);
      }
      if (typeId == Fury.FURY_TYPE_TAG_ID) {
        typeTag = serializer.getCrossLanguageTypeTag();
        typeTagToClassXLangMap.put(typeTag, type);
      }
    }
    ClassInfo classInfo;
    Short classId = extRegistry.registeredClassIdMap.get(type);
    // set serializer for class if it's registered by now.
    if (classId != null) {
      classInfo = registeredId2ClassInfo[classId];
      classInfo.serializer = serializer;
    } else {
      classId = NO_CLASS_ID;
      classInfo = classInfoMap.get(type);
    }
    if (classInfo == null || typeTag != null || classId != classInfo.classId) {
      classInfo = new ClassInfo(this, type, typeTag, serializer, classId);
    } else {
      classInfo.serializer = serializer;
    }
    // make `extRegistry.registeredClassIdMap` and `classInfoMap` share same classInfo instances.
    classInfoMap.put(type, classInfo);
  }

  @SuppressWarnings("unchecked")
  public <T> Serializer<T> getSerializer(Class<T> cls, boolean createIfNotExist) {
    Preconditions.checkNotNull(cls);
    if (createIfNotExist) {
      return getSerializer(cls);
    }
    ClassInfo classInfo = classInfoMap.get(cls);
    return classInfo == null ? null : (Serializer<T>) classInfo.serializer;
  }

  /** Get or create serializer for <code>cls</code>. */
  @SuppressWarnings("unchecked")
  public <T> Serializer<T> getSerializer(Class<T> cls) {
    Preconditions.checkNotNull(cls);
    return (Serializer<T>) getOrUpdateClassInfo(cls).serializer;
  }

  public Class<? extends Serializer> getSerializerClass(Class<?> cls) {
    if (cls.isPrimitive()) {
      cls = Primitives.wrap(cls);
    }
    ClassInfo classInfo = classInfoMap.get(cls);
    if (classInfo != null && classInfo.serializer != null) {
      // Note: need to check `classInfo.serializer != null`, because sometimes `cls` is already
      // serialized, which will create a class info with serializer null, see `#writeClassInternal`
      return classInfo.serializer.getClass();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Whether to track reference for this type. If false, reference tracing of subclasses may be
   * ignored too.
   */
  public boolean needToWriteReference(Class<?> cls) {
    if (fury.trackingReference()) {
      ClassInfo classInfo = getClassInfo(cls, false);
      if (classInfo == null || classInfo.serializer == null) {
        // TODO group related logic together for extendability and consistency.
        return !cls.isEnum();
      } else {
        return classInfo.serializer.needToWriteReference();
      }
    }
    return false;
  }

  public ClassInfo getClassInfo(short classId) {
    ClassInfo classInfo = registeredId2ClassInfo[classId];
    if (classInfo.serializer == null) {
      addSerializer(classInfo.cls, createSerializer(classInfo.cls));
      classInfo = classInfoMap.get(classInfo.cls);
    }
    return classInfo;
  }

  // Invoked by fury JIT.
  public ClassInfo getClassInfo(Class<?> cls) {
    ClassInfo classInfo = classInfoMap.get(cls);
    if (classInfo == null || classInfo.serializer == null) {
      addSerializer(cls, createSerializer(cls));
      classInfo = classInfoMap.get(cls);
    }
    return classInfo;
  }

  public ClassInfo getClassInfo(Class<?> cls, ClassInfoCache classInfoCache) {
    ClassInfo classInfo = classInfoCache.classInfo;
    if (classInfo.getCls() != cls) {
      classInfo = classInfoMap.get(cls);
      if (classInfo == null || classInfo.serializer == null) {
        addSerializer(cls, createSerializer(cls));
        classInfo = Objects.requireNonNull(classInfoMap.get(cls));
      }
      classInfoCache.classInfo = classInfo;
    }
    assert classInfo.serializer != null;
    return classInfo;
  }

  /**
   * Get class information, create class info if not found and `createClassInfoIfNotFound` is true.
   *
   * @param cls which class to get class info.
   * @param createClassInfoIfNotFound whether create class info if not found.
   * @return Class info.
   */
  public ClassInfo getClassInfo(Class<?> cls, boolean createClassInfoIfNotFound) {
    if (createClassInfoIfNotFound) {
      return getOrUpdateClassInfo(cls);
    }
    if (extRegistry.getClassCtx.contains(cls)) {
      return null;
    } else {
      return classInfoMap.get(cls);
    }
  }

  @Internal
  public ClassInfo getOrUpdateClassInfo(Class<?> cls) {
    ClassInfo classInfo = classInfoCache;
    if (classInfo.cls != cls) {
      classInfo = classInfoMap.get(cls);
      if (classInfo == null || classInfo.serializer == null) {
        addSerializer(cls, createSerializer(cls));
        classInfo = classInfoMap.get(cls);
      }
      classInfoCache = classInfo;
    }
    return classInfo;
  }

  private ClassInfo getOrUpdateClassInfo(short classId) {
    ClassInfo classInfo = classInfoCache;
    if (classInfo.classId != classId) {
      classInfo = registeredId2ClassInfo[classId];
      if (classInfo.serializer == null) {
        addSerializer(classInfo.cls, createSerializer(classInfo.cls));
        classInfo = classInfoMap.get(classInfo.cls);
      }
      classInfoCache = classInfo;
    }
    return classInfo;
  }

  public <T> Serializer<T> createSerializerSafe(Class<T> cls, Supplier<Serializer<T>> func) {
    Serializer serializer = fury.getClassResolver().getSerializer(cls, false);
    try {
      return func.get();
    } catch (Throwable t) {
      // Some serializer may set itself in constructor as serializer, but the
      // constructor failed later. For example, some final type field doesn't
      // support serialization.
      resetSerializer(cls, serializer);
      Platform.throwException(t);
      throw new IllegalStateException("unreachable");
    }
  }

  private Serializer createSerializer(Class<?> cls) {
    if (!extRegistry.registeredClassIdMap.containsKey(cls)) {
      String msg =
          String.format(
              "%s is not registered, if it's not the type you want to serialize, "
                  + "it may be a **vulnerability**. If it's not a vulnerability, "
                  + "registering class by `Fury#register` will have better performance, "
                  + "otherwise class name will be serialized too.",
              cls);
      if ((fury.isClassRegistrationRequired()
          && !isSecure(extRegistry.registeredClassIdMap, cls))) {
        throw new InsecureException(msg);
      } else {
        if (!Functions.isLambda(cls) && !ReflectionUtils.isJdkProxy(cls)) {
          LOG.warn(msg);
        }
      }
    }
    if (extRegistry.serializerFactory != null) {
      Serializer serializer = extRegistry.serializerFactory.createSerializer(fury, cls);
      if (serializer != null) {
        return serializer;
      }
    }
    Class<? extends Serializer> serializerClass = getSerializerClass(cls);
    return Serializers.newSerializer(fury, cls, serializerClass);
  }

  private static boolean isSecure(IdentityMap<Class<?>, Short> registeredClasses, Class<?> cls) {
    // if (BlackList.getDefaultBlackList().contains(cls.getName())) {
    //   return false;
    // }
    if (registeredClasses.containsKey(cls)) {
      return true;
    }
    if (cls.isArray()) {
      return isSecure(registeredClasses, TypeUtils.getArrayComponent(cls));
    }
    // Don't take java Exception as secure in case future JDK introduce insecure JDK exception.
    // if (Exception.class.isAssignableFrom(cls)
    //     && cls.getName().startsWith("java.")
    //     && !cls.getName().startsWith("java.sql")) {
    //   return true;
    // }
    return Functions.isLambda(cls) || ReflectionUtils.isJdkProxy(cls);
  }

  /**
   * Write class info to <code>buffer</code>. TODO(chaokunyang): The method should try to write
   * aligned data to reduce cpu instruction overhead. `writeClass` is the last step before
   * serializing object, if this writes are aligned, then later serialization will be more
   * efficient.
   */
  public void writeClassAndUpdateCache(MemoryBuffer buffer, Class<?> cls) {
    // fast path for common type
    if (cls == Long.class) {
      buffer.writeByte(USE_STRING_ID);
      buffer.writeShort(LONG_CLASS_ID);
    } else if (cls == Integer.class) {
      buffer.writeByte(USE_STRING_ID);
      buffer.writeShort(INTEGER_CLASS_ID);
    } else if (cls == Double.class) {
      buffer.writeByte(USE_STRING_ID);
      buffer.writeShort(DOUBLE_CLASS_ID);
    } else {
      writeClass(buffer, getOrUpdateClassInfo(cls));
    }
  }

  // The jit-compiled native code fot this method will be too big for inline, so we generated
  // `getClassInfo`
  // in fury-jit, see `BaseSeqCodecBuilder#writeAndGetClassInfo`
  // public ClassInfo writeClass(MemoryBuffer buffer, Class<?> cls, ClassInfoCache classInfoCache) {
  //   ClassInfo classInfo = getClassInfo(cls, classInfoCache);
  //   writeClass(buffer, classInfo);
  //   return classInfo;
  // }

  /** Write classname for java serialization. */
  public void writeClass(MemoryBuffer buffer, ClassInfo classInfo) {
    if (classInfo.classId == NO_CLASS_ID) { // no class id provided.
      // use classname
      buffer.writeByte(USE_CLASS_VALUE);
      if (metaContextShareEnabled) {
        // FIXME(chaokunyang) Register class but oot register serializer can't be used with
        //  meta share mode, because no class def are sent to peer.
        throw new UnsupportedOperationException();
      } else {
        // if it's null, it's a bug.
        assert classInfo.packageNameBytes != null;
        enumStringResolver.writeEnumStringBytes(buffer, classInfo.packageNameBytes);
        assert classInfo.classNameBytes != null;
        enumStringResolver.writeEnumStringBytes(buffer, classInfo.classNameBytes);
      }
    } else {
      // use classId
      int writerIndex = buffer.writerIndex();
      buffer.increaseWriterIndex(3);
      buffer.unsafePut(writerIndex, USE_STRING_ID);
      buffer.unsafePutShort(writerIndex + 1, classInfo.classId);
    }
  }

  public ClassInfo readAndUpdateClassInfoCache(MemoryBuffer buffer) {
    if (buffer.readByte() == USE_CLASS_VALUE) {
      ClassInfo classInfo;
      if (metaContextShareEnabled) {
        throw new UnsupportedOperationException();
      } else {
        classInfo = readClassInfoFromBytes(buffer, classInfoCache);
      }
      classInfoCache = classInfo;
      currentReadClass = classInfo.cls;
      return classInfo;
    } else {
      // use classId
      short classId = buffer.readShort();
      ClassInfo classInfo = getOrUpdateClassInfo(classId);
      currentReadClass = classInfo.cls;
      return classInfo;
    }
  }

  /** Read class info from java data <code>buffer</code> as a Class. */
  public Class<?> readClassAndUpdateCache(MemoryBuffer buffer) {
    if (buffer.readByte() == USE_CLASS_VALUE) {
      ClassInfo classInfo;
      if (metaContextShareEnabled) {
        throw new UnsupportedOperationException();
      } else {
        classInfo = readClassInfoFromBytes(buffer, classInfoCache);
      }
      classInfoCache = classInfo;
      currentReadClass = classInfo.cls;
      return classInfo.cls;
    } else {
      // use classId
      short classId = buffer.readShort();
      ClassInfo classInfo = getOrUpdateClassInfo(classId);
      final Class<?> cls = classInfo.cls;
      currentReadClass = cls;
      return cls;
    }
  }

  public ClassInfo readClassInfo(MemoryBuffer buffer, ClassInfoCache classInfoCache) {
    if (buffer.readByte() == USE_CLASS_VALUE) {
      return readClassInfoFromBytes(buffer, classInfoCache);
    } else {
      // use classId
      short classId = buffer.readShort();
      return getClassInfo(classId);
    }
  }

  private ClassInfo readClassInfoFromBytes(MemoryBuffer buffer, ClassInfoCache classInfoCache) {
    ClassInfo classInfo = readClassInfoFromBytes(buffer, classInfoCache.classInfo);
    classInfoCache.classInfo = classInfo;
    return classInfo;
  }

  private ClassInfo readClassInfoFromBytes(MemoryBuffer buffer, ClassInfo classInfoCache) {
    EnumStringBytes simpleClassNameBytesCache = classInfoCache.classNameBytes;
    if (simpleClassNameBytesCache != null) {
      EnumStringBytes packageNameBytesCache = classInfoCache.packageNameBytes;
      EnumStringBytes packageBytes =
          enumStringResolver.readEnumStringBytes(buffer, packageNameBytesCache);
      assert packageNameBytesCache != null;
      EnumStringBytes simpleClassNameBytes =
          enumStringResolver.readEnumStringBytes(buffer, simpleClassNameBytesCache);
      if (simpleClassNameBytesCache.hashCode == simpleClassNameBytes.hashCode
          && packageNameBytesCache.hashCode == packageBytes.hashCode) {
        return classInfoCache;
      } else {
        Class<?> cls = loadBytesToClass(packageBytes, simpleClassNameBytes);
        return getClassInfo(cls);
      }
    } else {
      EnumStringBytes packageBytes = enumStringResolver.readEnumStringBytes(buffer);
      EnumStringBytes simpleClassNameBytes = enumStringResolver.readEnumStringBytes(buffer);
      Class<?> cls = loadBytesToClass(packageBytes, simpleClassNameBytes);
      return getClassInfo(cls);
    }
  }

  private Class<?> loadBytesToClass(
      EnumStringBytes packageBytes, EnumStringBytes simpleClassNameBytes) {
    ClassNameBytes classNameBytes =
        new ClassNameBytes(packageBytes.hashCode, simpleClassNameBytes.hashCode);
    Class<?> cls = compositeClassNameBytes2Class.get(classNameBytes);
    if (cls == null) {
      String packageName = new String(packageBytes.bytes, StandardCharsets.UTF_8);
      String className = new String(simpleClassNameBytes.bytes, StandardCharsets.UTF_8);
      String entireClassName;
      if (StringUtils.isBlank(packageName)) {
        entireClassName = className;
      } else {
        entireClassName = packageName + "." + className;
      }
      cls = loadClass(entireClassName);
      compositeClassNameBytes2Class.put(classNameBytes, cls);
    }
    return cls;
  }

  public void crossLanguageWriteClass(MemoryBuffer buffer, Class<?> cls) {
    enumStringResolver.writeEnumStringBytes(buffer, getOrUpdateClassInfo(cls).fullClassNameBytes);
  }

  public void crossLanguageWriteTypeTag(MemoryBuffer buffer, Class<?> cls) {
    enumStringResolver.writeEnumStringBytes(buffer, getOrUpdateClassInfo(cls).typeTagBytes);
  }

  public Class<?> crossLanguageReadClass(MemoryBuffer buffer) {
    EnumStringBytes byteString = enumStringResolver.readEnumStringBytes(buffer);
    Class<?> cls = classNameBytes2Class.get(byteString);
    if (cls == null) {
      Preconditions.checkNotNull(byteString);
      String className = new String(byteString.bytes, StandardCharsets.UTF_8);
      cls = loadClass(className);
      classNameBytes2Class.put(byteString, cls);
    }
    currentReadClass = cls;
    return cls;
  }

  public String crossLanguageReadClassName(MemoryBuffer buffer) {
    return enumStringResolver.readEnumString(buffer);
  }

  public Class<?> getCurrentReadClass() {
    return currentReadClass;
  }

  private Class<?> loadClass(String className) {
    try {
      return Class.forName(className, false, fury.getClassLoader());
    } catch (ClassNotFoundException e) {
      try {
        return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
      } catch (ClassNotFoundException ex) {
        String msg =
            String.format(
                "Class %s not found from classloaders [%s, %s]",
                className, fury.getClassLoader(), Thread.currentThread().getContextClassLoader());
        throw new IllegalStateException(msg, ex);
      }
    }
  }

  public void reset() {
    resetRead();
    resetWrite();
  }

  public void resetRead() {}

  public void resetWrite() {}

  public Class<?> getClassByTypeId(short typeId) {
    return typeIdToClassXLangMap.get(typeId);
  }

  public Class<?> readClassByTypeTag(MemoryBuffer buffer) {
    String tag = enumStringResolver.readEnumString(buffer);
    return typeTagToClassXLangMap.get(tag);
  }

  private static class ClassNameBytes {
    private final long packageHash;
    private final long classNameHash;

    private ClassNameBytes(long packageHash, long classNameHash) {
      this.packageHash = packageHash;
      this.classNameHash = classNameHash;
    }

    @Override
    public boolean equals(Object o) {
      // ClassNameBytes is used internally, skip
      ClassNameBytes that = (ClassNameBytes) o;
      return packageHash == that.packageHash && classNameHash == that.classNameHash;
    }

    @Override
    public int hashCode() {
      int result = 31 + (int) (packageHash ^ (packageHash >>> 32));
      result = result * 31 + (int) (classNameHash ^ (classNameHash >>> 32));
      return result;
    }
  }

  // Invoked by fury JIT.
  public ClassInfo nilClassInfo() {
    return new ClassInfo(this, null, null, null, NO_CLASS_ID);
  }

  public ClassInfoCache nilClassInfoCache() {
    return new ClassInfoCache(nilClassInfo());
  }

  public boolean isPrimitive(short classId) {
    return classId >= PRIMITIVE_VOID_CLASS_ID && classId <= PRIMITIVE_DOUBLE_CLASS_ID;
  }

  public EnumStringResolver getEnumStringResolver() {
    return enumStringResolver;
  }

  public Fury getFury() {
    return fury;
  }
}
