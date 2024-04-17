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

package org.apache.fury.resolver;

import static org.apache.fury.serializer.CodegenSerializer.loadCodegenSerializer;
import static org.apache.fury.serializer.CodegenSerializer.loadCompatibleCodegenSerializer;
import static org.apache.fury.serializer.CodegenSerializer.supportCodegenForJavaSerialization;
import static org.apache.fury.type.TypeUtils.OBJECT_TYPE;
import static org.apache.fury.type.TypeUtils.getRawType;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import java.io.Externalizable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.fury.Fury;
import org.apache.fury.annotation.CodegenInvoke;
import org.apache.fury.annotation.Internal;
import org.apache.fury.builder.CodecUtils;
import org.apache.fury.builder.Generated;
import org.apache.fury.builder.JITContext;
import org.apache.fury.codegen.CodeGenerator;
import org.apache.fury.codegen.Expression;
import org.apache.fury.codegen.Expression.Invoke;
import org.apache.fury.codegen.Expression.Literal;
import org.apache.fury.collection.IdentityMap;
import org.apache.fury.collection.IdentityObjectIntMap;
import org.apache.fury.collection.ObjectMap;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
import org.apache.fury.exception.InsecureException;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.meta.ClassDef;
import org.apache.fury.serializer.ArraySerializers;
import org.apache.fury.serializer.BufferSerializers;
import org.apache.fury.serializer.CodegenSerializer.LazyInitBeanSerializer;
import org.apache.fury.serializer.CompatibleSerializer;
import org.apache.fury.serializer.ExternalizableSerializer;
import org.apache.fury.serializer.JavaSerializer;
import org.apache.fury.serializer.JdkProxySerializer;
import org.apache.fury.serializer.LambdaSerializer;
import org.apache.fury.serializer.LocaleSerializer;
import org.apache.fury.serializer.MetaSharedSerializer;
import org.apache.fury.serializer.ObjectSerializer;
import org.apache.fury.serializer.OptionalSerializers;
import org.apache.fury.serializer.PrimitiveSerializers;
import org.apache.fury.serializer.ReplaceResolveSerializer;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.SerializerFactory;
import org.apache.fury.serializer.Serializers;
import org.apache.fury.serializer.StringSerializer;
import org.apache.fury.serializer.StructSerializer;
import org.apache.fury.serializer.TimeSerializers;
import org.apache.fury.serializer.UnexistedClassSerializers.UnexistedClassSerializer;
import org.apache.fury.serializer.UnexistedClassSerializers.UnexistedMetaSharedClass;
import org.apache.fury.serializer.UnexistedClassSerializers.UnexistedSkipClass;
import org.apache.fury.serializer.collection.ChildContainerSerializers;
import org.apache.fury.serializer.collection.CollectionSerializer;
import org.apache.fury.serializer.collection.CollectionSerializers;
import org.apache.fury.serializer.collection.GuavaCollectionSerializers;
import org.apache.fury.serializer.collection.ImmutableCollectionSerializers;
import org.apache.fury.serializer.collection.MapSerializer;
import org.apache.fury.serializer.collection.MapSerializers;
import org.apache.fury.serializer.collection.SynchronizedSerializers;
import org.apache.fury.serializer.collection.UnmodifiableSerializers;
import org.apache.fury.serializer.scala.SingletonCollectionSerializer;
import org.apache.fury.serializer.scala.SingletonMapSerializer;
import org.apache.fury.serializer.scala.SingletonObjectSerializer;
import org.apache.fury.serializer.shim.ShimDispatcher;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.GenericType;
import org.apache.fury.type.ScalaTypes;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.util.GraalvmSupport;
import org.apache.fury.util.Platform;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;
import org.apache.fury.util.StringUtils;
import org.apache.fury.util.function.Functions;

/**
 * Class registry for types of serializing objects, responsible for reading/writing types, setting
 * up relations between serializer and types.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClassResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ClassResolver.class);

  // bit 0 unset indicates class is written as an id.
  public static final byte USE_CLASS_VALUE_FLAG = 0b1;
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
  public static final short EMPTY_OBJECT_ID = (short) (PRIMITIVE_DOUBLE_ARRAY_CLASS_ID + 7);
  // use a lower load factor to minimize hash collision
  private static final float loadFactor = 0.25f;
  private static final float furyMapLoadFactor = 0.25f;
  private static final int estimatedNumRegistered = 150;
  private static final String META_SHARE_FIELDS_INFO_KEY = "shareFieldsInfo";
  private static final ClassInfo NIL_CLASS_INFO =
      new ClassInfo(null, null, null, null, false, null, null, ClassResolver.NO_CLASS_ID);

  private final Fury fury;
  private ClassInfo[] registeredId2ClassInfo = new ClassInfo[] {};

  // IdentityMap has better lookup performance, when loadFactor is 0.05f, performance is better
  private final IdentityMap<Class<?>, ClassInfo> classInfoMap =
      new IdentityMap<>(estimatedNumRegistered, furyMapLoadFactor);
  private ClassInfo classInfoCache;
  private final ObjectMap<MetaStringBytes, Class<?>> classNameBytes2Class =
      new ObjectMap<>(16, furyMapLoadFactor);
  // Every deserialization for unregistered class will query it, performance is important.
  private final ObjectMap<ClassNameBytes, Class<?>> compositeClassNameBytes2Class =
      new ObjectMap<>(16, furyMapLoadFactor);
  private final HashMap<Short, Class<?>> typeIdToClassXLangMap = new HashMap<>(8, loadFactor);
  private final HashMap<String, Class<?>> typeTagToClassXLangMap = new HashMap<>(8, loadFactor);
  private final MetaStringResolver metaStringResolver;
  private final boolean metaContextShareEnabled;
  private final Map<Class<?>, ClassDef> classDefMap = new HashMap<>();
  private Class<?> currentReadClass;
  // class id of last default registered class.
  private short innerEndClassId;
  private final ExtRegistry extRegistry;
  private final ShimDispatcher shimDispatcher;

  private static class ExtRegistry {
    // Here we set it to 1 because `NO_CLASS_ID` is 0 to avoid calculating it again in
    // `register(Class<?> cls)`.
    private short classIdGenerator = 1;
    private SerializerFactory serializerFactory;
    private final IdentityMap<Class<?>, Short> registeredClassIdMap =
        new IdentityMap<>(estimatedNumRegistered);
    private final Map<String, Class<?>> registeredClasses = new HashMap<>(estimatedNumRegistered);
    // avoid potential recursive call for seq codec generation.
    // ex. A->field1: B, B.field1: A
    private final Set<Class<?>> getClassCtx = new HashSet<>();
    private final Map<Class<?>, FieldResolver> fieldResolverMap = new HashMap<>();
    private final Map<Long, Tuple2<ClassDef, ClassInfo>> classIdToDef = new HashMap<>();
    // TODO(chaokunyang) Better to  use soft reference, see ObjectStreamClass.
    private final ConcurrentHashMap<Tuple2<Class<?>, Boolean>, SortedMap<Field, Descriptor>>
        descriptorsCache = new ConcurrentHashMap<>();
    private ClassChecker classChecker = (classResolver, className) -> true;
    private GenericType objectGenericType;
    private Map<List<ClassLoader>, CodeGenerator> codeGeneratorMap = new HashMap<>();
  }

  public ClassResolver(Fury fury) {
    this.fury = fury;
    metaStringResolver = fury.getMetaStringResolver();
    classInfoCache = NIL_CLASS_INFO;
    metaContextShareEnabled = fury.getConfig().shareMetaContext();
    extRegistry = new ExtRegistry();
    extRegistry.objectGenericType = buildGenericType(OBJECT_TYPE);
    shimDispatcher = new ShimDispatcher(fury);
  }

  public void initialize() {
    register(LambdaSerializer.ReplaceStub.class, LAMBDA_STUB_ID);
    register(JdkProxySerializer.ReplaceStub.class, JDK_PROXY_STUB_ID);
    register(ReplaceResolveSerializer.ReplaceStub.class, REPLACE_STUB_ID);
    register(void.class, PRIMITIVE_VOID_CLASS_ID);
    register(boolean.class, PRIMITIVE_BOOLEAN_CLASS_ID);
    register(byte.class, PRIMITIVE_BYTE_CLASS_ID);
    register(char.class, PRIMITIVE_CHAR_CLASS_ID);
    register(short.class, PRIMITIVE_SHORT_CLASS_ID);
    register(int.class, PRIMITIVE_INT_CLASS_ID);
    register(float.class, PRIMITIVE_FLOAT_CLASS_ID);
    register(long.class, PRIMITIVE_LONG_CLASS_ID);
    register(double.class, PRIMITIVE_DOUBLE_CLASS_ID);
    register(Void.class, VOID_CLASS_ID);
    register(Boolean.class, BOOLEAN_CLASS_ID);
    register(Byte.class, BYTE_CLASS_ID);
    register(Character.class, CHAR_CLASS_ID);
    register(Short.class, SHORT_CLASS_ID);
    register(Integer.class, INTEGER_CLASS_ID);
    register(Float.class, FLOAT_CLASS_ID);
    register(Long.class, LONG_CLASS_ID);
    register(Double.class, DOUBLE_CLASS_ID);
    register(String.class, STRING_CLASS_ID);
    register(boolean[].class, PRIMITIVE_BOOLEAN_ARRAY_CLASS_ID);
    register(byte[].class, PRIMITIVE_BYTE_ARRAY_CLASS_ID);
    register(char[].class, PRIMITIVE_CHAR_ARRAY_CLASS_ID);
    register(short[].class, PRIMITIVE_SHORT_ARRAY_CLASS_ID);
    register(int[].class, PRIMITIVE_INT_ARRAY_CLASS_ID);
    register(float[].class, PRIMITIVE_FLOAT_ARRAY_CLASS_ID);
    register(long[].class, PRIMITIVE_LONG_ARRAY_CLASS_ID);
    register(double[].class, PRIMITIVE_DOUBLE_ARRAY_CLASS_ID);
    register(String[].class, STRING_ARRAY_CLASS_ID);
    register(Object[].class, OBJECT_ARRAY_CLASS_ID);
    register(ArrayList.class, ARRAYLIST_CLASS_ID);
    register(HashMap.class, HASHMAP_CLASS_ID);
    register(HashSet.class, HASHSET_CLASS_ID);
    register(Class.class, CLASS_CLASS_ID);
    register(Object.class, EMPTY_OBJECT_ID);
    registerCommonUsedClasses();
    registerDefaultClasses();
    addDefaultSerializers();
    shimDispatcher.initialize();
    innerEndClassId = extRegistry.classIdGenerator;
  }

  private void addDefaultSerializers() {
    // primitive types will be boxed.
    addDefaultSerializer(String.class, new StringSerializer(fury));
    PrimitiveSerializers.registerDefaultSerializers(fury);
    Serializers.registerDefaultSerializers(fury);
    ArraySerializers.registerDefaultSerializers(fury);
    TimeSerializers.registerDefaultSerializers(fury);
    OptionalSerializers.registerDefaultSerializers(fury);
    CollectionSerializers.registerDefaultSerializers(fury);
    MapSerializers.registerDefaultSerializers(fury);
    addDefaultSerializer(Locale.class, new LocaleSerializer(fury));
    addDefaultSerializer(
        LambdaSerializer.ReplaceStub.class,
        new LambdaSerializer(fury, LambdaSerializer.ReplaceStub.class));
    addDefaultSerializer(
        JdkProxySerializer.ReplaceStub.class,
        new JdkProxySerializer(fury, JdkProxySerializer.ReplaceStub.class));
    addDefaultSerializer(
        ReplaceResolveSerializer.ReplaceStub.class,
        new ReplaceResolveSerializer(fury, ReplaceResolveSerializer.ReplaceStub.class));
    SynchronizedSerializers.registerSerializers(fury);
    UnmodifiableSerializers.registerSerializers(fury);
    ImmutableCollectionSerializers.registerSerializers(fury);
    if (fury.getConfig().registerGuavaTypes()) {
      GuavaCollectionSerializers.registerDefaultSerializers(fury);
    }
    if (metaContextShareEnabled) {
      addDefaultSerializer(
          UnexistedMetaSharedClass.class, new UnexistedClassSerializer(fury, null));
      // Those class id must be known in advance, here is two bytes, so
      // `UnexistedClassSerializer.writeClassDef`
      // can overwrite written classinfo and replace with real classinfo.
      short classId =
          Objects.requireNonNull(classInfoMap.get(UnexistedMetaSharedClass.class)).classId;
      Preconditions.checkArgument(classId > 63 && classId < 8192, classId);
    } else {
      register(UnexistedSkipClass.class);
    }
  }

  private void addDefaultSerializer(Class<?> type, Class<? extends Serializer> serializerClass) {
    addDefaultSerializer(type, Serializers.newSerializer(fury, type, serializerClass));
  }

  private void addDefaultSerializer(Class type, Serializer serializer) {
    registerSerializer(type, serializer);
    register(type);
  }

  /** Register common class ahead to get smaller class id for serialization. */
  private void registerCommonUsedClasses() {
    register(LinkedList.class, TreeSet.class);
    register(LinkedHashMap.class, TreeMap.class);
    register(Date.class, Timestamp.class, LocalDateTime.class, Instant.class);
    register(BigInteger.class, BigDecimal.class);
    register(Optional.class, OptionalInt.class);
    register(Boolean[].class, Byte[].class, Short[].class, Character[].class);
    register(Integer[].class, Float[].class, Long[].class, Double[].class);
  }

  private void registerDefaultClasses() {
    register(Platform.HEAP_BYTE_BUFFER_CLASS);
    register(Platform.DIRECT_BYTE_BUFFER_CLASS);
    register(Comparator.naturalOrder().getClass());
    register(Comparator.reverseOrder().getClass());
    register(ConcurrentHashMap.class);
    register(ArrayBlockingQueue.class);
    register(LinkedBlockingQueue.class);
    register(AtomicBoolean.class);
    register(AtomicInteger.class);
    register(AtomicLong.class);
    register(AtomicReference.class);
    register(EnumSet.allOf(Language.class).getClass());
    register(EnumSet.of(Language.JAVA).getClass());
    register(Throwable.class, StackTraceElement.class, Exception.class, RuntimeException.class);
    register(NullPointerException.class);
    register(IOException.class);
    register(IllegalArgumentException.class);
    register(IllegalStateException.class);
    register(IndexOutOfBoundsException.class, ArrayIndexOutOfBoundsException.class);
  }

  /** register class. */
  public void register(Class<?> cls) {
    if (!extRegistry.registeredClassIdMap.containsKey(cls)) {
      while (extRegistry.classIdGenerator < registeredId2ClassInfo.length
          && registeredId2ClassInfo[extRegistry.classIdGenerator] != null) {
        extRegistry.classIdGenerator++;
      }
      register(cls, extRegistry.classIdGenerator);
    }
  }

  public void register(Class<?>... classes) {
    for (Class<?> cls : classes) {
      register(cls);
    }
  }

  public void register(Class<?> cls, boolean createSerializer) {
    register(cls);
    if (createSerializer) {
      getSerializer(cls);
    }
  }

  /** register class with given type tag which will be used for cross-language serialization. */
  public void register(Class<?> cls, String typeTag) {
    if (fury.getLanguage() == Language.JAVA) {
      throw new IllegalArgumentException(
          "Java serialization should register class by "
              + "Fury#register(Class) or Fury.register(Class<?>, Short)");
    }
    register(cls);
    Preconditions.checkArgument(!typeTagToClassXLangMap.containsKey(typeTag));
    addSerializer(cls, new StructSerializer<>(fury, cls, typeTag));
  }

  /**
   * Register class with specified id. Currently class id must be `classId >= 0 && classId < 32767`.
   * In the future this limitation may be relaxed.
   */
  public void register(Class<?> cls, int classId) {
    // class id must be less than Integer.MAX_VALUE/2 since we use bit 0 as class id flag.
    Preconditions.checkArgument(classId >= 0 && classId < Short.MAX_VALUE);
    if (extRegistry.registeredClassIdMap.containsKey(cls)) {
      throw new IllegalArgumentException(
          String.format(
              "Class %s already registered with id %s.",
              cls, extRegistry.registeredClassIdMap.get(cls)));
    }
    if (extRegistry.registeredClasses.containsKey(cls.getName())) {
      throw new IllegalArgumentException(
          String.format(
              "Class %s with name %s has been registered, registering class with same name are not allowed.",
              extRegistry.registeredClasses.get(cls.getName()), cls.getName()));
    }
    short id = (short) classId;
    if (id < registeredId2ClassInfo.length && registeredId2ClassInfo[id] != null) {
      throw new IllegalArgumentException(
          String.format(
              "Class %s with id %s has been registered, registering class %s with same id are not allowed.",
              registeredId2ClassInfo[id].getCls(), id, cls.getName()));
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
    extRegistry.classIdGenerator++;
  }

  public void register(Class<?> cls, Short id, boolean createSerializer) {
    register(cls, id);
    if (createSerializer) {
      getSerializer(cls);
    }
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

  /**
   * Mark non-inner registered final types as non-final to write class def for those types. Note if
   * a class is registered but not an inner class with inner serializer, it will still be taken as
   * non-final to write class def, so that it can be deserialized by the peer still..
   */
  public boolean isMonomorphic(Class<?> clz) {
    if (fury.getConfig().shareMetaContext()) {
      // can't create final map/collection type using TypeUtils.mapOf(TypeToken<K>,
      // TypeToken<V>)
      return ReflectionUtils.isMonomorphic(clz)
          && isInnerClass(clz)
          && (!Map.class.isAssignableFrom(clz) && !Collection.class.isAssignableFrom(clz));
    }
    return ReflectionUtils.isMonomorphic(clz);
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
   * Return true if the class has jdk `writeReplace`/`readResolve` method defined, which we need to
   * use {@link ReplaceResolveSerializer}.
   */
  public static boolean useReplaceResolveSerializer(Class<?> clz) {
    // FIXME class with `writeReplace` method defined should be Serializable,
    //  but hessian ignores this check and many existing system are using hessian.
    return (JavaSerializer.getWriteReplaceMethod(clz) != null)
        || JavaSerializer.getReadResolveMethod(clz) != null;
  }

  /**
   * Return true if a class satisfy following requirements.
   * <li>implements {@link Serializable}
   * <li>is not an {@link Enum}
   * <li>is not an array
   * <li>Doesn't have {@code readResolve}/{@code writePlace} method
   * <li>has {@code readObject}/{@code writeObject} method, but doesn't implements {@link
   *     Externalizable}
   * <li/>
   */
  public static boolean requireJavaSerialization(Class<?> clz) {
    if (clz.isEnum() || clz.isArray()) {
      return false;
    }
    if (ReflectionUtils.isDynamicGeneratedCLass(clz)) {
      // use corresponding serializer.
      return false;
    }
    if (!Serializable.class.isAssignableFrom(clz)) {
      return false;
    }
    if (useReplaceResolveSerializer(clz)) {
      return false;
    }
    if (Externalizable.class.isAssignableFrom(clz)) {
      return false;
    } else {
      // `AnnotationInvocationHandler#readObject` may invoke `toString` of object, which may be
      // risky.
      // For example, JsonObject#toString may invoke `getter`.
      // Use fury serialization to avoid this.
      if ("sun.reflect.annotation.AnnotationInvocationHandler".equals(clz.getName())) {
        return false;
      }
      return JavaSerializer.getReadObjectMethod(clz) != null
          || JavaSerializer.getWriteObjectMethod(clz) != null;
    }
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

  /** Set serializer for class whose name is {@code className}. */
  public void setSerializer(String className, Class<? extends Serializer> serializer) {
    for (Map.Entry<Class<?>, ClassInfo> entry : classInfoMap.iterable()) {
      if (extRegistry.registeredClasses.containsKey(className)) {
        LOG.warn("Skip clear serializer for registered class {}", className);
        return;
      }
      Class<?> cls = entry.getKey();
      if (cls.getName().equals(className)) {
        LOG.info("Clear serializer for class {}.", className);
        entry.getValue().serializer = Serializers.newSerializer(fury, cls, serializer);
        classInfoCache = NIL_CLASS_INFO;
        return;
      }
    }
  }

  /** Set serializer for classes starts with {@code classNamePrefix}. */
  public void setSerializers(String classNamePrefix, Class<? extends Serializer> serializer) {
    for (Map.Entry<Class<?>, ClassInfo> entry : classInfoMap.iterable()) {
      Class<?> cls = entry.getKey();
      String className = cls.getName();
      if (extRegistry.registeredClasses.containsKey(className)) {
        continue;
      }
      if (className.startsWith(classNamePrefix)) {
        LOG.info("Clear serializer for class {}.", className);
        entry.getValue().serializer = Serializers.newSerializer(fury, cls, serializer);
        classInfoCache = NIL_CLASS_INFO;
      }
    }
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
    short typeId = serializer.getXtypeId();
    if (typeId != Fury.NOT_SUPPORT_CROSS_LANGUAGE) {
      if (typeId > Fury.NOT_SUPPORT_CROSS_LANGUAGE) {
        typeIdToClassXLangMap.put(typeId, type);
      }
      if (typeId == Fury.FURY_TYPE_TAG_ID) {
        typeTag = serializer.getCrossLanguageTypeTag();
        typeTagToClassXLangMap.put(typeTag, type);
      }
    }

    // 1. Try to get ClassInfo from `registeredId2ClassInfo` and
    // `classInfoMap` or create a new `ClassInfo`.
    ClassInfo classInfo;
    Short classId = extRegistry.registeredClassIdMap.get(type);
    boolean registered = classId != null;
    // set serializer for class if it's registered by now.
    if (registered) {
      classInfo = registeredId2ClassInfo[classId];
    } else {
      if (serializer instanceof ReplaceResolveSerializer) {
        classId = REPLACE_STUB_ID;
      } else {
        classId = NO_CLASS_ID;
      }
      classInfo = classInfoMap.get(type);
    }

    if (classInfo == null || typeTag != null || classId != classInfo.classId) {
      classInfo = new ClassInfo(this, type, typeTag, null, classId);
      classInfoMap.put(type, classInfo);
      if (registered) {
        registeredId2ClassInfo[classId] = classInfo;
      }
    }

    // 2. Set `Serializer` for `ClassInfo`.
    classInfo.serializer = serializer;
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

  /**
   * Return serializer without generics for specified class. The cast of Serializer to subclass
   * serializer with generic is easy to raise compiler error for javac, so just use raw type.
   */
  @Internal
  @CodegenInvoke
  public Serializer<?> getRawSerializer(Class<?> cls) {
    Preconditions.checkNotNull(cls);
    return getOrUpdateClassInfo(cls).serializer;
  }

  public boolean isSerializable(Class<?> cls) {
    if (ReflectionUtils.isAbstract(cls) || cls.isInterface()) {
      return false;
    }
    try {
      getSerializerClass(cls, false);
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  public Class<? extends Serializer> getSerializerClass(Class<?> cls) {
    boolean codegen =
        supportCodegenForJavaSerialization(cls) && fury.getConfig().isCodeGenEnabled();
    return getSerializerClass(cls, codegen);
  }

  public Class<? extends Serializer> getSerializerClass(Class<?> cls, boolean codegen) {
    if (ReflectionUtils.isAbstract(cls) || cls.isInterface()) {
      throw new UnsupportedOperationException(
          String.format("Class %s doesn't support serialization.", cls));
    }
    Class<? extends Serializer> serializerClass = getSerializerClassFromGraalvmRegistry(cls);
    if (serializerClass != null) {
      return serializerClass;
    }
    cls = TypeUtils.boxedType(cls);
    ClassInfo classInfo = classInfoMap.get(cls);
    if (classInfo != null && classInfo.serializer != null) {
      // Note: need to check `classInfo.serializer != null`, because sometimes `cls` is already
      // serialized, which will create a class info with serializer null, see `#writeClassInternal`
      return classInfo.serializer.getClass();
    } else {
      if (cls.isEnum()) {
        return Serializers.EnumSerializer.class;
      } else if (Enum.class.isAssignableFrom(cls) && cls != Enum.class) {
        // handles an enum value that is an inner class. Eg: enum A {b{}};
        return Serializers.EnumSerializer.class;
      } else if (EnumSet.class.isAssignableFrom(cls)) {
        return CollectionSerializers.EnumSetSerializer.class;
      } else if (Charset.class.isAssignableFrom(cls)) {
        return Serializers.CharsetSerializer.class;
      } else if (cls.isArray()) {
        Preconditions.checkArgument(!cls.getComponentType().isPrimitive());
        return ArraySerializers.ObjectArraySerializer.class;
      } else if (Functions.isLambda(cls)) {
        return LambdaSerializer.class;
      } else if (ReflectionUtils.isJdkProxy(cls)) {
        return JdkProxySerializer.class;
      } else if (Calendar.class.isAssignableFrom(cls)) {
        return TimeSerializers.CalendarSerializer.class;
      } else if (ZoneId.class.isAssignableFrom(cls)) {
        return TimeSerializers.ZoneIdSerializer.class;
      } else if (TimeZone.class.isAssignableFrom(cls)) {
        return TimeSerializers.TimeZoneSerializer.class;
      } else if (ByteBuffer.class.isAssignableFrom(cls)) {
        return BufferSerializers.ByteBufferSerializer.class;
      }
      if (fury.getConfig().checkJdkClassSerializable()) {
        if (cls.getName().startsWith("java") && !(Serializable.class.isAssignableFrom(cls))) {
          throw new UnsupportedOperationException(
              String.format("Class %s doesn't support serialization.", cls));
        }
      }
      if (fury.getConfig().isScalaOptimizationEnabled()
          && ReflectionUtils.isScalaSingletonObject(cls)) {
        if (isCollection(cls)) {
          return SingletonCollectionSerializer.class;
        } else if (isMap(cls)) {
          return SingletonMapSerializer.class;
        } else {
          return SingletonObjectSerializer.class;
        }
      }
      if (isCollection(cls)) {
        // Serializer of common collection such as ArrayList/LinkedList should be registered
        // already.
        serializerClass = ChildContainerSerializers.getCollectionSerializerClass(cls);
        if (serializerClass != null) {
          return serializerClass;
        }
        if (requireJavaSerialization(cls) || useReplaceResolveSerializer(cls)) {
          return CollectionSerializers.JDKCompatibleCollectionSerializer.class;
        }
        if (fury.getLanguage() == Language.JAVA) {
          return CollectionSerializers.DefaultJavaCollectionSerializer.class;
        } else {
          return CollectionSerializer.class;
        }
      } else if (isMap(cls)) {
        // Serializer of common map such as HashMap/LinkedHashMap should be registered already.
        serializerClass = ChildContainerSerializers.getMapSerializerClass(cls);
        if (serializerClass != null) {
          return serializerClass;
        }
        if (requireJavaSerialization(cls) || useReplaceResolveSerializer(cls)) {
          return MapSerializers.JDKCompatibleMapSerializer.class;
        }
        if (fury.getLanguage() == Language.JAVA) {
          return MapSerializers.DefaultJavaMapSerializer.class;
        } else {
          return MapSerializer.class;
        }
      }
      if (fury.getLanguage() != Language.JAVA) {
        LOG.warn("Class {} isn't supported for cross-language serialization.", cls);
      }
      if (useReplaceResolveSerializer(cls)) {
        return ReplaceResolveSerializer.class;
      }
      if (Externalizable.class.isAssignableFrom(cls)) {
        return ExternalizableSerializer.class;
      }
      if (requireJavaSerialization(cls)) {
        return getJavaSerializer(cls);
      }
      Class<?> clz = cls;
      return getObjectSerializerClass(
          cls,
          metaContextShareEnabled,
          codegen,
          new JITContext.SerializerJITCallback<Class<? extends Serializer>>() {
            @Override
            public void onSuccess(Class<? extends Serializer> result) {
              setSerializer(clz, Serializers.newSerializer(fury, clz, result));
              if (classInfoCache.cls == clz) {
                classInfoCache = NIL_CLASS_INFO; // clear class info cache
              }
              Preconditions.checkState(getSerializer(clz).getClass() == result);
            }

            @Override
            public Object id() {
              return clz;
            }
          });
    }
  }

  public boolean isCollection(Class<?> cls) {
    if (Collection.class.isAssignableFrom(cls)) {
      return true;
    }
    if (fury.getConfig().isScalaOptimizationEnabled()) {
      // Scala map is scala iterable too.
      if (ScalaTypes.getScalaMapType().isAssignableFrom(cls)) {
        return false;
      }
      return ScalaTypes.getScalaIterableType().isAssignableFrom(cls);
    } else {
      return false;
    }
  }

  public boolean isMap(Class<?> cls) {
    return Map.class.isAssignableFrom(cls)
        || (fury.getConfig().isScalaOptimizationEnabled()
            && ScalaTypes.getScalaMapType().isAssignableFrom(cls));
  }

  public Class<? extends Serializer> getObjectSerializerClass(
      Class<?> cls, JITContext.SerializerJITCallback<Class<? extends Serializer>> callback) {
    boolean codegen =
        supportCodegenForJavaSerialization(cls) && fury.getConfig().isCodeGenEnabled();
    return getObjectSerializerClass(cls, false, codegen, callback);
  }

  private Class<? extends Serializer> getObjectSerializerClass(
      Class<?> cls,
      boolean shareMeta,
      boolean codegen,
      JITContext.SerializerJITCallback<Class<? extends Serializer>> callback) {
    if (codegen) {
      if (extRegistry.getClassCtx.contains(cls)) {
        // avoid potential recursive call for seq codec generation.
        return LazyInitBeanSerializer.class;
      } else {
        try {
          extRegistry.getClassCtx.add(cls);
          Class<? extends Serializer> sc;
          switch (fury.getCompatibleMode()) {
            case SCHEMA_CONSISTENT:
              sc =
                  fury.getJITContext()
                      .registerSerializerJITCallback(
                          () -> ObjectSerializer.class,
                          () -> loadCodegenSerializer(fury, cls),
                          callback);
              return sc;
            case COMPATIBLE:
              // If share class meta, compatible serializer won't be necessary, class
              // definition will be sent to peer to create serializer for deserialization.
              sc =
                  fury.getJITContext()
                      .registerSerializerJITCallback(
                          () -> shareMeta ? ObjectSerializer.class : CompatibleSerializer.class,
                          () ->
                              shareMeta
                                  ? loadCodegenSerializer(fury, cls)
                                  : loadCompatibleCodegenSerializer(fury, cls),
                          callback);
              return sc;
            default:
              throw new UnsupportedOperationException(
                  String.format("Unsupported mode %s", fury.getCompatibleMode()));
          }
        } finally {
          extRegistry.getClassCtx.remove(cls);
        }
      }
    } else {
      LOG.info("Object of type {} can't be serialized by jit", cls);
      switch (fury.getCompatibleMode()) {
        case SCHEMA_CONSISTENT:
          return ObjectSerializer.class;
        case COMPATIBLE:
          return shareMeta ? ObjectSerializer.class : CompatibleSerializer.class;
        default:
          throw new UnsupportedOperationException(
              String.format("Unsupported mode %s", fury.getCompatibleMode()));
      }
    }
  }

  public Class<? extends Serializer> getJavaSerializer(Class<?> clz) {
    if (Collection.class.isAssignableFrom(clz)) {
      return CollectionSerializers.JDKCompatibleCollectionSerializer.class;
    } else if (Map.class.isAssignableFrom(clz)) {
      return MapSerializers.JDKCompatibleMapSerializer.class;
    } else {
      if (useReplaceResolveSerializer(clz)) {
        return ReplaceResolveSerializer.class;
      }
      return fury.getDefaultJDKStreamSerializerType();
    }
  }

  public ClassChecker getClassChecker() {
    return extRegistry.classChecker;
  }

  public void setClassChecker(ClassChecker classChecker) {
    extRegistry.classChecker = classChecker;
  }

  public FieldResolver getFieldResolver(Class<?> cls) {
    // can't use computeIfAbsent, since there may be recursive multiple
    // `getFieldResolver` thus multiple updates, which cause concurrent
    // modification exception.
    FieldResolver fieldResolver = extRegistry.fieldResolverMap.get(cls);
    if (fieldResolver == null) {
      fieldResolver = FieldResolver.of(fury, cls);
      extRegistry.fieldResolverMap.put(cls, fieldResolver);
    }
    return fieldResolver;
  }

  // thread safe
  public SortedMap<Field, Descriptor> getAllDescriptorsMap(Class<?> clz, boolean searchParent) {
    // when jit thread query this, it is already built by serialization main thread.
    return extRegistry.descriptorsCache.computeIfAbsent(
        Tuple2.of(clz, searchParent), t -> Descriptor.getAllDescriptorsMap(clz, searchParent));
  }

  /**
   * Whether to track reference for this type. If false, reference tracing of subclasses may be
   * ignored too.
   */
  public boolean needToWriteRef(Class<?> cls) {
    if (fury.trackingRef()) {
      ClassInfo classInfo = getClassInfo(cls, false);
      if (classInfo == null || classInfo.serializer == null) {
        // TODO group related logic together for extendability and consistency.
        return !cls.isEnum();
      } else {
        return classInfo.serializer.needToWriteRef();
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

  /** Get classinfo by cache, update cache if miss. */
  public ClassInfo getClassInfo(Class<?> cls, ClassInfoHolder classInfoHolder) {
    ClassInfo classInfo = classInfoHolder.classInfo;
    if (classInfo.getCls() != cls) {
      classInfo = classInfoMap.get(cls);
      if (classInfo == null || classInfo.serializer == null) {
        addSerializer(cls, createSerializer(cls));
        classInfo = Objects.requireNonNull(classInfoMap.get(cls));
      }
      classInfoHolder.classInfo = classInfo;
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
    DisallowedList.checkNotInDisallowedList(cls.getName());
    if (!isSecure(cls)) {
      throw new InsecureException(generateSecurityMsg(cls));
    } else {
      if (!fury.getConfig().suppressClassRegistrationWarnings()
          && !Functions.isLambda(cls)
          && !ReflectionUtils.isJdkProxy(cls)
          && !extRegistry.registeredClassIdMap.containsKey(cls)
          && !shimDispatcher.contains(cls)) {
        LOG.warn(generateSecurityMsg(cls));
      }
    }

    if (extRegistry.serializerFactory != null) {
      Serializer serializer = extRegistry.serializerFactory.createSerializer(fury, cls);
      if (serializer != null) {
        return serializer;
      }
    }

    Serializer<?> shimSerializer = shimDispatcher.getSerializer(cls);
    if (shimSerializer != null) {
      return shimSerializer;
    }

    Class<? extends Serializer> serializerClass = getSerializerClass(cls);
    return Serializers.newSerializer(fury, cls, serializerClass);
  }

  private String generateSecurityMsg(Class<?> cls) {
    String tpl =
        "%s is not registered, please check whether it's the type you want to serialize or "
            + "a **vulnerability**. If safe, you should invoke `Fury#register` to register class, "
            + " which will have better performance by skipping classname serialization. "
            + "If your env is 100%% secure, you can also avoid this exception by disabling class "
            + "registration check using `FuryBuilder#requireClassRegistration(false)`";
    return String.format(tpl, cls);
  }

  private boolean isSecure(Class<?> cls) {
    if (extRegistry.registeredClassIdMap.containsKey(cls) || shimDispatcher.contains(cls)) {
      return true;
    }
    if (cls.isArray()) {
      return isSecure(TypeUtils.getArrayComponent(cls));
    }
    if (fury.getConfig().requireClassRegistration()) {
      return Functions.isLambda(cls)
          || ReflectionUtils.isJdkProxy(cls)
          || extRegistry.registeredClassIdMap.containsKey(cls)
          || shimDispatcher.contains(cls);
    } else {
      return extRegistry.classChecker.checkClass(this, cls.getName());
    }
    // Don't take java Exception as secure in case future JDK introduce insecure JDK exception.
    // if (Exception.class.isAssignableFrom(cls)
    //     && cls.getName().startsWith("java.")
    //     && !cls.getName().startsWith("java.sql")) {
    //   return true;
    // }
  }

  /**
   * Write class info to <code>buffer</code>. TODO(chaokunyang): The method should try to write
   * aligned data to reduce cpu instruction overhead. `writeClass` is the last step before
   * serializing object, if this writes are aligned, then later serialization will be more
   * efficient.
   */
  public void writeClassAndUpdateCache(MemoryBuffer buffer, Class<?> cls) {
    // fast path for common type
    if (cls == Integer.class) {
      buffer.writeVarUint32Small7(INTEGER_CLASS_ID << 1);
    } else if (cls == Long.class) {
      buffer.writeVarUint32Small7(LONG_CLASS_ID << 1);
    } else {
      writeClass(buffer, getOrUpdateClassInfo(cls));
    }
  }

  // The jit-compiled native code fot this method will be too big for inline, so we generated
  // `getClassInfo`
  // in fury-jit, see `BaseSeqCodecBuilder#writeAndGetClassInfo`
  // public ClassInfo writeClass(MemoryBuffer buffer, Class<?> cls, ClassInfoHolder classInfoHolder)
  // {
  //   ClassInfo classInfo = getClassInfo(cls, classInfoHolder);
  //   writeClass(buffer, classInfo);
  //   return classInfo;
  // }

  /** Write classname for java serialization. */
  public void writeClass(MemoryBuffer buffer, ClassInfo classInfo) {
    if (classInfo.classId == NO_CLASS_ID) { // no class id provided.
      // use classname
      if (metaContextShareEnabled) {
        buffer.writeByte(USE_CLASS_VALUE_FLAG);
        // FIXME(chaokunyang) Register class but not register serializer can't be used with
        //  meta share mode, because no class def are sent to peer.
        writeClassWithMetaShare(buffer, classInfo);
      } else {
        // if it's null, it's a bug.
        assert classInfo.packageNameBytes != null;
        metaStringResolver.writeMetaStringBytesWithFlag(buffer, classInfo.packageNameBytes);
        assert classInfo.classNameBytes != null;
        metaStringResolver.writeMetaStringBytes(buffer, classInfo.classNameBytes);
      }
    } else {
      // use classId
      buffer.writeVarUint32(classInfo.classId << 1);
    }
  }

  public void writeClassWithMetaShare(MemoryBuffer buffer, ClassInfo classInfo) {
    MetaContext metaContext = fury.getSerializationContext().getMetaContext();
    Preconditions.checkNotNull(
        metaContext,
        "Meta context must be set before serialization,"
            + " please set meta context by SerializationContext.setMetaContext");
    IdentityObjectIntMap<Class<?>> classMap = metaContext.classMap;
    int newId = classMap.size;
    int id = classMap.putOrGet(classInfo.cls, newId);
    if (id >= 0) {
      buffer.writeVarUint32(id);
    } else {
      buffer.writeVarUint32(newId);
      ClassDef classDef;
      Serializer<?> serializer = classInfo.serializer;
      Preconditions.checkArgument(serializer.getClass() != UnexistedClassSerializer.class);
      if (fury.getConfig().getCompatibleMode() == CompatibleMode.COMPATIBLE
          && (serializer instanceof Generated.GeneratedObjectSerializer
              // May already switched to MetaSharedSerializer when update class info cache.
              || serializer instanceof Generated.GeneratedMetaSharedSerializer
              || serializer instanceof LazyInitBeanSerializer
              || serializer instanceof ObjectSerializer
              || serializer instanceof MetaSharedSerializer)) {
        classDef =
            classDefMap.computeIfAbsent(classInfo.cls, cls -> ClassDef.buildClassDef(cls, fury));
      } else {
        classDef =
            classDefMap.computeIfAbsent(
                classInfo.cls,
                cls ->
                    ClassDef.buildClassDef(
                        this,
                        cls,
                        new ArrayList<>(),
                        ImmutableMap.of(META_SHARE_FIELDS_INFO_KEY, "false")));
      }
      metaContext.writingClassDefs.add(classDef);
    }
  }

  private Class<?> readClassWithMetaShare(MemoryBuffer buffer) {
    MetaContext metaContext = fury.getSerializationContext().getMetaContext();
    Preconditions.checkNotNull(
        metaContext,
        "Meta context must be set before serialization,"
            + " please set meta context by SerializationContext.setMetaContext");
    int id = buffer.readVarUint32Small14();
    List<ClassInfo> readClassInfos = metaContext.readClassInfos;
    ClassInfo classInfo = readClassInfos.get(id);
    if (classInfo == null) {
      List<ClassDef> readClassDefs = metaContext.readClassDefs;
      ClassDef classDef = readClassDefs.get(id);
      Class<?> cls = loadClass(classDef.getClassName());
      classInfo = getClassInfo(cls, false);
      if (classInfo == null) {
        Short classId = extRegistry.registeredClassIdMap.get(cls);
        classInfo = new ClassInfo(this, cls, null, null, classId == null ? NO_CLASS_ID : classId);
        classInfoMap.put(cls, classInfo);
      }
      readClassInfos.set(id, classInfo);
    }
    return classInfo.cls;
  }

  private ClassInfo readClassInfoWithMetaShare(MemoryBuffer buffer, MetaContext metaContext) {
    Preconditions.checkNotNull(
        metaContext,
        "Meta context must be set before serialization,"
            + " please set meta context by SerializationContext.setMetaContext");
    int id = buffer.readVarUint32Small14();
    List<ClassInfo> readClassInfos = metaContext.readClassInfos;
    ClassInfo classInfo = readClassInfos.get(id);
    if (classInfo == null) {
      List<ClassDef> readClassDefs = metaContext.readClassDefs;
      ClassDef classDef = readClassDefs.get(id);
      if ("false".equals(classDef.getExtMeta().getOrDefault(META_SHARE_FIELDS_INFO_KEY, ""))) {
        Class<?> cls = loadClass(classDef.getClassName());
        classInfo = getClassInfo(cls);
      } else {
        Tuple2<ClassDef, ClassInfo> classDefTuple = extRegistry.classIdToDef.get(classDef.getId());
        if (classDefTuple == null || classDefTuple.f1 == null) {
          if (classDefTuple != null) {
            classDef = classDefTuple.f0;
          }
          Class<?> cls = loadClass(classDef.getClassName());
          classInfo = getMetaSharedClassInfo(classDef, cls);
          // Share serializer for same version class def to avoid too much different meta
          // context take up too much memory.
          extRegistry.classIdToDef.put(classDef.getId(), Tuple2.of(classDef, classInfo));
        } else {
          classInfo = classDefTuple.f1;
        }
      }
      readClassInfos.set(id, classInfo);
    }
    return classInfo;
  }

  // TODO(chaokunyang) if ClassDef is consistent with class in this process,
  //  use existing serializer instead.
  private ClassInfo getMetaSharedClassInfo(ClassDef classDef, Class<?> clz) {
    if (clz == UnexistedSkipClass.class) {
      clz = UnexistedMetaSharedClass.class;
    }
    Class<?> cls = clz;
    Short classId = extRegistry.registeredClassIdMap.get(cls);
    ClassInfo classInfo =
        new ClassInfo(this, cls, null, null, classId == null ? NO_CLASS_ID : classId);
    if (cls == UnexistedMetaSharedClass.class) {
      classInfo.serializer = new UnexistedClassSerializer(fury, classDef);
      // ensure `UnexistedMetaSharedClass` registered to write fixed-length class def,
      // so we can rewrite it in `UnexistedClassSerializer`.
      Preconditions.checkNotNull(classId);
      return classInfo;
    }
    Class<? extends Serializer> sc =
        fury.getJITContext()
            .registerSerializerJITCallback(
                () -> MetaSharedSerializer.class,
                () -> CodecUtils.loadOrGenMetaSharedCodecClass(fury, cls, classDef),
                c -> classInfo.serializer = Serializers.newSerializer(fury, cls, c));
    if (sc == MetaSharedSerializer.class) {
      classInfo.serializer = new MetaSharedSerializer(fury, cls, classDef);
    } else {
      classInfo.serializer = Serializers.newSerializer(fury, cls, sc);
    }
    return classInfo;
  }

  /**
   * Write all new class definitions meta to buffer at last, so that if some class doesn't exist on
   * peer, but one of class which exists on both side are sent in this stream, the definition meta
   * can still be stored in peer, and can be resolved next time when sent only an id.
   */
  public void writeClassDefs(MemoryBuffer buffer) {
    MetaContext metaContext = fury.getSerializationContext().getMetaContext();
    buffer.writeVarUint32Small7(metaContext.writingClassDefs.size());
    for (ClassDef classDef : metaContext.writingClassDefs) {
      classDef.writeClassDef(buffer);
    }
    metaContext.writingClassDefs.clear();
  }

  /**
   * Ensure all class definition are read and populated, even there are deserialization exception
   * such as ClassNotFound. So next time a class def written previously identified by an id can be
   * got from the meta context.
   */
  public void readClassDefs(MemoryBuffer buffer) {
    MetaContext metaContext = fury.getSerializationContext().getMetaContext();
    int classDefOffset = buffer.readInt32();
    int readerIndex = buffer.readerIndex();
    buffer.readerIndex(classDefOffset);
    int numClassDefs = buffer.readVarUint32Small14();
    for (int i = 0; i < numClassDefs; i++) {
      ClassDef readClassDef = ClassDef.readClassDef(buffer);
      // Share same class def to reduce memory footprint, since there may be many meta context.
      ClassDef classDef =
          extRegistry.classIdToDef.computeIfAbsent(
                  readClassDef.getId(), key -> Tuple2.of(readClassDef, null))
              .f0;
      metaContext.readClassDefs.add(classDef);
      // Will be set lazily, so even some classes doesn't exist, remaining classinfo
      // can be created still.
      metaContext.readClassInfos.add(null);
    }
    buffer.readerIndex(readerIndex);
  }

  /**
   * Native code for ClassResolver.writeClass is too big to inline, so inline it manually.
   *
   * <p>See `already compiled into a big method` in <a
   * href="https://wiki.openjdk.org/display/HotSpot/Server+Compiler+Inlining+Messages">Server+Compiler+Inlining+Messages</a>
   */
  // Note: Thread safe fot jit thread to call.
  public Expression writeClassExpr(
      Expression classResolverRef, Expression buffer, Expression classInfo) {
    return new Invoke(classResolverRef, "writeClass", buffer, classInfo);
  }

  // Note: Thread safe fot jit thread to call.
  public Expression writeClassExpr(Expression buffer, short classId) {
    Preconditions.checkArgument(classId != NO_CLASS_ID);
    return writeClassExpr(buffer, Literal.ofShort(classId));
  }

  // Note: Thread safe fot jit thread to call.
  public Expression writeClassExpr(Expression buffer, Expression classId) {
    return new Invoke(buffer, "writeVarUint32", new Expression.BitShift("<<", classId, 1));
  }

  // Note: Thread safe fot jit thread to call.
  public Expression skipRegisteredClassExpr(Expression buffer) {
    return new Invoke(buffer, "readVarUint32Small14");
  }

  /**
   * Write classname for java serialization. Note that the object of provided class can be
   * non-serializable, and class with writeReplace/readResolve defined won't be skipped. For
   * serializable object, {@link #writeClass(MemoryBuffer, ClassInfo)} should be invoked.
   */
  public void writeClassInternal(MemoryBuffer buffer, Class<?> cls) {
    ClassInfo classInfo = classInfoMap.get(cls);
    if (classInfo == null) {
      Short classId = extRegistry.registeredClassIdMap.get(cls);
      // Don't create serializer in case the object for class is non-serializable,
      // Or class is abstract or interface.
      classInfo = new ClassInfo(this, cls, null, null, classId == null ? NO_CLASS_ID : classId);
      classInfoMap.put(cls, classInfo);
    }
    short classId = classInfo.classId;
    if (classId == REPLACE_STUB_ID) {
      // clear class id to avoid replaced class written as
      // ReplaceResolveSerializer.ReplaceStub
      classInfo.classId = NO_CLASS_ID;
    }
    writeClass(buffer, classInfo);
    classInfo.classId = classId;
  }

  /**
   * Read serialized java classname. Note that the object of the class can be non-serializable. For
   * serializable object, {@link #readClassInfo(MemoryBuffer)} or {@link
   * #readClassInfo(MemoryBuffer, ClassInfoHolder)} should be invoked.
   */
  public Class<?> readClassInternal(MemoryBuffer buffer) {
    int header = buffer.readVarUint32Small14();
    if ((header & 0b1) != 0) {
      if (metaContextShareEnabled) {
        return readClassWithMetaShare(buffer);
      }
      MetaStringBytes packageBytes = metaStringResolver.readMetaStringBytesWithFlag(buffer, header);
      MetaStringBytes simpleClassNameBytes = metaStringResolver.readMetaStringBytes(buffer);
      final Class<?> cls = loadBytesToClass(packageBytes, simpleClassNameBytes);
      currentReadClass = cls;
      return cls;
    } else {
      ClassInfo classInfo = registeredId2ClassInfo[(short) (header >> 1)];
      final Class<?> cls = classInfo.cls;
      currentReadClass = cls;
      return cls;
    }
  }

  /**
   * Read class info from java data <code>buffer</code>. {@link #readClassInfo(MemoryBuffer,
   * ClassInfo)} is faster since it use a non-global class info cache.
   */
  public ClassInfo readClassInfo(MemoryBuffer buffer) {
    int header = buffer.readVarUint32Small14();
    if ((header & 0b1) != 0) {
      ClassInfo classInfo;
      if (metaContextShareEnabled) {
        classInfo =
            readClassInfoWithMetaShare(buffer, fury.getSerializationContext().getMetaContext());
      } else {
        classInfo = readClassInfoFromBytes(buffer, classInfoCache, header);
      }
      classInfoCache = classInfo;
      currentReadClass = classInfo.cls;
      return classInfo;
    } else {
      ClassInfo classInfo = getOrUpdateClassInfo((short) (header >> 1));
      currentReadClass = classInfo.cls;
      return classInfo;
    }
  }

  /**
   * Read class info from java data <code>buffer</code>. `classInfoCache` is used as a cache to
   * reduce map lookup to load class from binary.
   */
  @CodegenInvoke
  public ClassInfo readClassInfo(MemoryBuffer buffer, ClassInfo classInfoCache) {
    int header = buffer.readVarUint32Small14();
    if ((header & 0b1) != 0) {
      return readClassInfoByCache(buffer, classInfoCache, header);
    } else {
      return getClassInfo((short) (header >> 1));
    }
  }

  /** Read class info, update classInfoHolder if cache not hit. */
  @CodegenInvoke
  public ClassInfo readClassInfo(MemoryBuffer buffer, ClassInfoHolder classInfoHolder) {
    int header = buffer.readVarUint32Small14();
    if ((header & 0b1) != 0) {
      return readClassInfoFromBytes(buffer, classInfoHolder, header);
    } else {
      return getClassInfo((short) (header >> 1));
    }
  }

  private ClassInfo readClassInfoByCache(
      MemoryBuffer buffer, ClassInfo classInfoCache, int header) {
    if (metaContextShareEnabled) {
      return readClassInfoWithMetaShare(buffer, fury.getSerializationContext().getMetaContext());
    }
    return readClassInfoFromBytes(buffer, classInfoCache, header);
  }

  private ClassInfo readClassInfoFromBytes(
      MemoryBuffer buffer, ClassInfoHolder classInfoHolder, int header) {
    if (metaContextShareEnabled) {
      return readClassInfoWithMetaShare(buffer, fury.getSerializationContext().getMetaContext());
    }
    ClassInfo classInfo = readClassInfoFromBytes(buffer, classInfoHolder.classInfo, header);
    classInfoHolder.classInfo = classInfo;
    return classInfo;
  }

  private ClassInfo readClassInfoFromBytes(
      MemoryBuffer buffer, ClassInfo classInfoCache, int header) {
    MetaStringBytes simpleClassNameBytesCache = classInfoCache.classNameBytes;
    if (simpleClassNameBytesCache != null) {
      MetaStringBytes packageNameBytesCache = classInfoCache.packageNameBytes;
      MetaStringBytes packageBytes =
          metaStringResolver.readMetaStringBytesWithFlag(buffer, packageNameBytesCache, header);
      assert packageNameBytesCache != null;
      MetaStringBytes simpleClassNameBytes =
          metaStringResolver.readMetaStringBytes(buffer, simpleClassNameBytesCache);
      if (simpleClassNameBytesCache.hashCode == simpleClassNameBytes.hashCode
          && packageNameBytesCache.hashCode == packageBytes.hashCode) {
        return classInfoCache;
      } else {
        Class<?> cls = loadBytesToClass(packageBytes, simpleClassNameBytes);
        return getClassInfo(cls);
      }
    } else {
      MetaStringBytes packageBytes = metaStringResolver.readMetaStringBytesWithFlag(buffer, header);
      MetaStringBytes simpleClassNameBytes = metaStringResolver.readMetaStringBytes(buffer);
      Class<?> cls = loadBytesToClass(packageBytes, simpleClassNameBytes);
      return getClassInfo(cls);
    }
  }

  private Class<?> loadBytesToClass(
      MetaStringBytes packageBytes, MetaStringBytes simpleClassNameBytes) {
    ClassNameBytes classNameBytes =
        new ClassNameBytes(packageBytes.hashCode, simpleClassNameBytes.hashCode);
    Class<?> cls = compositeClassNameBytes2Class.get(classNameBytes);
    if (cls == null) {
      String packageName = packageBytes.decode('.', '_');
      String className = simpleClassNameBytes.decode('.', '$');
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

  public void xwriteClass(MemoryBuffer buffer, Class<?> cls) {
    metaStringResolver.writeMetaStringBytes(buffer, getOrUpdateClassInfo(cls).fullClassNameBytes);
  }

  public void xwriteTypeTag(MemoryBuffer buffer, Class<?> cls) {
    metaStringResolver.writeMetaStringBytes(buffer, getOrUpdateClassInfo(cls).typeTagBytes);
  }

  public Class<?> xreadClass(MemoryBuffer buffer) {
    MetaStringBytes byteString = metaStringResolver.readMetaStringBytes(buffer);
    Class<?> cls = classNameBytes2Class.get(byteString);
    if (cls == null) {
      Preconditions.checkNotNull(byteString);
      String className = byteString.decode('.', '_');
      cls = loadClass(className);
      classNameBytes2Class.put(byteString, cls);
    }
    currentReadClass = cls;
    return cls;
  }

  public String xreadClassName(MemoryBuffer buffer) {
    return metaStringResolver.readMetaString(buffer);
  }

  public Class<?> getCurrentReadClass() {
    return currentReadClass;
  }

  private Class<?> loadClass(String className) {
    extRegistry.classChecker.checkClass(this, className);
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
        if (fury.getConfig().deserializeUnexistedClass()) {
          LOG.warn(msg);
          // FIXME create a subclass dynamically may be better?
          return UnexistedSkipClass.class;
        }
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
    String tag = metaStringResolver.readMetaString(buffer);
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

  public GenericType buildGenericType(TypeToken<?> typeToken) {
    return GenericType.build(
        typeToken.getType(),
        t -> {
          if (t.getClass() == Class.class) {
            return isMonomorphic((Class<?>) t);
          } else {
            return isMonomorphic(getRawType(t));
          }
        });
  }

  public GenericType buildGenericType(Type type) {
    return GenericType.build(
        type,
        t -> {
          if (t.getClass() == Class.class) {
            return isMonomorphic((Class<?>) t);
          } else {
            return isMonomorphic(getRawType(t));
          }
        });
  }

  public GenericType getObjectGenericType() {
    return extRegistry.objectGenericType;
  }

  public ClassInfo newClassInfo(Class<?> cls, Serializer<?> serializer, short classId) {
    return new ClassInfo(this, cls, null, serializer, classId);
  }

  // Invoked by fury JIT.
  public ClassInfo nilClassInfo() {
    return new ClassInfo(this, null, null, null, NO_CLASS_ID);
  }

  public ClassInfoHolder nilClassInfoHolder() {
    return new ClassInfoHolder(nilClassInfo());
  }

  public boolean isPrimitive(short classId) {
    return classId >= PRIMITIVE_VOID_CLASS_ID && classId <= PRIMITIVE_DOUBLE_CLASS_ID;
  }

  public MetaStringResolver getMetaStringResolver() {
    return metaStringResolver;
  }

  public CodeGenerator getCodeGenerator(ClassLoader... loaders) {
    List<ClassLoader> loaderList = new ArrayList<>(loaders.length);
    Collections.addAll(loaderList, loaders);
    return extRegistry.codeGeneratorMap.get(loaderList);
  }

  public void setCodeGenerator(ClassLoader loader, CodeGenerator codeGenerator) {
    setCodeGenerator(new ClassLoader[] {loader}, codeGenerator);
  }

  public void setCodeGenerator(ClassLoader[] loaders, CodeGenerator codeGenerator) {
    extRegistry.codeGeneratorMap.put(Arrays.asList(loaders), codeGenerator);
  }

  public Fury getFury() {
    return fury;
  }

  private static final ConcurrentMap<Integer, List<ClassResolver>> GRAALVM_REGISTRY =
      new ConcurrentHashMap<>();

  // CHECKSTYLE.OFF:MethodName
  public static void _addGraalvmClassRegistry(int furyConfigHash, ClassResolver classResolver) {
    // CHECKSTYLE.ON:MethodName
    if (GraalvmSupport.isGraalBuildtime()) {
      List<ClassResolver> resolvers =
          GRAALVM_REGISTRY.computeIfAbsent(
              furyConfigHash, k -> Collections.synchronizedList(new ArrayList<>()));
      resolvers.add(classResolver);
    }
  }

  private Class<? extends Serializer> getSerializerClassFromGraalvmRegistry(Class<?> cls) {
    List<ClassResolver> classResolvers = GRAALVM_REGISTRY.get(fury.getConfig().getConfigHash());
    if (classResolvers == null || classResolvers.isEmpty()) {
      return null;
    }
    for (ClassResolver classResolver : classResolvers) {
      if (classResolver != this) {
        ClassInfo classInfo = classResolver.classInfoMap.get(cls);
        if (classInfo != null) {
          return classInfo.serializer.getClass();
        }
      }
    }
    if (GraalvmSupport.isGraalRuntime()) {
      if (Functions.isLambda(cls) || ReflectionUtils.isJdkProxy(cls)) {
        return null;
      }
      throw new RuntimeException(String.format("Class %s is not registered", cls));
    }
    return null;
  }
}
