package org.apache.fury.resolver;

import static org.apache.fury.meta.Encoders.GENERIC_ENCODER;
import static org.apache.fury.meta.Encoders.PACKAGE_DECODER;
import static org.apache.fury.meta.Encoders.TYPE_NAME_DECODER;
import static org.apache.fury.resolver.ClassResolver.NO_CLASS_ID;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.fury.collection.LongMap;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.Platform;
import org.apache.fury.meta.Encoders;
import org.apache.fury.meta.MetaString;
import org.apache.fury.serializer.EnumSerializer;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.StructSerializer;
import org.apache.fury.type.Types;
import org.apache.fury.util.Preconditions;

// TODO(chaokunyang) Abstract type resolver for java/xlang type resolution.
public class XtypeResolver {
  private static final float loadFactor = 0.5f;
  // Most systems won't have so many types for serialization.
  private static final int MAX_TYPE_ID = 4096;

  private final ClassResolver classResolver;
  private final MetaStringResolver metaStringResolver;
  private int xtypeIdGenerator = 64;

  // Use ClassInfo[] or LongMap?
  // ClassInfo[] is faster, but we can't have bigger type id.
  private final LongMap<ClassInfo> xtypeIdToClassMap = new LongMap<>(8, loadFactor);
  private final Set<Integer> registeredTypeIds = new HashSet<>();

  public XtypeResolver(ClassResolver classResolver) {
    this.classResolver = classResolver;
    this.metaStringResolver = classResolver.getMetaStringResolver();
    registerDefaultTypes();
  }

  public void register(Class<?> type) {
    while (registeredTypeIds.contains(xtypeIdGenerator)) {
      xtypeIdGenerator++;
    }
    register(type, xtypeIdGenerator++);
  }

  public void register(Class<?> type, int typeId) {
    // ClassInfo[] has length of max type id. If the type id is too big, Fury will waste many
    // memory.
    // We can relax this limit in the future.
    Preconditions.checkArgument(typeId < MAX_TYPE_ID, "Too big type id %s", typeId);
    ClassInfo classInfo = classResolver.getClassInfo(type, false);
    Serializer<?> serializer = null;
    if (classInfo != null) {
      serializer = classInfo.serializer;
      if (classInfo.xtypeId != 0) {
        throw new IllegalArgumentException(
            String.format("Type %s has been registered with id %s", type, classInfo.xtypeId));
      }
      String prevNamespace = decodeNamespace(classInfo.packageNameBytes);
      String prevTypeName = decodeTypeName(classInfo.classNameBytes);
      if (!type.getSimpleName().equals(prevTypeName)) {
        throw new IllegalArgumentException(
            String.format(
                "Type %s has been registered with namespace %s type %s",
                type, prevNamespace, prevTypeName));
      }
    }
    int xtypeId = typeId;
    if (type.isEnum()) {
      xtypeId = xtypeId << 8 + Types.ENUM;

    } else {
      if (serializer != null) {
        if (serializer instanceof StructSerializer) {
          xtypeId = xtypeId << 8 + Types.STRUCT;
        } else {
          xtypeId = xtypeId << 8 + Types.EXT;
        }
      }
    }
    register(type, serializer, xtypeId);
  }

  public void register(Class<?> type, String namespace, String typeName) {
    Preconditions.checkArgument(
        !typeName.contains("."),
        "Typename %s should not contains `.`, please put it into namespace",
        typeName);
    ClassInfo classInfo = classResolver.getClassInfo(type, false);
    Serializer<?> serializer = null;
    if (classInfo != null) {
      serializer = classInfo.serializer;
      if (classInfo.classNameBytes != null) {
        String prevNamespace = decodeNamespace(classInfo.packageNameBytes);
        String prevTypeName = decodeTypeName(classInfo.classNameBytes);
        if (!namespace.equals(prevNamespace) || typeName.equals(prevTypeName)) {
          throw new IllegalArgumentException(
              String.format(
                  "Type %s has been registered with namespace %s type %s",
                  type, prevNamespace, prevTypeName));
        }
      }
    }
    short xtypeId = -1;
    if (type.isEnum()) {
      xtypeId = Types.NS_ENUM;
    } else {
      if (serializer != null) {
        if (serializer instanceof StructSerializer) {
          xtypeId = Types.NS_STRUCT;
        } else {
          xtypeId = Types.NS_EXT;
        }
      }
    }
    MetaStringBytes fullClassNameBytes =
        metaStringResolver.getOrCreateMetaStringBytes(
            GENERIC_ENCODER.encode(type.getName(), MetaString.Encoding.UTF_8));
    MetaStringBytes nsBytes =
        metaStringResolver.getOrCreateMetaStringBytes(Encoders.encodePackage(namespace));
    MetaStringBytes classNameBytes =
        metaStringResolver.getOrCreateMetaStringBytes(Encoders.encodeTypeName(typeName));
    ClassInfo info =
        new ClassInfo(
            type,
            fullClassNameBytes,
            nsBytes,
            classNameBytes,
            false,
            serializer,
            NO_CLASS_ID,
            xtypeId);
    classResolver.setClassInfo(type, info);
    register(type, serializer, xtypeId);
  }

  private String decodeNamespace(MetaStringBytes packageNameBytes) {
    return packageNameBytes.decode(PACKAGE_DECODER);
  }

  private String decodeTypeName(MetaStringBytes classNameBytes) {
    return classNameBytes.decode(TYPE_NAME_DECODER);
  }

  private void register(Class<?> type, Serializer<?> serializer, int xtypeId) {
    if (serializer == null) {
      if (type.isEnum()) {
        classResolver.registerSerializer(
            type, new EnumSerializer(classResolver.getFury(), (Class<Enum>) type));
      } else {
        classResolver.registerSerializer(
            type, new StructSerializer<>(classResolver.getFury(), type));
      }
    }
    registeredTypeIds.add(xtypeId);
    ClassInfo classInfo = classResolver.getClassInfo(type);
    xtypeIdToClassMap.put(xtypeId, classInfo);
    classInfo.xtypeId = xtypeId;
  }

  public <T> void registerSerializer(Class<T> type, Class<? extends Serializer> serializerClass) {
    checkClassRegistration(type);
    classResolver.registerSerializer(type, serializerClass);
  }

  public void registerSerializer(Class<?> type, Serializer<?> serializer) {
    checkClassRegistration(type);
    classResolver.registerSerializer(type, serializer);
  }

  private void checkClassRegistration(Class<?> type) {
    ClassInfo classInfo = classResolver.getClassInfo(type, false);
    Preconditions.checkArgument(
        classInfo != null
            && (classInfo.xtypeId != 0
                || !type.getSimpleName().equals(decodeTypeName(classInfo.classNameBytes))),
        "Type %s should be registered with id or namespace+typename before register serializer",
        type);
  }

  private void registerDefaultTypes() {
    registerDefaultTypes(Types.BOOL, Boolean.class, boolean.class, AtomicBoolean.class);
    registerDefaultTypes(Types.INT8, Byte.class, byte.class);
    registerDefaultTypes(Types.INT16, Short.class, short.class);
    registerDefaultTypes(Types.INT32, Integer.class, int.class, AtomicInteger.class);
    registerDefaultTypes(Types.INT64, Long.class, long.class, AtomicLong.class);
    registerDefaultTypes(Types.FLOAT32, Float.class, float.class);
    registerDefaultTypes(Types.FLOAT64, Double.class, double.class);
    registerDefaultTypes(Types.STRING, String.class);
    registerDefaultTypes(Types.DURATION, Duration.class);
    registerDefaultTypes(
        Types.TIMESTAMP, Instant.class, Date.class, Timestamp.class, LocalDateTime.class);
    registerDefaultTypes(Types.DECIMAL, BigDecimal.class, BigInteger.class);
    registerDefaultTypes(
        Types.BINARY,
        byte[].class,
        Platform.HEAP_BYTE_BUFFER_CLASS,
        Platform.DIRECT_BYTE_BUFFER_CLASS);
    registerDefaultTypes(Types.BOOL_ARRAY, boolean[].class);
    registerDefaultTypes(Types.INT16_ARRAY, short[].class);
    registerDefaultTypes(Types.INT32_ARRAY, int[].class);
    registerDefaultTypes(Types.INT64_ARRAY, long[].class);
    registerDefaultTypes(Types.FLOAT32_ARRAY, float[].class);
    registerDefaultTypes(Types.FLOAT64_ARRAY, double[].class);
    registerDefaultTypes(Types.LIST, ArrayList.class, Object[].class);
    registerDefaultTypes(Types.SET, HashSet.class, LinkedHashSet.class);
    registerDefaultTypes(Types.MAP, HashMap.class, LinkedHashMap.class);
  }

  private void registerDefaultTypes(int xtypeId, Class<?> defaultType, Class<?>... otherTypes) {
    internalRegister(defaultType, xtypeId);
    xtypeIdToClassMap.put(xtypeId, classResolver.getClassInfo(defaultType));
    for (Class<?> otherType : otherTypes) {
      internalRegister(otherType, xtypeId);
    }
  }

  private void internalRegister(Class<?> type, int xtypeId) {
    ClassInfo classInfo = classResolver.getClassInfo(type);
    Preconditions.checkArgument(
      classInfo.xtypeId == 0, "Type %s has be registered with id %s", type, classInfo.xtypeId);
    classInfo.xtypeId = xtypeId;
  }

  public ClassInfo writeClassInfo(MemoryBuffer buffer, Object obj) {
    ClassInfo classInfo = classResolver.getOrUpdateClassInfo(obj.getClass());
    int xtypeId = classInfo.getXtypeId();
    byte internalTypeId = (byte) xtypeId;
    buffer.writeVarUint32Small7(xtypeId);
    switch (internalTypeId) {
      case Types.NS_ENUM:
      case Types.NS_STRUCT:
      case Types.NS_EXT:
        assert classInfo.packageNameBytes != null;
        metaStringResolver.writeMetaStringBytes(buffer, classInfo.packageNameBytes);
        assert classInfo.classNameBytes != null;
        metaStringResolver.writeMetaStringBytes(buffer, classInfo.classNameBytes);
        break;
    }
    return classInfo;
  }

  public ClassInfo readClassInfo(MemoryBuffer buffer) {
    long xtypeId = buffer.readVarUint32Small14();
    byte internalTypeId = (byte) xtypeId;
    switch (internalTypeId) {
      case Types.NS_ENUM:
      case Types.NS_STRUCT:
      case Types.NS_COMPATIBLE_STRUCT:
      case Types.NS_EXT:
        MetaStringBytes packageBytes = metaStringResolver.readMetaStringBytes(buffer);
        MetaStringBytes simpleClassNameBytes = metaStringResolver.readMetaStringBytes(buffer);
        return classResolver.loadBytesToClassInfo(packageBytes, simpleClassNameBytes);
      default:
        return xtypeIdToClassMap.get(xtypeId);
    }
  }
}
