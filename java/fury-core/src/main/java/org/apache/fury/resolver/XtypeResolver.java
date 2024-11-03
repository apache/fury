package org.apache.fury.resolver;

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
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.fury.collection.LongMap;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.Platform;
import org.apache.fury.serializer.EnumSerializer;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.StructSerializer;
import org.apache.fury.type.Types;
import org.apache.fury.util.Preconditions;

// TODO(chaokunyang) Abstract type resolver for java/xlang type resolution.
public class XtypeResolver {
  private static final float loadFactor = 0.5f;

  private final ClassResolver classResolver;
  private final MetaStringResolver metaStringResolver;
  private int xtypeIdGenerator = 64;

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
    // We can relax this limit in the future, but currently we keep it in small range for future
    // extension.
    Preconditions.checkArgument(typeId < Short.MAX_VALUE, "Too big type id %s", typeId);
    ClassInfo classInfo = classResolver.getClassInfo(type, false);
    Serializer<?> serializer = null;
    if (classInfo != null) {
      serializer = classInfo.serializer;
      if (classInfo.xtypeId != 0) {
        throw new IllegalArgumentException(
            String.format("Type %s has been registered with id %s", type, classInfo.xtypeId));
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
    classInfo = classResolver.getClassInfo(type);
    xtypeIdToClassMap.put(xtypeId, classInfo);
    internalRegister(type, xtypeId);
  }

  public void register(Class<?> cls, String namespace, String typeName) {
    Preconditions.checkArgument(
        !typeName.contains("."),
        "Typename %s should not contains `.`, please put it into namespace",
        typeName);
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
    registerDefaultTypes(Types.SET, HashSet.class);
    registerDefaultTypes(Types.MAP, HashMap.class);
  }

  public <T> void registerSerializer(Class<T> type, Class<? extends Serializer> serializerClass) {
    classResolver.registerSerializer(type, serializerClass);

    if (classResolver.getClassInfo(type).xtypeId == 0) {
      register(type);
    }
  }

  public void registerSerializer(Class<?> type, Serializer<?> serializer) {
    classResolver.registerSerializer(type, serializer);
  }

  public ClassInfo writeClassInfo(MemoryBuffer buffer, Object obj) {
    ClassInfo classInfo = classResolver.getOrUpdateClassInfo(obj.getClass());
    int xtypeId = classInfo.getXtypeId();
    buffer.writeVarUint32Small7((byte) xtypeId);
    switch ((byte) xtypeId) {
      case Types.ENUM:
      case Types.STRUCT:
      case Types.EXT:
        buffer.writeVarUint32Small7(xtypeId >>> 8);
        break;
      case Types.NS_ENUM:
      case Types.NS_STRUCT:
      case Types.NS_EXT:
        // if it's null, it's a bug.
        assert classInfo.packageNameBytes != null;
        metaStringResolver.writeMetaStringBytes(buffer, classInfo.packageNameBytes);
        assert classInfo.classNameBytes != null;
        metaStringResolver.writeMetaStringBytes(buffer, classInfo.classNameBytes);
    }
    return classInfo;
  }

  public ClassInfo readClassInfo(MemoryBuffer buffer) {
    int xtypeId = buffer.readVarUint32Small14();
    switch (xtypeId) {
      case Types.ENUM:
        return xtypeIdToClassMap.get(xtypeId << 8 + Types.ENUM);
      case Types.STRUCT:
        return xtypeIdToClassMap.get(xtypeId << 8 + Types.STRUCT);
      case Types.EXT:
        return xtypeIdToClassMap.get(xtypeId << 8 + Types.EXT);
      case Types.NS_ENUM:
      case Types.NS_STRUCT:
      case Types.NS_EXT:
        MetaStringBytes packageBytes = metaStringResolver.readMetaStringBytes(buffer);
        MetaStringBytes simpleClassNameBytes = metaStringResolver.readMetaStringBytes(buffer);
        return classResolver.loadBytesToClassInfo(packageBytes, simpleClassNameBytes);
      default:
        return xtypeIdToClassMap.get(xtypeId);
    }
  }
}
