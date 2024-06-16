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
import org.apache.fury.serializer.StructSerializer;
import org.apache.fury.type.Types;
import org.apache.fury.util.Preconditions;

public class XtypeResolver {
  private static final float loadFactor = 0.25f;

  private final ClassResolver classResolver;
  private int xtypeIdGenerator = 64;

  private final LongMap<ClassInfo> xtypeIdToClassMap = new LongMap<>(8, loadFactor);
  private final Set<Integer> registeredTypeIds = new HashSet<>();

  public XtypeResolver(ClassResolver classResolver) {
    this.classResolver = classResolver;
    registerDefaultTypes();
  }

  public ClassInfo readClassInfo(MemoryBuffer buffer) {
    int typeId = buffer.readVarUint32Small7();
    return xtypeIdToClassMap.get(typeId);
  }

  public void register(Class<?> type) {
    while (registeredTypeIds.contains(xtypeIdGenerator)) {
      xtypeIdGenerator++;
    }
    register(type, xtypeIdGenerator++);
  }

  public void register(Class<?> type, int xtypeId) {
    // We can relax this limit in the future, but currently we keep it in small range for future
    // extension.
    Preconditions.checkArgument(xtypeId < Short.MAX_VALUE, "Too big type id %s", xtypeId);
    ClassInfo classInfo = xtypeIdToClassMap.get(xtypeId);
    if (classInfo != null) {
      throw new IllegalArgumentException(
          String.format("Type %s has been registered with id %s", type, classInfo.xtypeId));
    }
    if (type.isEnum()) {
      classResolver.registerSerializer(
          type, new EnumSerializer<>(classResolver.getFury(), type.asSubclass(Enum.class)));
    } else {
      classResolver.registerSerializer(type, new StructSerializer<>(classResolver.getFury(), type));
    }
    registeredTypeIds.add(xtypeId);
    classInfo = classResolver.getClassInfo(type);
    xtypeIdToClassMap.put(xtypeId, classInfo);
    internalRegister(type, xtypeId);
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
    classInfo.xtypeId = (short) xtypeId;
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
}
