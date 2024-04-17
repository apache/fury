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

package org.apache.fury.serializer;

import static org.apache.fury.type.TypeUtils.getRawType;

import com.google.common.reflect.TypeToken;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.fury.Fury;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.collection.Tuple3;
import org.apache.fury.exception.FuryException;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.DescriptorGrouper;
import org.apache.fury.type.FinalObjectTypeStub;
import org.apache.fury.type.GenericType;
import org.apache.fury.type.Generics;
import org.apache.fury.util.FieldAccessor;
import org.apache.fury.util.Platform;
import org.apache.fury.util.ReflectionUtils;
import org.apache.fury.util.record.RecordInfo;
import org.apache.fury.util.record.RecordUtils;

/**
 * A schema-consistent serializer used only for java serialization.
 *
 * <ul>
 *   <li>non-public class
 *   <li>non-static class
 *   <li>lambda
 *   <li>inner class
 *   <li>local class
 *   <li>anonymous class
 *   <li>class that can't be handled by other serializers or codegen-based serializers
 * </ul>
 */
// TODO(chaokunyang) support generics optimization for {@code SomeClass<T>}
@SuppressWarnings({"unchecked"})
public final class ObjectSerializer<T> extends Serializer<T> {
  private final RefResolver refResolver;
  private final ClassResolver classResolver;
  private final boolean isRecord;
  private final RecordInfo recordInfo;
  private final FinalTypeField[] finalFields;

  /**
   * Whether write class def for non-inner final types.
   *
   * @see ClassResolver#isMonomorphic(Class)
   */
  private final boolean[] isFinal;

  private final GenericTypeField[] otherFields;
  private final GenericTypeField[] containerFields;
  private final MethodHandle constructor;
  private final int classVersionHash;

  public ObjectSerializer(Fury fury, Class<T> cls) {
    this(fury, cls, true);
  }

  public ObjectSerializer(Fury fury, Class<T> cls, boolean resolveParent) {
    super(fury, cls);
    this.refResolver = fury.getRefResolver();
    this.classResolver = fury.getClassResolver();
    // avoid recursive building serializers.
    // Use `setSerializerIfAbsent` to avoid overwriting existing serializer for class when used
    // as data serializer.
    classResolver.setSerializerIfAbsent(cls, this);
    Collection<Descriptor> descriptors =
        fury.getClassResolver().getAllDescriptorsMap(cls, resolveParent).values();
    DescriptorGrouper descriptorGrouper =
        DescriptorGrouper.createDescriptorGrouper(
            descriptors, false, fury.compressInt(), fury.compressLong());
    isRecord = RecordUtils.isRecord(cls);
    if (isRecord) {
      constructor = RecordUtils.getRecordConstructor(cls).f1;
      List<String> fieldNames =
          descriptorGrouper.getSortedDescriptors().stream()
              .map(Descriptor::getName)
              .collect(Collectors.toList());
      recordInfo = new RecordInfo(cls, fieldNames);
    } else {
      this.constructor = ReflectionUtils.getCtrHandle(cls, false);
      recordInfo = null;
    }
    if (fury.checkClassVersion()) {
      classVersionHash = computeVersionHash(descriptors);
    } else {
      classVersionHash = 0;
    }
    Tuple3<Tuple2<FinalTypeField[], boolean[]>, GenericTypeField[], GenericTypeField[]> infos =
        buildFieldInfos(fury, descriptorGrouper);
    finalFields = infos.f0.f0;
    isFinal = infos.f0.f1;
    otherFields = infos.f1;
    containerFields = infos.f2;
  }

  static Tuple3<Tuple2<FinalTypeField[], boolean[]>, GenericTypeField[], GenericTypeField[]>
      buildFieldInfos(Fury fury, DescriptorGrouper grouper) {
    // When a type is both Collection/Map and final, add it to collection/map fields to keep
    // consistent with jit.
    Collection<Descriptor> primitives = grouper.getPrimitiveDescriptors();
    Collection<Descriptor> boxed = grouper.getBoxedDescriptors();
    Collection<Descriptor> finals = grouper.getFinalDescriptors();
    FinalTypeField[] finalFields =
        new FinalTypeField[primitives.size() + boxed.size() + finals.size()];
    int cnt = 0;
    for (Descriptor d : primitives) {
      finalFields[cnt++] = buildFinalTypeField(fury, d);
    }
    for (Descriptor d : boxed) {
      finalFields[cnt++] = buildFinalTypeField(fury, d);
    }
    // TODO(chaokunyang) Support Pojo<T> generics besides Map/Collection subclass
    //  when it's supported in BaseObjectCodecBuilder.
    for (Descriptor d : finals) {
      finalFields[cnt++] = buildFinalTypeField(fury, d);
    }
    boolean[] isFinal = new boolean[finalFields.length];
    for (int i = 0; i < isFinal.length; i++) {
      ClassInfo classInfo = finalFields[i].classInfo;
      isFinal[i] = classInfo != null && fury.getClassResolver().isMonomorphic(classInfo.getCls());
    }
    cnt = 0;
    GenericTypeField[] otherFields = new GenericTypeField[grouper.getOtherDescriptors().size()];
    for (Descriptor descriptor : grouper.getOtherDescriptors()) {
      GenericTypeField genericTypeField =
          new GenericTypeField(
              descriptor.getRawType(),
              descriptor.getDeclaringClass() + "." + descriptor.getName(),
              descriptor.getField() != null
                  ? FieldAccessor.createAccessor(descriptor.getField())
                  : null,
              fury);
      otherFields[cnt++] = genericTypeField;
    }
    cnt = 0;
    Collection<Descriptor> collections = grouper.getCollectionDescriptors();
    Collection<Descriptor> maps = grouper.getMapDescriptors();
    GenericTypeField[] containerFields = new GenericTypeField[collections.size() + maps.size()];
    for (Descriptor d : collections) {
      containerFields[cnt++] = buildContainerField(fury, d);
    }
    for (Descriptor d : maps) {
      containerFields[cnt++] = buildContainerField(fury, d);
    }
    return Tuple3.of(Tuple2.of(finalFields, isFinal), otherFields, containerFields);
  }

  private static FinalTypeField buildFinalTypeField(Fury fury, Descriptor d) {
    return new FinalTypeField(
        d.getRawType(),
        d.getDeclaringClass() + "." + d.getName(),
        // `d.getField()` will be null when peer class doesn't have this field.
        d.getField() != null ? FieldAccessor.createAccessor(d.getField()) : null,
        fury);
  }

  private static GenericTypeField buildContainerField(Fury fury, Descriptor d) {
    return new GenericTypeField(
        d.getTypeToken(),
        d.getDeclaringClass() + "." + d.getName(),
        d.getField() != null ? FieldAccessor.createAccessor(d.getField()) : null,
        fury);
  }

  @Override
  public void write(MemoryBuffer buffer, T value) {
    Fury fury = this.fury;
    RefResolver refResolver = this.refResolver;
    ClassResolver classResolver = this.classResolver;
    if (fury.checkClassVersion()) {
      buffer.writeInt32(classVersionHash);
    }
    // write order: primitive,boxed,final,other,collection,map
    writeFinalFields(buffer, value, fury, refResolver, classResolver);
    for (GenericTypeField fieldInfo : otherFields) {
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      Object fieldValue = fieldAccessor.getObject(value);
      if (fieldInfo.trackingRef) {
        fury.writeRef(buffer, fieldValue, fieldInfo.classInfoHolder);
      } else {
        fury.writeNullable(buffer, fieldValue, fieldInfo.classInfoHolder);
      }
    }
    writeContainerFields(buffer, value, fury, refResolver, classResolver);
  }

  private void writeFinalFields(
      MemoryBuffer buffer,
      T value,
      Fury fury,
      RefResolver refResolver,
      ClassResolver classResolver) {
    FinalTypeField[] finalFields = this.finalFields;
    boolean metaContextShareEnabled = fury.getConfig().shareMetaContext();
    for (int i = 0; i < finalFields.length; i++) {
      FinalTypeField fieldInfo = finalFields[i];
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      short classId = fieldInfo.classId;
      if (writePrimitiveFieldValueFailed(fury, buffer, value, fieldAccessor, classId)) {
        Object fieldValue = fieldAccessor.getObject(value);
        if (writeBasicObjectFieldValueFailed(fury, buffer, fieldValue, classId)) {
          Serializer<Object> serializer = fieldInfo.classInfo.getSerializer();
          if (!metaContextShareEnabled || isFinal[i]) {
            // whether tracking ref is recorded in `fieldInfo.serializer`, so it's still
            // consistent with jit serializer.
            fury.writeRef(buffer, fieldValue, serializer);
          } else {
            if (serializer.needToWriteRef()) {
              if (!refResolver.writeRefOrNull(buffer, fieldValue)) {
                classResolver.writeClass(buffer, fieldInfo.classInfo);
                // No generics for field, no need to update `depth`.
                serializer.write(buffer, fieldValue);
              }
            } else {
              fury.writeNullable(buffer, fieldValue, fieldInfo.classInfo);
            }
          }
        }
      }
    }
  }

  private void writeContainerFields(
      MemoryBuffer buffer,
      T value,
      Fury fury,
      RefResolver refResolver,
      ClassResolver classResolver) {
    Generics generics = fury.getGenerics();
    for (GenericTypeField fieldInfo : containerFields) {
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      Object fieldValue = fieldAccessor.getObject(value);
      writeContainerFieldValue(
          fury, refResolver, classResolver, generics, fieldInfo, buffer, fieldValue);
    }
  }

  static void writeContainerFieldValue(
      Fury fury,
      RefResolver refResolver,
      ClassResolver classResolver,
      Generics generics,
      GenericTypeField fieldInfo,
      MemoryBuffer buffer,
      Object fieldValue) {
    if (fieldInfo.trackingRef) {
      if (!refResolver.writeRefOrNull(buffer, fieldValue)) {
        ClassInfo classInfo =
            classResolver.getClassInfo(fieldValue.getClass(), fieldInfo.classInfoHolder);
        generics.pushGenericType(fieldInfo.genericType);
        fury.writeNonRef(buffer, fieldValue, classInfo);
        generics.popGenericType();
      }
    } else {
      if (fieldValue == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        generics.pushGenericType(fieldInfo.genericType);
        fury.writeNonRef(
            buffer,
            fieldValue,
            classResolver.getClassInfo(fieldValue.getClass(), fieldInfo.classInfoHolder));
        generics.popGenericType();
      }
    }
  }

  @Override
  public T read(MemoryBuffer buffer) {
    if (isRecord) {
      Object[] fields = readFields(buffer);
      RecordUtils.remapping(recordInfo, fields);
      try {
        T obj = (T) constructor.invokeWithArguments(recordInfo.getRecordComponents());
        Arrays.fill(recordInfo.getRecordComponents(), null);
        return obj;
      } catch (Throwable e) {
        Platform.throwException(e);
      }
    }
    T obj = newBean(constructor, type);
    refResolver.reference(obj);
    return readAndSetFields(buffer, obj);
  }

  public Object[] readFields(MemoryBuffer buffer) {
    Fury fury = this.fury;
    RefResolver refResolver = this.refResolver;
    ClassResolver classResolver = this.classResolver;
    if (fury.checkClassVersion()) {
      int hash = buffer.readInt32();
      checkClassVersion(fury, hash, classVersionHash);
    }
    Object[] fieldValues =
        new Object[finalFields.length + otherFields.length + containerFields.length];
    int counter = 0;
    // read order: primitive,boxed,final,other,collection,map
    FinalTypeField[] finalFields = this.finalFields;
    boolean metaContextShareEnabled = fury.getConfig().shareMetaContext();
    for (int i = 0; i < finalFields.length; i++) {
      FinalTypeField fieldInfo = finalFields[i];
      boolean isFinal = !metaContextShareEnabled || this.isFinal[i];
      short classId = fieldInfo.classId;
      if (classId >= ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID
          && classId <= ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID) {
        fieldValues[counter++] = Serializers.readPrimitiveValue(fury, buffer, classId);
      } else {
        Object fieldValue =
            readFinalObjectFieldValue(fury, refResolver, classResolver, fieldInfo, isFinal, buffer);
        fieldValues[counter++] = fieldValue;
      }
    }
    for (GenericTypeField fieldInfo : otherFields) {
      Object fieldValue = readOtherFieldValue(fury, fieldInfo, buffer);
      fieldValues[counter++] = fieldValue;
    }
    Generics generics = fury.getGenerics();
    for (GenericTypeField fieldInfo : containerFields) {
      Object fieldValue = readContainerFieldValue(fury, generics, fieldInfo, buffer);
      fieldValues[counter++] = fieldValue;
    }
    return fieldValues;
  }

  public T readAndSetFields(MemoryBuffer buffer, T obj) {
    Fury fury = this.fury;
    RefResolver refResolver = this.refResolver;
    ClassResolver classResolver = this.classResolver;
    if (fury.checkClassVersion()) {
      int hash = buffer.readInt32();
      checkClassVersion(fury, hash, classVersionHash);
    }
    // read order: primitive,boxed,final,other,collection,map
    FinalTypeField[] finalFields = this.finalFields;
    boolean metaContextShareEnabled = fury.getConfig().shareMetaContext();
    for (int i = 0; i < finalFields.length; i++) {
      FinalTypeField fieldInfo = finalFields[i];
      boolean isFinal = !metaContextShareEnabled || this.isFinal[i];
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      short classId = fieldInfo.classId;
      if (readPrimitiveFieldValueFailed(fury, buffer, obj, fieldAccessor, classId)
          && readBasicObjectFieldValueFailed(fury, buffer, obj, fieldAccessor, classId)) {
        Object fieldValue =
            readFinalObjectFieldValue(fury, refResolver, classResolver, fieldInfo, isFinal, buffer);
        fieldAccessor.putObject(obj, fieldValue);
      }
    }
    for (GenericTypeField fieldInfo : otherFields) {
      Object fieldValue = readOtherFieldValue(fury, fieldInfo, buffer);
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      fieldAccessor.putObject(obj, fieldValue);
    }
    Generics generics = fury.getGenerics();
    for (GenericTypeField fieldInfo : containerFields) {
      Object fieldValue = readContainerFieldValue(fury, generics, fieldInfo, buffer);
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      fieldAccessor.putObject(obj, fieldValue);
    }
    return obj;
  }

  /**
   * Read final object field value. Note that primitive field value can't be read by this method,
   * because primitive field doesn't write null flag.
   */
  static Object readFinalObjectFieldValue(
      Fury fury,
      RefResolver refResolver,
      ClassResolver classResolver,
      FinalTypeField fieldInfo,
      boolean isFinal,
      MemoryBuffer buffer) {
    Serializer<Object> serializer = fieldInfo.classInfo.getSerializer();
    Object fieldValue;
    if (isFinal) {
      // whether tracking ref is recorded in `fieldInfo.serializer`, so it's still
      // consistent with jit serializer.
      fieldValue = fury.readRef(buffer, serializer);
    } else {
      if (serializer.needToWriteRef()) {
        int nextReadRefId = refResolver.tryPreserveRefId(buffer);
        if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
          classResolver.readClassInfo(buffer, fieldInfo.classInfo);
          fieldValue = serializer.read(buffer);
          refResolver.setReadObject(nextReadRefId, fieldValue);
        } else {
          fieldValue = refResolver.getReadObject();
        }
      } else {
        byte headFlag = buffer.readByte();
        if (headFlag == Fury.NULL_FLAG) {
          fieldValue = null;
        } else {
          classResolver.readClassInfo(buffer, fieldInfo.classInfo);
          fieldValue = serializer.read(buffer);
        }
      }
    }
    return fieldValue;
  }

  static Object readOtherFieldValue(Fury fury, GenericTypeField fieldInfo, MemoryBuffer buffer) {
    Object fieldValue;
    if (fieldInfo.trackingRef) {
      fieldValue = fury.readRef(buffer, fieldInfo.classInfoHolder);
    } else {
      byte headFlag = buffer.readByte();
      if (headFlag == Fury.NULL_FLAG) {
        fieldValue = null;
      } else {
        fieldValue = fury.readNonRef(buffer, fieldInfo.classInfoHolder);
      }
    }
    return fieldValue;
  }

  static Object readContainerFieldValue(
      Fury fury, Generics generics, GenericTypeField fieldInfo, MemoryBuffer buffer) {
    Object fieldValue;
    if (fieldInfo.trackingRef) {
      generics.pushGenericType(fieldInfo.genericType);
      fieldValue = fury.readRef(buffer, fieldInfo.classInfoHolder);
      generics.popGenericType();
    } else {
      byte headFlag = buffer.readByte();
      if (headFlag == Fury.NULL_FLAG) {
        fieldValue = null;
      } else {
        generics.pushGenericType(fieldInfo.genericType);
        fieldValue = fury.readNonRef(buffer, fieldInfo.classInfoHolder);
        generics.popGenericType();
      }
    }
    return fieldValue;
  }

  static boolean writePrimitiveFieldValueFailed(
      Fury fury,
      MemoryBuffer buffer,
      Object targetObject,
      FieldAccessor fieldAccessor,
      short classId) {
    long fieldOffset = fieldAccessor.getFieldOffset();
    if (fieldOffset != -1) {
      return writePrimitiveFieldValueFailed(fury, buffer, targetObject, fieldOffset, classId);
    }
    switch (classId) {
      case ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID:
        buffer.writeBoolean((Boolean) fieldAccessor.get(targetObject));
        return false;
      case ClassResolver.PRIMITIVE_BYTE_CLASS_ID:
        buffer.writeByte((Byte) fieldAccessor.get(targetObject));
        return false;
      case ClassResolver.PRIMITIVE_CHAR_CLASS_ID:
        buffer.writeChar((Character) fieldAccessor.get(targetObject));
        return false;
      case ClassResolver.PRIMITIVE_SHORT_CLASS_ID:
        buffer.writeInt16((Short) fieldAccessor.get(targetObject));
        return false;
      case ClassResolver.PRIMITIVE_INT_CLASS_ID:
        {
          int fieldValue = (Integer) fieldAccessor.get(targetObject);
          if (fury.compressInt()) {
            buffer.writeVarInt32(fieldValue);
          } else {
            buffer.writeInt32(fieldValue);
          }
          return false;
        }
      case ClassResolver.PRIMITIVE_FLOAT_CLASS_ID:
        buffer.writeFloat32((Float) fieldAccessor.get(targetObject));
        return false;
      case ClassResolver.PRIMITIVE_LONG_CLASS_ID:
        {
          long fieldValue = (long) fieldAccessor.get(targetObject);
          fury.writeInt64(buffer, fieldValue);
          return false;
        }
      case ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID:
        buffer.writeFloat64((Double) fieldAccessor.get(targetObject));
        return false;
      default:
        return true;
    }
  }

  static boolean writePrimitiveFieldValueFailed(
      Fury fury, MemoryBuffer buffer, Object targetObject, long fieldOffset, short classId) {
    switch (classId) {
      case ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID:
        buffer.writeBoolean(Platform.getBoolean(targetObject, fieldOffset));
        return false;
      case ClassResolver.PRIMITIVE_BYTE_CLASS_ID:
        buffer.writeByte(Platform.getByte(targetObject, fieldOffset));
        return false;
      case ClassResolver.PRIMITIVE_CHAR_CLASS_ID:
        buffer.writeChar(Platform.getChar(targetObject, fieldOffset));
        return false;
      case ClassResolver.PRIMITIVE_SHORT_CLASS_ID:
        buffer.writeInt16(Platform.getShort(targetObject, fieldOffset));
        return false;
      case ClassResolver.PRIMITIVE_INT_CLASS_ID:
        {
          int fieldValue = Platform.getInt(targetObject, fieldOffset);
          if (fury.compressInt()) {
            buffer.writeVarInt32(fieldValue);
          } else {
            buffer.writeInt32(fieldValue);
          }
          return false;
        }
      case ClassResolver.PRIMITIVE_FLOAT_CLASS_ID:
        buffer.writeFloat32(Platform.getFloat(targetObject, fieldOffset));
        return false;
      case ClassResolver.PRIMITIVE_LONG_CLASS_ID:
        {
          long fieldValue = Platform.getLong(targetObject, fieldOffset);
          fury.writeInt64(buffer, fieldValue);
          return false;
        }
      case ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID:
        buffer.writeFloat64(Platform.getDouble(targetObject, fieldOffset));
        return false;
      default:
        return true;
    }
  }

  /**
   * Write field value to buffer.
   *
   * @return true if field value isn't written by this function.
   */
  static boolean writeBasicObjectFieldValueFailed(
      Fury fury, MemoryBuffer buffer, Object fieldValue, short classId) {
    if (!fury.isBasicTypesRefIgnored()) {
      return true; // let common path handle this.
    }
    // add time types serialization here.
    switch (classId) {
      case ClassResolver.STRING_CLASS_ID: // fastpath for string.
        fury.writeJavaStringRef(buffer, (String) (fieldValue));
        return false;
      case ClassResolver.BOOLEAN_CLASS_ID:
        {
          if (fieldValue == null) {
            buffer.writeByte(Fury.NULL_FLAG);
          } else {
            buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
            buffer.writeBoolean((Boolean) (fieldValue));
          }
          return false;
        }
      case ClassResolver.BYTE_CLASS_ID:
        {
          if (fieldValue == null) {
            buffer.writeByte(Fury.NULL_FLAG);
          } else {
            buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
            buffer.writeByte((Byte) (fieldValue));
          }
          return false;
        }
      case ClassResolver.CHAR_CLASS_ID:
        {
          if (fieldValue == null) {
            buffer.writeByte(Fury.NULL_FLAG);
          } else {
            buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
            buffer.writeChar((Character) (fieldValue));
          }
          return false;
        }
      case ClassResolver.SHORT_CLASS_ID:
        {
          if (fieldValue == null) {
            buffer.writeByte(Fury.NULL_FLAG);
          } else {
            buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
            buffer.writeInt16((Short) (fieldValue));
          }
          return false;
        }
      case ClassResolver.INTEGER_CLASS_ID:
        {
          if (fieldValue == null) {
            buffer.writeByte(Fury.NULL_FLAG);
          } else {
            buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
            if (fury.compressInt()) {
              buffer.writeVarInt32((Integer) (fieldValue));
            } else {
              buffer.writeInt32((Integer) (fieldValue));
            }
          }
          return false;
        }
      case ClassResolver.FLOAT_CLASS_ID:
        {
          if (fieldValue == null) {
            buffer.writeByte(Fury.NULL_FLAG);
          } else {
            buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
            buffer.writeFloat32((Float) (fieldValue));
          }
          return false;
        }
      case ClassResolver.LONG_CLASS_ID:
        {
          if (fieldValue == null) {
            buffer.writeByte(Fury.NULL_FLAG);
          } else {
            buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
            fury.writeInt64(buffer, (Long) fieldValue);
          }
          return false;
        }
      case ClassResolver.DOUBLE_CLASS_ID:
        {
          if (fieldValue == null) {
            buffer.writeByte(Fury.NULL_FLAG);
          } else {
            buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
            buffer.writeFloat64((Double) (fieldValue));
          }
          return false;
        }
      default:
        return true;
    }
  }

  /**
   * Read a primitive value from buffer and set it to field referenced by <code>fieldAccessor</code>
   * of <code>targetObject</code>.
   *
   * @return true if <code>classId</code> is not a primitive type id.
   */
  static boolean readPrimitiveFieldValueFailed(
      Fury fury,
      MemoryBuffer buffer,
      Object targetObject,
      FieldAccessor fieldAccessor,
      short classId) {
    long fieldOffset = fieldAccessor.getFieldOffset();
    if (fieldOffset != -1) {
      return readPrimitiveFieldValueFailed(fury, buffer, targetObject, fieldOffset, classId);
    }
    switch (classId) {
      case ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID:
        fieldAccessor.set(targetObject, buffer.readBoolean());
        return false;
      case ClassResolver.PRIMITIVE_BYTE_CLASS_ID:
        fieldAccessor.set(targetObject, buffer.readByte());
        return false;
      case ClassResolver.PRIMITIVE_CHAR_CLASS_ID:
        fieldAccessor.set(targetObject, buffer.readChar());
        return false;
      case ClassResolver.PRIMITIVE_SHORT_CLASS_ID:
        fieldAccessor.set(targetObject, buffer.readInt16());
        return false;
      case ClassResolver.PRIMITIVE_INT_CLASS_ID:
        if (fury.compressInt()) {
          fieldAccessor.set(targetObject, buffer.readVarInt32());
        } else {
          fieldAccessor.set(targetObject, buffer.readInt32());
        }
        return false;
      case ClassResolver.PRIMITIVE_FLOAT_CLASS_ID:
        fieldAccessor.set(targetObject, buffer.readFloat32());
        return false;
      case ClassResolver.PRIMITIVE_LONG_CLASS_ID:
        fieldAccessor.set(targetObject, fury.readInt64(buffer));
        return false;
      case ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID:
        fieldAccessor.set(targetObject, buffer.readFloat64());
        return false;
      case ClassResolver.STRING_CLASS_ID:
        fieldAccessor.putObject(targetObject, fury.readJavaStringRef(buffer));
        return false;
      default:
        {
          return true;
        }
    }
  }

  private static boolean readPrimitiveFieldValueFailed(
      Fury fury, MemoryBuffer buffer, Object targetObject, long fieldOffset, short classId) {
    switch (classId) {
      case ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID:
        Platform.putBoolean(targetObject, fieldOffset, buffer.readBoolean());
        return false;
      case ClassResolver.PRIMITIVE_BYTE_CLASS_ID:
        Platform.putByte(targetObject, fieldOffset, buffer.readByte());
        return false;
      case ClassResolver.PRIMITIVE_CHAR_CLASS_ID:
        Platform.putChar(targetObject, fieldOffset, buffer.readChar());
        return false;
      case ClassResolver.PRIMITIVE_SHORT_CLASS_ID:
        Platform.putShort(targetObject, fieldOffset, buffer.readInt16());
        return false;
      case ClassResolver.PRIMITIVE_INT_CLASS_ID:
        if (fury.compressInt()) {
          Platform.putInt(targetObject, fieldOffset, buffer.readVarInt32());
        } else {
          Platform.putInt(targetObject, fieldOffset, buffer.readInt32());
        }
        return false;
      case ClassResolver.PRIMITIVE_FLOAT_CLASS_ID:
        Platform.putFloat(targetObject, fieldOffset, buffer.readFloat32());
        return false;
      case ClassResolver.PRIMITIVE_LONG_CLASS_ID:
        Platform.putLong(targetObject, fieldOffset, fury.readInt64(buffer));
        return false;
      case ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID:
        Platform.putDouble(targetObject, fieldOffset, buffer.readFloat64());
        return false;
      case ClassResolver.STRING_CLASS_ID:
        Platform.putObject(targetObject, fieldOffset, fury.readJavaStringRef(buffer));
        return false;
      default:
        {
          return true;
        }
    }
  }

  static boolean readBasicObjectFieldValueFailed(
      Fury fury,
      MemoryBuffer buffer,
      Object targetObject,
      FieldAccessor fieldAccessor,
      short classId) {
    if (!fury.isBasicTypesRefIgnored()) {
      return true; // let common path handle this.
    }
    // add time types serialization here.
    switch (classId) {
      case ClassResolver.STRING_CLASS_ID: // fastpath for string.
        fieldAccessor.putObject(targetObject, fury.readJavaStringRef(buffer));
        return false;
      case ClassResolver.BOOLEAN_CLASS_ID:
        {
          if (buffer.readByte() == Fury.NULL_FLAG) {
            fieldAccessor.putObject(targetObject, null);
          } else {
            fieldAccessor.putObject(targetObject, buffer.readBoolean());
          }
          return false;
        }
      case ClassResolver.BYTE_CLASS_ID:
        {
          if (buffer.readByte() == Fury.NULL_FLAG) {
            fieldAccessor.putObject(targetObject, null);
          } else {
            fieldAccessor.putObject(targetObject, buffer.readByte());
          }
          return false;
        }
      case ClassResolver.CHAR_CLASS_ID:
        {
          if (buffer.readByte() == Fury.NULL_FLAG) {
            fieldAccessor.putObject(targetObject, null);
          } else {
            fieldAccessor.putObject(targetObject, buffer.readChar());
          }
          return false;
        }
      case ClassResolver.SHORT_CLASS_ID:
        {
          if (buffer.readByte() == Fury.NULL_FLAG) {
            fieldAccessor.putObject(targetObject, null);
          } else {
            fieldAccessor.putObject(targetObject, buffer.readInt16());
          }
          return false;
        }
      case ClassResolver.INTEGER_CLASS_ID:
        {
          if (buffer.readByte() == Fury.NULL_FLAG) {
            fieldAccessor.putObject(targetObject, null);
          } else {
            if (fury.compressInt()) {
              fieldAccessor.putObject(targetObject, buffer.readVarInt32());
            } else {
              fieldAccessor.putObject(targetObject, buffer.readInt32());
            }
          }
          return false;
        }
      case ClassResolver.FLOAT_CLASS_ID:
        {
          if (buffer.readByte() == Fury.NULL_FLAG) {
            fieldAccessor.putObject(targetObject, null);
          } else {
            fieldAccessor.putObject(targetObject, buffer.readFloat32());
          }
          return false;
        }
      case ClassResolver.LONG_CLASS_ID:
        {
          if (buffer.readByte() == Fury.NULL_FLAG) {
            fieldAccessor.putObject(targetObject, null);
          } else {
            fieldAccessor.putObject(targetObject, fury.readInt64(buffer));
          }
          return false;
        }
      case ClassResolver.DOUBLE_CLASS_ID:
        {
          if (buffer.readByte() == Fury.NULL_FLAG) {
            fieldAccessor.putObject(targetObject, null);
          } else {
            fieldAccessor.putObject(targetObject, buffer.readFloat64());
          }
          return false;
        }
      default:
        return true;
    }
  }

  static <T> T newBean(MethodHandle constructor, Class<T> type) {
    if (constructor != null) {
      try {
        return (T) constructor.invoke();
      } catch (Throwable e) {
        Platform.throwException(e);
      }
    }
    return Platform.newInstance(type);
  }

  static class InternalFieldInfo {
    protected final short classId;
    protected final String qualifiedFieldName;
    protected final FieldAccessor fieldAccessor;

    private InternalFieldInfo(
        short classId, String qualifiedFieldName, FieldAccessor fieldAccessor) {
      this.classId = classId;
      this.qualifiedFieldName = qualifiedFieldName;
      this.fieldAccessor = fieldAccessor;
    }

    @Override
    public String toString() {
      return "InternalFieldInfo{"
          + "classId="
          + classId
          + ", fieldName="
          + qualifiedFieldName
          + ", field="
          + (fieldAccessor != null ? fieldAccessor.getField() : null)
          + '}';
    }
  }

  static final class FinalTypeField extends InternalFieldInfo {
    final ClassInfo classInfo;

    private FinalTypeField(Class<?> type, String fieldName, FieldAccessor accessor, Fury fury) {
      super(getRegisteredClassId(fury, type), fieldName, accessor);
      // invoke `copy` to avoid ObjectSerializer construct clear serializer by `clearSerializer`.
      if (type == FinalObjectTypeStub.class) {
        // `FinalObjectTypeStub` has no fields, using its `classInfo`
        // will make deserialization failed.
        classInfo = null;
      } else {
        classInfo = fury.getClassResolver().getClassInfo(type);
      }
    }
  }

  static final class GenericTypeField extends InternalFieldInfo {
    private final GenericType genericType;
    final ClassInfoHolder classInfoHolder;
    final boolean trackingRef;

    private GenericTypeField(
        Class<?> cls, String qualifiedFieldName, FieldAccessor accessor, Fury fury) {
      super(getRegisteredClassId(fury, cls), qualifiedFieldName, accessor);
      // TODO support generics <T> in Pojo<T>, see ComplexObjectSerializer.getGenericTypes
      genericType = fury.getClassResolver().buildGenericType(cls);
      classInfoHolder = fury.getClassResolver().nilClassInfoHolder();
      trackingRef = fury.getClassResolver().needToWriteRef(cls);
    }

    private GenericTypeField(
        TypeToken<?> typeToken, String qualifiedFieldName, FieldAccessor accessor, Fury fury) {
      super(getRegisteredClassId(fury, getRawType(typeToken)), qualifiedFieldName, accessor);
      // TODO support generics <T> in Pojo<T>, see ComplexObjectSerializer.getGenericTypes
      genericType = fury.getClassResolver().buildGenericType(typeToken);
      classInfoHolder = fury.getClassResolver().nilClassInfoHolder();
      trackingRef = fury.getClassResolver().needToWriteRef(getRawType(typeToken));
    }

    @Override
    public String toString() {
      return "GenericTypeField{"
          + "genericType="
          + genericType
          + ", classId="
          + classId
          + ", qualifiedFieldName="
          + qualifiedFieldName
          + ", field="
          + (fieldAccessor != null ? fieldAccessor.getField() : null)
          + '}';
    }
  }

  private static short getRegisteredClassId(Fury fury, Class<?> cls) {
    Short classId = fury.getClassResolver().getRegisteredClassId(cls);
    return classId == null ? ClassResolver.NO_CLASS_ID : classId;
  }

  public static int computeVersionHash(Collection<Descriptor> descriptors) {
    // TODO(chaokunyang) use murmurhash
    List<Integer> list = new ArrayList<>();
    for (Descriptor d : descriptors) {
      Integer integer = Objects.hash(d.getName(), d.getRawType().getName(), d.getDeclaringClass());
      list.add(integer);
    }
    return list.hashCode();
  }

  public static void checkClassVersion(Fury fury, int readHash, int classVersionHash) {
    if (readHash != classVersionHash) {
      throw new FuryException(
          String.format(
              "Read class %s version %s is not consistent with %s",
              fury.getClassResolver().getCurrentReadClass(), readHash, classVersionHash));
    }
  }
}
