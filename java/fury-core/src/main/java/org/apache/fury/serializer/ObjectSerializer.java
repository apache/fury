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

import static org.apache.fury.type.DescriptorGrouper.createDescriptorGrouper;

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
import org.apache.fury.memory.Platform;
import org.apache.fury.meta.ClassDef;
import org.apache.fury.reflect.FieldAccessor;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.resolver.TypeResolver;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.DescriptorGrouper;
import org.apache.fury.type.Generics;
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
public final class ObjectSerializer<T> extends AbstractObjectSerializer<T> {
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
  private final int classVersionHash;
  private final SerializationBinding binding;
  private final TypeResolver typeResolver;

  public ObjectSerializer(Fury fury, Class<T> cls) {
    this(fury, cls, true);
  }

  public ObjectSerializer(Fury fury, Class<T> cls, boolean resolveParent) {
    super(fury, cls);
    binding = SerializationBinding.createBinding(fury);
    // avoid recursive building serializers.
    // Use `setSerializerIfAbsent` to avoid overwriting existing serializer for class when used
    // as data serializer.
    if (resolveParent) {
      classResolver.setSerializerIfAbsent(cls, this);
    }
    typeResolver = fury.isCrossLanguage() ? fury.getXtypeResolver() : classResolver;
    Collection<Descriptor> descriptors;
    boolean shareMeta = fury.getConfig().isMetaShareEnabled();
    if (shareMeta) {
      ClassDef classDef = classResolver.getClassDef(cls, resolveParent);
      descriptors = classDef.getDescriptors(classResolver, cls);
    } else {
      descriptors = fury.getClassResolver().getAllDescriptorsMap(cls, resolveParent).values();
    }
    DescriptorGrouper descriptorGrouper =
        createDescriptorGrouper(
            fury.getClassResolver()::isMonomorphic,
            descriptors,
            false,
            fury.compressInt(),
            fury.compressLong());

    if (isRecord) {
      List<String> fieldNames =
          descriptorGrouper.getSortedDescriptors().stream()
              .map(Descriptor::getName)
              .collect(Collectors.toList());
      recordInfo = new RecordInfo(cls, fieldNames);
    } else {
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

  @Override
  public void write(MemoryBuffer buffer, T value) {
    Fury fury = this.fury;
    RefResolver refResolver = this.refResolver;
    if (fury.checkClassVersion()) {
      buffer.writeInt32(classVersionHash);
    }
    // write order: primitive,boxed,final,other,collection,map
    writeFinalFields(buffer, value, fury, refResolver, typeResolver);
    for (GenericTypeField fieldInfo : otherFields) {
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      Object fieldValue = fieldAccessor.getObject(value);
      if (fieldInfo.trackingRef) {
        binding.writeRef(buffer, fieldValue, fieldInfo.classInfoHolder);
      } else {
        binding.writeNullable(buffer, fieldValue, fieldInfo.classInfoHolder);
      }
    }
    writeContainerFields(buffer, value, fury, refResolver, typeResolver);
  }

  private void writeFinalFields(
      MemoryBuffer buffer, T value, Fury fury, RefResolver refResolver, TypeResolver typeResolver) {
    FinalTypeField[] finalFields = this.finalFields;
    boolean metaShareEnabled = fury.getConfig().isMetaShareEnabled();
    for (int i = 0; i < finalFields.length; i++) {
      FinalTypeField fieldInfo = finalFields[i];
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      short classId = fieldInfo.classId;
      if (writePrimitiveFieldValueFailed(fury, buffer, value, fieldAccessor, classId)) {
        Object fieldValue = fieldAccessor.getObject(value);
        if (writeBasicObjectFieldValueFailed(fury, buffer, fieldValue, classId)) {
          Serializer<Object> serializer = fieldInfo.classInfo.getSerializer();
          if (!metaShareEnabled || isFinal[i]) {
            if (!fieldInfo.trackingRef) {
              binding.writeNullable(buffer, fieldValue, serializer);
            } else {
              // whether tracking ref is recorded in `fieldInfo.serializer`, so it's still
              // consistent with jit serializer.
              binding.writeRef(buffer, fieldValue, serializer);
            }
          } else {
            if (fieldInfo.trackingRef && serializer.needToWriteRef()) {
              if (!refResolver.writeRefOrNull(buffer, fieldValue)) {
                typeResolver.writeClassInfo(buffer, fieldInfo.classInfo);
                // No generics for field, no need to update `depth`.
                binding.write(buffer, serializer, fieldValue);
              }
            } else {
              binding.writeNullable(buffer, fieldValue, fieldInfo.classInfo);
            }
          }
        }
      }
    }
  }

  private void writeContainerFields(
      MemoryBuffer buffer, T value, Fury fury, RefResolver refResolver, TypeResolver typeResolver) {
    Generics generics = fury.getGenerics();
    for (GenericTypeField fieldInfo : containerFields) {
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      Object fieldValue = fieldAccessor.getObject(value);
      writeContainerFieldValue(
          binding, refResolver, typeResolver, generics, fieldInfo, buffer, fieldValue);
    }
  }

  static void writeContainerFieldValue(
      SerializationBinding binding,
      RefResolver refResolver,
      TypeResolver typeResolver,
      Generics generics,
      GenericTypeField fieldInfo,
      MemoryBuffer buffer,
      Object fieldValue) {
    if (fieldInfo.trackingRef) {
      if (!refResolver.writeRefOrNull(buffer, fieldValue)) {
        ClassInfo classInfo =
            typeResolver.getClassInfo(fieldValue.getClass(), fieldInfo.classInfoHolder);
        generics.pushGenericType(fieldInfo.genericType);
        binding.writeNonRef(buffer, fieldValue, classInfo);
        generics.popGenericType();
      }
    } else {
      if (fieldValue == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        generics.pushGenericType(fieldInfo.genericType);
        binding.writeNonRef(
            buffer,
            fieldValue,
            typeResolver.getClassInfo(fieldValue.getClass(), fieldInfo.classInfoHolder));
        generics.popGenericType();
      }
    }
  }

  @Override
  public T read(MemoryBuffer buffer) {
    if (isRecord) {
      Object[] fields = readFields(buffer);
      fields = RecordUtils.remapping(recordInfo, fields);
      try {
        T obj = (T) constructor.invokeWithArguments(fields);
        Arrays.fill(recordInfo.getRecordComponents(), null);
        return obj;
      } catch (Throwable e) {
        Platform.throwException(e);
      }
    }
    T obj = newBean();
    refResolver.reference(obj);
    return readAndSetFields(buffer, obj);
  }

  public Object[] readFields(MemoryBuffer buffer) {
    Fury fury = this.fury;
    RefResolver refResolver = this.refResolver;
    TypeResolver typeResolver = this.typeResolver;
    if (fury.checkClassVersion()) {
      int hash = buffer.readInt32();
      checkClassVersion(fury, hash, classVersionHash);
    }
    Object[] fieldValues =
        new Object[finalFields.length + otherFields.length + containerFields.length];
    int counter = 0;
    // read order: primitive,boxed,final,other,collection,map
    FinalTypeField[] finalFields = this.finalFields;
    boolean metaShareEnabled = fury.getConfig().isMetaShareEnabled();
    for (int i = 0; i < finalFields.length; i++) {
      FinalTypeField fieldInfo = finalFields[i];
      boolean isFinal = !metaShareEnabled || this.isFinal[i];
      short classId = fieldInfo.classId;
      if (classId >= ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID
          && classId <= ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID) {
        fieldValues[counter++] = Serializers.readPrimitiveValue(fury, buffer, classId);
      } else {
        Object fieldValue =
            readFinalObjectFieldValue(
                binding, refResolver, typeResolver, fieldInfo, isFinal, buffer);
        fieldValues[counter++] = fieldValue;
      }
    }
    for (GenericTypeField fieldInfo : otherFields) {
      Object fieldValue = readOtherFieldValue(binding, fieldInfo, buffer);
      fieldValues[counter++] = fieldValue;
    }
    Generics generics = fury.getGenerics();
    for (GenericTypeField fieldInfo : containerFields) {
      Object fieldValue = readContainerFieldValue(binding, generics, fieldInfo, buffer);
      fieldValues[counter++] = fieldValue;
    }
    return fieldValues;
  }

  public T readAndSetFields(MemoryBuffer buffer, T obj) {
    Fury fury = this.fury;
    RefResolver refResolver = this.refResolver;
    TypeResolver typeResolver = this.typeResolver;
    if (fury.checkClassVersion()) {
      int hash = buffer.readInt32();
      checkClassVersion(fury, hash, classVersionHash);
    }
    // read order: primitive,boxed,final,other,collection,map
    FinalTypeField[] finalFields = this.finalFields;
    boolean metaShareEnabled = fury.getConfig().isMetaShareEnabled();
    for (int i = 0; i < finalFields.length; i++) {
      FinalTypeField fieldInfo = finalFields[i];
      boolean isFinal = !metaShareEnabled || this.isFinal[i];
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      short classId = fieldInfo.classId;
      if (readPrimitiveFieldValueFailed(fury, buffer, obj, fieldAccessor, classId)
          && readBasicObjectFieldValueFailed(fury, buffer, obj, fieldAccessor, classId)) {
        Object fieldValue =
            readFinalObjectFieldValue(
                binding, refResolver, typeResolver, fieldInfo, isFinal, buffer);
        fieldAccessor.putObject(obj, fieldValue);
      }
    }
    for (GenericTypeField fieldInfo : otherFields) {
      Object fieldValue = readOtherFieldValue(binding, fieldInfo, buffer);
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      fieldAccessor.putObject(obj, fieldValue);
    }
    Generics generics = fury.getGenerics();
    for (GenericTypeField fieldInfo : containerFields) {
      Object fieldValue = readContainerFieldValue(binding, generics, fieldInfo, buffer);
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
      SerializationBinding binding,
      RefResolver refResolver,
      TypeResolver typeResolver,
      FinalTypeField fieldInfo,
      boolean isFinal,
      MemoryBuffer buffer) {
    Serializer<Object> serializer = fieldInfo.classInfo.getSerializer();
    Object fieldValue;
    if (isFinal) {
      if (!fieldInfo.trackingRef) {
        return binding.readNullable(buffer, serializer);
      }
      // whether tracking ref is recorded in `fieldInfo.serializer`, so it's still
      // consistent with jit serializer.
      fieldValue = binding.readRef(buffer, serializer);
    } else {
      if (serializer.needToWriteRef()) {
        int nextReadRefId = refResolver.tryPreserveRefId(buffer);
        if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
          typeResolver.readClassInfo(buffer, fieldInfo.classInfo);
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
          typeResolver.readClassInfo(buffer, fieldInfo.classInfo);
          fieldValue = serializer.read(buffer);
        }
      }
    }
    return fieldValue;
  }

  static Object readOtherFieldValue(
      SerializationBinding binding, GenericTypeField fieldInfo, MemoryBuffer buffer) {
    Object fieldValue;
    if (fieldInfo.trackingRef) {
      fieldValue = binding.readRef(buffer, fieldInfo.classInfoHolder);
    } else {
      byte headFlag = buffer.readByte();
      if (headFlag == Fury.NULL_FLAG) {
        fieldValue = null;
      } else {
        fieldValue = binding.readNonRef(buffer, fieldInfo.classInfoHolder);
      }
    }
    return fieldValue;
  }

  static Object readContainerFieldValue(
      SerializationBinding binding,
      Generics generics,
      GenericTypeField fieldInfo,
      MemoryBuffer buffer) {
    Object fieldValue;
    if (fieldInfo.trackingRef) {
      generics.pushGenericType(fieldInfo.genericType);
      fieldValue = binding.readRef(buffer, fieldInfo.classInfoHolder);
      generics.popGenericType();
    } else {
      byte headFlag = buffer.readByte();
      if (headFlag == Fury.NULL_FLAG) {
        fieldValue = null;
      } else {
        generics.pushGenericType(fieldInfo.genericType);
        fieldValue = binding.readNonRef(buffer, fieldInfo.classInfoHolder);
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
        return true;
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
        return true;
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
