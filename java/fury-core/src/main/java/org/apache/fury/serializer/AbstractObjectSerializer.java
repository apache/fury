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

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.fury.Fury;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.collection.Tuple3;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.FieldAccessor;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.reflect.TypeRef;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.resolver.TypeResolver;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.DescriptorGrouper;
import org.apache.fury.type.FinalObjectTypeStub;
import org.apache.fury.type.GenericType;
import org.apache.fury.type.Generics;
import org.apache.fury.util.record.RecordComponent;
import org.apache.fury.util.record.RecordInfo;
import org.apache.fury.util.record.RecordUtils;

public abstract class AbstractObjectSerializer<T> extends Serializer<T> {
  protected final RefResolver refResolver;
  protected final ClassResolver classResolver;
  protected final boolean isRecord;
  protected final MethodHandle constructor;
  private InternalFieldInfo[] fieldInfos;
  private RecordInfo copyRecordInfo;

  public AbstractObjectSerializer(Fury fury, Class<T> type) {
    this(
        fury,
        type,
        RecordUtils.isRecord(type)
            ? RecordUtils.getRecordConstructor(type).f1
            : ReflectionUtils.getCtrHandle(type, false));
  }

  public AbstractObjectSerializer(Fury fury, Class<T> type, MethodHandle constructor) {
    super(fury, type);
    this.refResolver = fury.getRefResolver();
    this.classResolver = fury.getClassResolver();
    this.isRecord = RecordUtils.isRecord(type);
    this.constructor = constructor;
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
      fieldValue = binding.readRef(buffer, fieldInfo);
    } else {
      byte headFlag = buffer.readByte();
      if (headFlag == Fury.NULL_FLAG) {
        fieldValue = null;
      } else {
        fieldValue = binding.readNonRef(buffer, fieldInfo);
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
      fieldValue = binding.readContainerFieldValueRef(buffer, fieldInfo);
      generics.popGenericType();
    } else {
      byte headFlag = buffer.readByte();
      if (headFlag == Fury.NULL_FLAG) {
        fieldValue = null;
      } else {
        generics.pushGenericType(fieldInfo.genericType);
        fieldValue = binding.readContainerFieldValue(buffer, fieldInfo);
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

  @Override
  public T copy(T originObj) {
    if (immutable) {
      return originObj;
    }
    if (isRecord) {
      return copyRecord(originObj);
    }
    T newObj = newBean();
    if (needToCopyRef) {
      fury.reference(originObj, newObj);
    }
    copyFields(originObj, newObj);
    return newObj;
  }

  private T copyRecord(T originObj) {
    Object[] fieldValues = copyFields(originObj);
    try {
      T t = (T) constructor.invokeWithArguments(fieldValues);
      Arrays.fill(copyRecordInfo.getRecordComponents(), null);
      fury.reference(originObj, t);
      return t;
    } catch (Throwable e) {
      Platform.throwException(e);
    }
    return originObj;
  }

  private Object[] copyFields(T originObj) {
    InternalFieldInfo[] fieldInfos = this.fieldInfos;
    if (fieldInfos == null) {
      fieldInfos = buildFieldsInfo();
    }
    Object[] fieldValues = new Object[fieldInfos.length];
    for (int i = 0; i < fieldInfos.length; i++) {
      InternalFieldInfo fieldInfo = fieldInfos[i];
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      long fieldOffset = fieldAccessor.getFieldOffset();
      if (fieldOffset != -1) {
        fieldValues[i] = copyField(originObj, fieldOffset, fieldInfo.classId);
      } else {
        // field in record class has offset -1
        Object fieldValue = fieldAccessor.get(originObj);
        fieldValues[i] = fury.copyObject(fieldValue, fieldInfo.classId);
      }
    }
    return RecordUtils.remapping(copyRecordInfo, fieldValues);
  }

  private void copyFields(T originObj, T newObj) {
    InternalFieldInfo[] fieldInfos = this.fieldInfos;
    if (fieldInfos == null) {
      fieldInfos = buildFieldsInfo();
    }
    for (InternalFieldInfo fieldInfo : fieldInfos) {
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      long fieldOffset = fieldAccessor.getFieldOffset();
      // record class won't go to this path;
      assert fieldOffset != -1;
      switch (fieldInfo.classId) {
        case ClassResolver.PRIMITIVE_BYTE_CLASS_ID:
          Platform.putByte(newObj, fieldOffset, Platform.getByte(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_CHAR_CLASS_ID:
          Platform.putChar(newObj, fieldOffset, Platform.getChar(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_SHORT_CLASS_ID:
          Platform.putShort(newObj, fieldOffset, Platform.getShort(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_INT_CLASS_ID:
          Platform.putInt(newObj, fieldOffset, Platform.getInt(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_LONG_CLASS_ID:
          Platform.putLong(newObj, fieldOffset, Platform.getLong(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_FLOAT_CLASS_ID:
          Platform.putFloat(newObj, fieldOffset, Platform.getFloat(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID:
          Platform.putDouble(newObj, fieldOffset, Platform.getDouble(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID:
          Platform.putBoolean(newObj, fieldOffset, Platform.getBoolean(originObj, fieldOffset));
          break;
        case ClassResolver.BOOLEAN_CLASS_ID:
        case ClassResolver.BYTE_CLASS_ID:
        case ClassResolver.CHAR_CLASS_ID:
        case ClassResolver.SHORT_CLASS_ID:
        case ClassResolver.INTEGER_CLASS_ID:
        case ClassResolver.FLOAT_CLASS_ID:
        case ClassResolver.LONG_CLASS_ID:
        case ClassResolver.DOUBLE_CLASS_ID:
        case ClassResolver.STRING_CLASS_ID:
          Platform.putObject(newObj, fieldOffset, Platform.getObject(originObj, fieldOffset));
          break;
        default:
          Platform.putObject(
              newObj, fieldOffset, fury.copyObject(Platform.getObject(originObj, fieldOffset)));
      }
    }
  }

  public static void copyFields(
      Fury fury, InternalFieldInfo[] fieldInfos, Object originObj, Object newObj) {
    for (InternalFieldInfo fieldInfo : fieldInfos) {
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      long fieldOffset = fieldAccessor.getFieldOffset();
      // record class won't go to this path;
      assert fieldOffset != -1;
      switch (fieldInfo.classId) {
        case ClassResolver.PRIMITIVE_BYTE_CLASS_ID:
          Platform.putByte(newObj, fieldOffset, Platform.getByte(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_CHAR_CLASS_ID:
          Platform.putChar(newObj, fieldOffset, Platform.getChar(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_SHORT_CLASS_ID:
          Platform.putShort(newObj, fieldOffset, Platform.getShort(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_INT_CLASS_ID:
          Platform.putInt(newObj, fieldOffset, Platform.getInt(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_LONG_CLASS_ID:
          Platform.putLong(newObj, fieldOffset, Platform.getLong(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_FLOAT_CLASS_ID:
          Platform.putFloat(newObj, fieldOffset, Platform.getFloat(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID:
          Platform.putDouble(newObj, fieldOffset, Platform.getDouble(originObj, fieldOffset));
          break;
        case ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID:
          Platform.putBoolean(newObj, fieldOffset, Platform.getBoolean(originObj, fieldOffset));
          break;
        case ClassResolver.BOOLEAN_CLASS_ID:
        case ClassResolver.BYTE_CLASS_ID:
        case ClassResolver.CHAR_CLASS_ID:
        case ClassResolver.SHORT_CLASS_ID:
        case ClassResolver.INTEGER_CLASS_ID:
        case ClassResolver.FLOAT_CLASS_ID:
        case ClassResolver.LONG_CLASS_ID:
        case ClassResolver.DOUBLE_CLASS_ID:
        case ClassResolver.STRING_CLASS_ID:
          Platform.putObject(newObj, fieldOffset, Platform.getObject(originObj, fieldOffset));
          break;
        default:
          Platform.putObject(
              newObj, fieldOffset, fury.copyObject(Platform.getObject(originObj, fieldOffset)));
      }
    }
  }

  private Object copyField(Object targetObject, long fieldOffset, short classId) {
    switch (classId) {
      case ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID:
        return Platform.getBoolean(targetObject, fieldOffset);
      case ClassResolver.PRIMITIVE_BYTE_CLASS_ID:
        return Platform.getByte(targetObject, fieldOffset);
      case ClassResolver.PRIMITIVE_CHAR_CLASS_ID:
        return Platform.getChar(targetObject, fieldOffset);
      case ClassResolver.PRIMITIVE_SHORT_CLASS_ID:
        return Platform.getShort(targetObject, fieldOffset);
      case ClassResolver.PRIMITIVE_INT_CLASS_ID:
        return Platform.getInt(targetObject, fieldOffset);
      case ClassResolver.PRIMITIVE_FLOAT_CLASS_ID:
        return Platform.getFloat(targetObject, fieldOffset);
      case ClassResolver.PRIMITIVE_LONG_CLASS_ID:
        return Platform.getLong(targetObject, fieldOffset);
      case ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID:
        return Platform.getDouble(targetObject, fieldOffset);
      case ClassResolver.BOOLEAN_CLASS_ID:
      case ClassResolver.BYTE_CLASS_ID:
      case ClassResolver.CHAR_CLASS_ID:
      case ClassResolver.SHORT_CLASS_ID:
      case ClassResolver.INTEGER_CLASS_ID:
      case ClassResolver.FLOAT_CLASS_ID:
      case ClassResolver.LONG_CLASS_ID:
      case ClassResolver.DOUBLE_CLASS_ID:
      case ClassResolver.STRING_CLASS_ID:
        return Platform.getObject(targetObject, fieldOffset);
      default:
        return fury.copyObject(Platform.getObject(targetObject, fieldOffset));
    }
  }

  private InternalFieldInfo[] buildFieldsInfo() {
    List<Descriptor> descriptors = new ArrayList<>();
    if (RecordUtils.isRecord(type)) {
      RecordComponent[] components = RecordUtils.getRecordComponents(type);
      assert components != null;
      try {
        for (RecordComponent component : components) {
          Field field = type.getDeclaredField(component.getName());
          descriptors.add(
              new Descriptor(
                  field, TypeRef.of(field.getGenericType()), component.getAccessor(), null));
        }
      } catch (NoSuchFieldException e) {
        // impossible
        Platform.throwException(e);
      }
    } else {
      for (Field field : ReflectionUtils.getFields(type, true)) {
        if (!Modifier.isStatic(field.getModifiers())) {
          descriptors.add(new Descriptor(field, TypeRef.of(field.getGenericType()), null, null));
        }
      }
    }
    DescriptorGrouper descriptorGrouper =
        fury.getClassResolver().createDescriptorGrouper(descriptors, false);
    Tuple3<Tuple2<FinalTypeField[], boolean[]>, GenericTypeField[], GenericTypeField[]> infos =
        buildFieldInfos(fury, descriptorGrouper);
    fieldInfos = new InternalFieldInfo[descriptors.size()];
    System.arraycopy(infos.f0.f0, 0, fieldInfos, 0, infos.f0.f0.length);
    System.arraycopy(infos.f1, 0, fieldInfos, infos.f0.f0.length, infos.f1.length);
    System.arraycopy(infos.f2, 0, fieldInfos, fieldInfos.length - infos.f2.length, infos.f2.length);
    if (isRecord) {
      List<String> fieldNames =
          Arrays.stream(fieldInfos)
              .map(f -> f.fieldAccessor.getField().getName())
              .collect(Collectors.toList());
      copyRecordInfo = new RecordInfo(type, fieldNames);
    }
    return fieldInfos;
  }

  public static InternalFieldInfo[] buildFieldsInfo(Fury fury, List<Field> fields) {
    List<Descriptor> descriptors = new ArrayList<>();
    for (Field field : fields) {
      if (!Modifier.isTransient(field.getModifiers()) && !Modifier.isStatic(field.getModifiers())) {
        descriptors.add(new Descriptor(field, TypeRef.of(field.getGenericType()), null, null));
      }
    }
    DescriptorGrouper descriptorGrouper =
        fury.getClassResolver().createDescriptorGrouper(descriptors, false);
    Tuple3<Tuple2<FinalTypeField[], boolean[]>, GenericTypeField[], GenericTypeField[]> infos =
        buildFieldInfos(fury, descriptorGrouper);
    InternalFieldInfo[] fieldInfos = new InternalFieldInfo[descriptors.size()];
    System.arraycopy(infos.f0.f0, 0, fieldInfos, 0, infos.f0.f0.length);
    System.arraycopy(infos.f1, 0, fieldInfos, infos.f0.f0.length, infos.f1.length);
    System.arraycopy(infos.f2, 0, fieldInfos, fieldInfos.length - infos.f2.length, infos.f2.length);
    return fieldInfos;
  }

  protected T newBean() {
    if (constructor != null) {
      try {
        return (T) constructor.invoke();
      } catch (Throwable e) {
        Platform.throwException(e);
      }
    }
    return Platform.newInstance(type);
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
              descriptor.getTypeRef(),
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
        d.getTypeRef(),
        d.getDeclaringClass() + "." + d.getName(),
        // `d.getField()` will be null when peer class doesn't have this field.
        d.getField() != null ? FieldAccessor.createAccessor(d.getField()) : null,
        fury);
  }

  private static GenericTypeField buildContainerField(Fury fury, Descriptor d) {
    return new GenericTypeField(
        d.getTypeRef(),
        d.getDeclaringClass() + "." + d.getName(),
        d.getField() != null ? FieldAccessor.createAccessor(d.getField()) : null,
        fury);
  }

  public static class InternalFieldInfo {
    private final TypeRef<?> typeRef;
    protected final short classId;
    protected final String qualifiedFieldName;
    protected final FieldAccessor fieldAccessor;

    private InternalFieldInfo(
        TypeRef<?> typeRef, short classId, String qualifiedFieldName, FieldAccessor fieldAccessor) {
      this.typeRef = typeRef;
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
    final boolean trackingRef;

    private FinalTypeField(TypeRef<?> type, String fieldName, FieldAccessor accessor, Fury fury) {
      super(type, getRegisteredClassId(fury, type.getRawType()), fieldName, accessor);
      // invoke `copy` to avoid ObjectSerializer construct clear serializer by `clearSerializer`.
      if (type.getRawType() == FinalObjectTypeStub.class) {
        // `FinalObjectTypeStub` has no fields, using its `classInfo`
        // will make deserialization failed.
        classInfo = null;
      } else {
        classInfo = fury.getClassResolver().getClassInfo(type.getRawType());
      }
      trackingRef = fury.getClassResolver().needToWriteRef(type);
    }
  }

  static final class GenericTypeField extends InternalFieldInfo {
    final GenericType genericType;
    final ClassInfoHolder classInfoHolder;
    final boolean trackingRef;
    final boolean isArray;
    final ClassInfo containerClassInfo;

    private GenericTypeField(
        TypeRef<?> typeRef, String qualifiedFieldName, FieldAccessor accessor, Fury fury) {
      super(typeRef, getRegisteredClassId(fury, getRawType(typeRef)), qualifiedFieldName, accessor);
      // TODO support generics <T> in Pojo<T>, see ComplexObjectSerializer.getGenericTypes
      ClassResolver classResolver = fury.getClassResolver();
      GenericType t = classResolver.buildGenericType(typeRef);
      Class<?> cls = t.getCls();
      if (t.getTypeParametersCount() > 0) {
        boolean skip =
            Arrays.stream(t.getTypeParameters()).allMatch(p -> p.getCls() == Object.class);
        if (skip) {
          t = new GenericType(t.getTypeRef(), t.isMonomorphic());
        }
      }
      genericType = t;
      classInfoHolder = classResolver.nilClassInfoHolder();
      trackingRef = classResolver.needToWriteRef(typeRef);
      isArray = cls.isArray();
      if (!fury.isCrossLanguage()) {
        containerClassInfo = null;
      } else {
        if (classResolver.isMap(cls)
            || classResolver.isCollection(cls)
            || classResolver.isSet(cls)) {
          containerClassInfo = fury.getXtypeResolver().getClassInfo(cls);
        } else {
          containerClassInfo = null;
        }
      }
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
}
