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

import com.google.common.base.Preconditions;
import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.resolver.ClassInfo;
import io.fury.resolver.ClassResolver;
import io.fury.resolver.FieldResolver;
import io.fury.resolver.RefResolver;
import io.fury.util.FieldAccessor;
import io.fury.util.Platform;
import io.fury.util.RecordUtils;
import io.fury.util.ReflectionUtils;
import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This Serializer provides both forward and backward compatibility: fields can be added or removed
 * without invalidating previously serialized bytes.
 *
 * @author chaokunyang
 * @see FieldResolver
 */
// TODO(chaokunyang) support generics optimization for {@code SomeClass<T>}
// TODO(chaokunyang) support generics optimization for nested collection/map fields.
@SuppressWarnings({"unchecked", "rawtypes"})
public final class CompatibleSerializer<T> extends CompatibleSerializerBase<T> {
  private static final int INDEX_FOR_SKIP_FILL_VALUES = -1;
  private final RefResolver refResolver;
  private final ClassResolver classResolver;
  private final FieldResolver fieldResolver;
  private final boolean isRecord;
  private final MethodHandle constructor;
  private final boolean compressNumber;
  private final int[] recordComponentsIndex;
  private final Object[] recordComponents;

  public CompatibleSerializer(Fury fury, Class<T> cls) {
    super(fury, cls);
    this.refResolver = fury.getRefResolver();
    this.classResolver = fury.getClassResolver();
    // Use `setSerializerIfAbsent` to avoid overwriting existing serializer for class when used
    // as data serializer.
    classResolver.setSerializerIfAbsent(cls, this);
    isRecord = RecordUtils.isRecord(type);
    if (isRecord) {
      constructor = RecordUtils.getRecordConstructor(type).f1;
    } else {
      this.constructor = ReflectionUtils.getExecutableNoArgConstructorHandle(type);
    }
    fieldResolver = classResolver.getFieldResolver(cls);
    List<String> fieldNames = fieldResolver.getAllFieldsList().stream()
      .map(FieldResolver.FieldInfo::getName)
      .collect(Collectors.toList());
    recordComponentsIndex = Serializers.buildRecordComponentMapping(cls, fieldNames);
    assert recordComponentsIndex != null;
    recordComponents = new Object[recordComponentsIndex.length];
    compressNumber = fury.compressNumber();
  }

  public CompatibleSerializer(Fury fury, Class<T> cls, FieldResolver fieldResolver) {
    super(fury, cls);
    this.refResolver = fury.getRefResolver();
    this.classResolver = fury.getClassResolver();
    isRecord = RecordUtils.isRecord(type);
    Preconditions.checkArgument(!isRecord, cls);
    recordComponentsIndex = null;
    recordComponents = null;
    this.constructor = null;
    this.fieldResolver = fieldResolver;
    compressNumber = fury.compressNumber();
  }

  @Override
  public void write(MemoryBuffer buffer, T value) {
    for (FieldResolver.FieldInfo fieldInfo : fieldResolver.getEmbedTypes4Fields()) {
      buffer.writeInt((int) fieldInfo.getEncodedFieldInfo());
      readAndWriteFieldValue(buffer, fieldInfo, value);
    }
    for (FieldResolver.FieldInfo fieldInfo : fieldResolver.getEmbedTypes9Fields()) {
      buffer.writeLong(fieldInfo.getEncodedFieldInfo());
      readAndWriteFieldValue(buffer, fieldInfo, value);
    }
    for (FieldResolver.FieldInfo fieldInfo : fieldResolver.getEmbedTypesHashFields()) {
      buffer.writeLong(fieldInfo.getEncodedFieldInfo());
      readAndWriteFieldValue(buffer, fieldInfo, value);
    }
    for (FieldResolver.FieldInfo fieldInfo : fieldResolver.getSeparateTypesHashFields()) {
      buffer.writeLong(fieldInfo.getEncodedFieldInfo());
      readAndWriteFieldValue(buffer, fieldInfo, value);
    }
    buffer.writeLong(fieldResolver.getEndTag());
  }

  public void writeFieldsValues(MemoryBuffer buffer, Object[] vals) {
    FieldResolver fieldResolver = this.fieldResolver;
    Fury fury = this.fury;
    int index = 0;
    for (FieldResolver.FieldInfo fieldInfo : fieldResolver.getEmbedTypes4Fields()) {
      buffer.writeInt((int) fieldInfo.getEncodedFieldInfo());
      writeFieldValue(fieldInfo, buffer, vals[index++]);
    }
    for (FieldResolver.FieldInfo fieldInfo : fieldResolver.getEmbedTypes9Fields()) {
      buffer.writeLong(fieldInfo.getEncodedFieldInfo());
      writeFieldValue(fieldInfo, buffer, vals[index++]);
    }
    for (FieldResolver.FieldInfo fieldInfo : fieldResolver.getEmbedTypesHashFields()) {
      buffer.writeLong(fieldInfo.getEncodedFieldInfo());
      writeFieldValue(fieldInfo, buffer, vals[index++]);
    }
    for (FieldResolver.FieldInfo fieldInfo : fieldResolver.getSeparateTypesHashFields()) {
      buffer.writeLong(fieldInfo.getEncodedFieldInfo());
      Object value = vals[index++];
      if (!fury.getRefResolver().writeRefOrNull(buffer, value)) {
        byte fieldType = fieldInfo.getFieldType();
        buffer.writeByte(fieldType);
        Preconditions.checkArgument(fieldType == FieldResolver.FieldTypes.OBJECT);
        ClassInfo classInfo = fieldInfo.getClassInfo(value.getClass());
        fury.writeNonRef(buffer, value, classInfo);
      }
    }
    buffer.writeLong(fieldResolver.getEndTag());
  }

  private void readAndWriteFieldValue(
      MemoryBuffer buffer, FieldResolver.FieldInfo fieldInfo, Object targetObject) {
    FieldAccessor fieldAccessor = fieldInfo.getFieldAccessor();
    short classId = fieldInfo.getEmbeddedClassId();
    if (ObjectSerializer.writePrimitiveFieldValueFailed(
        fury, buffer, targetObject, fieldAccessor, classId)) {
      Object fieldValue;
      fieldValue = fieldAccessor.getObject(targetObject);
      if (ObjectSerializer.writeBasicObjectFieldValueFailed(fury, buffer, fieldValue, classId)) {
        if (classId == ClassResolver.NO_CLASS_ID) { // SEPARATE_TYPES_HASH
          writeSeparateFieldValue(fieldInfo, buffer, fieldValue);
        } else {
          ClassInfo classInfo = fieldInfo.getClassInfo(classId);
          Serializer<Object> serializer = classInfo.getSerializer();
          fury.writeRef(buffer, fieldValue, serializer);
        }
      }
    }
  }

  private void writeFieldValue(
      FieldResolver.FieldInfo fieldInfo, MemoryBuffer buffer, Object fieldValue) {
    short classId = fieldInfo.getEmbeddedClassId();
    // PRIMITIVE fields, not need for null check.
    switch (classId) {
      case ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID:
        buffer.writeBoolean((Boolean) fieldValue);
        return;
      case ClassResolver.PRIMITIVE_BYTE_CLASS_ID:
        buffer.writeByte((Byte) fieldValue);
        return;
      case ClassResolver.PRIMITIVE_CHAR_CLASS_ID:
        buffer.writeChar((Character) fieldValue);
        return;
      case ClassResolver.PRIMITIVE_SHORT_CLASS_ID:
        buffer.writeShort((Short) fieldValue);
        return;
      case ClassResolver.PRIMITIVE_INT_CLASS_ID:
        if (compressNumber) {
          buffer.writeVarInt((Integer) fieldValue);
        } else {
          buffer.writeInt((Integer) fieldValue);
        }
        return;
      case ClassResolver.PRIMITIVE_FLOAT_CLASS_ID:
        buffer.writeFloat((Float) fieldValue);
        return;
      case ClassResolver.PRIMITIVE_LONG_CLASS_ID:
        if (compressNumber) {
          buffer.writeVarLong((Long) fieldValue);
        } else {
          buffer.writeLong((Long) fieldValue);
        }
        return;
      case ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID:
        buffer.writeDouble((Double) fieldValue);
        return;
      case ClassResolver.STRING_CLASS_ID:
        fury.writeJavaStringRef(buffer, (String) fieldValue);
        break;
      case ClassResolver.NO_CLASS_ID: // SEPARATE_TYPES_HASH
        writeSeparateFieldValue(fieldInfo, buffer, fieldValue);
        break;
      default:
        {
          ClassInfo classInfo = fieldInfo.getClassInfo(classId);
          Serializer<Object> serializer = classInfo.getSerializer();
          fury.writeRef(buffer, fieldValue, serializer);
        }
    }
  }

  private void writeSeparateFieldValue(
      FieldResolver.FieldInfo fieldInfo, MemoryBuffer buffer, Object fieldValue) {
    if (!refResolver.writeRefOrNull(buffer, fieldValue)) {
      byte fieldType = fieldInfo.getFieldType();
      buffer.writeByte(fieldType);
      if (fieldType == FieldResolver.FieldTypes.OBJECT) {
        ClassInfo classInfo = fieldInfo.getClassInfo(fieldValue.getClass());
        fury.writeNonRef(buffer, fieldValue, classInfo);
      } else {
        if (fieldType == FieldResolver.FieldTypes.COLLECTION_ELEMENT_FINAL) {
          writeCollectionField(
              buffer, (FieldResolver.CollectionFieldInfo) fieldInfo, (Collection) fieldValue);
        } else if (fieldType == FieldResolver.FieldTypes.MAP_KV_FINAL) {
          writeMapKVFinal(buffer, (FieldResolver.MapFieldInfo) fieldInfo, (Map) fieldValue);
        } else if (fieldType == FieldResolver.FieldTypes.MAP_KEY_FINAL) {
          writeMapKeyFinal(buffer, (FieldResolver.MapFieldInfo) fieldInfo, (Map) fieldValue);
        } else {
          Preconditions.checkArgument(fieldType == FieldResolver.FieldTypes.MAP_VALUE_FINAL);
          writeMapValueFinal(buffer, (FieldResolver.MapFieldInfo) fieldInfo, (Map) fieldValue);
        }
      }
    }
  }

  private void writeCollectionField(
      MemoryBuffer buffer, FieldResolver.CollectionFieldInfo fieldInfo, Collection fieldValue) {
    ClassInfo elementClassInfo = fieldInfo.getElementClassInfo();
    classResolver.writeClass(buffer, elementClassInfo);
    // following write is consistent with `BaseSeqCodecBuilder.serializeForCollection`
    ClassInfo classInfo = fieldInfo.getClassInfo(fieldValue.getClass());
    classResolver.writeClass(buffer, classInfo);
    CollectionSerializers.CollectionSerializer collectionSerializer =
        (CollectionSerializers.CollectionSerializer) classInfo.getSerializer();
    collectionSerializer.setElementSerializer(elementClassInfo.getSerializer());
    collectionSerializer.write(buffer, fieldValue);
  }

  private void writeMapKVFinal(
      MemoryBuffer buffer, FieldResolver.MapFieldInfo fieldInfo, Map fieldValue) {
    ClassInfo keyClassInfo = fieldInfo.getKeyClassInfo();
    ClassInfo valueClassInfo = fieldInfo.getValueClassInfo();
    classResolver.writeClass(buffer, keyClassInfo);
    classResolver.writeClass(buffer, valueClassInfo);
    // following write is consistent with `BaseSeqCodecBuilder.serializeForMap`
    ClassInfo classInfo = fieldInfo.getClassInfo(fieldValue.getClass());
    classResolver.writeClass(buffer, classInfo);
    MapSerializers.MapSerializer mapSerializer =
        (MapSerializers.MapSerializer) classInfo.getSerializer();
    mapSerializer.setKeySerializer(keyClassInfo.getSerializer());
    mapSerializer.setValueSerializer(valueClassInfo.getSerializer());
    mapSerializer.write(buffer, fieldValue);
  }

  private void writeMapKeyFinal(
      MemoryBuffer buffer, FieldResolver.MapFieldInfo fieldInfo, Map fieldValue) {
    ClassInfo keyClassInfo = fieldInfo.getKeyClassInfo();
    classResolver.writeClass(buffer, keyClassInfo);
    // following write is consistent with `BaseSeqCodecBuilder.serializeForMap`
    ClassInfo classInfo = fieldInfo.getClassInfo(fieldValue.getClass());
    classResolver.writeClass(buffer, classInfo);
    MapSerializers.MapSerializer mapSerializer =
        (MapSerializers.MapSerializer) classInfo.getSerializer();
    mapSerializer.setKeySerializer(keyClassInfo.getSerializer());
    mapSerializer.write(buffer, fieldValue);
  }

  private void writeMapValueFinal(
      MemoryBuffer buffer, FieldResolver.MapFieldInfo fieldInfo, Map fieldValue) {
    ClassInfo valueClassInfo = fieldInfo.getValueClassInfo();
    classResolver.writeClass(buffer, valueClassInfo);
    // following write is consistent with `BaseSeqCodecBuilder.serializeForMap`
    ClassInfo classInfo = fieldInfo.getClassInfo(fieldValue.getClass());
    classResolver.writeClass(buffer, classInfo);
    MapSerializers.MapSerializer mapSerializer =
        (MapSerializers.MapSerializer) classInfo.getSerializer();
    mapSerializer.setValueSerializer(valueClassInfo.getSerializer());
    mapSerializer.write(buffer, fieldValue);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T read(MemoryBuffer buffer) {
    if (isRecord) {
      Object[] fieldValues = new Object[fieldResolver.getNumFields()];
      readFields(buffer, fieldValues);
      Serializers.remapping(recordComponentsIndex, fieldValues, recordComponents);
      assert constructor != null;
      try {
        T t = (T) constructor.invokeWithArguments(recordComponents);
        Arrays.fill(recordComponents, null);
        return t;
      } catch (Throwable e) {
        Platform.throwException(e);
      }
    }
    T obj = (T) newBean();
    refResolver.reference(obj);
    return readAndSetFields(buffer, obj);
  }

  @Override
  public T readAndSetFields(MemoryBuffer buffer, T obj) {
    long partFieldInfo = readEmbedTypes4Fields(buffer, obj, null, INDEX_FOR_SKIP_FILL_VALUES);
    long endTag = fieldResolver.getEndTag();
    if (partFieldInfo == endTag) {
      return obj;
    }
    long tmp = buffer.readInt();
    partFieldInfo = tmp << 32 | (partFieldInfo & 0x00000000ffffffffL);
    partFieldInfo =
        readEmbedTypes9Fields(buffer, partFieldInfo, obj, null, INDEX_FOR_SKIP_FILL_VALUES);
    if (partFieldInfo == endTag) {
      return obj;
    }
    partFieldInfo =
        readEmbedTypesHashFields(buffer, partFieldInfo, obj, null, INDEX_FOR_SKIP_FILL_VALUES);
    if (partFieldInfo == endTag) {
      return obj;
    }
    readSeparateTypesHashField(buffer, partFieldInfo, obj, null, INDEX_FOR_SKIP_FILL_VALUES);
    return obj;
  }

  public void readFields(MemoryBuffer buffer, Object[] vals) {
    int startIndex = 0;
    long partFieldInfo = readEmbedTypes4Fields(buffer, null, vals, startIndex);
    long endTag = fieldResolver.getEndTag();
    if (partFieldInfo == endTag) {
      return;
    }
    startIndex += fieldResolver.getEmbedTypes4Fields().length;
    long tmp = buffer.readInt();
    partFieldInfo = tmp << 32 | (partFieldInfo & 0x00000000ffffffffL);
    partFieldInfo = readEmbedTypes9Fields(buffer, partFieldInfo, null, vals, startIndex);
    if (partFieldInfo == endTag) {
      return;
    }
    startIndex += fieldResolver.getEmbedTypes9Fields().length;
    partFieldInfo = readEmbedTypesHashFields(buffer, partFieldInfo, null, vals, startIndex);
    if (partFieldInfo == endTag) {
      return;
    }
    startIndex += fieldResolver.getEmbedTypesHashFields().length;
    readSeparateTypesHashField(buffer, partFieldInfo, null, vals, startIndex);
  }

  private long readEmbedTypes4Fields(
      MemoryBuffer buffer, Object obj, Object[] vals, int startIndex) {
    long partFieldInfo = buffer.readInt();
    FieldResolver.FieldInfo[] embedTypes4Fields = fieldResolver.getEmbedTypes4Fields();
    if (embedTypes4Fields.length > 0) {
      long minFieldInfo = embedTypes4Fields[0].getEncodedFieldInfo();
      while ((partFieldInfo & 0b11) == FieldResolver.EMBED_TYPES_4_FLAG
          && partFieldInfo < minFieldInfo) {
        long part = fieldResolver.skipDataBy4(buffer, (int) partFieldInfo);
        if (part != partFieldInfo) {
          return part;
        }
        partFieldInfo = buffer.readInt();
      }
      for (int i = 0; i < embedTypes4Fields.length; i++) {
        FieldResolver.FieldInfo fieldInfo = embedTypes4Fields[i];
        long encodedFieldInfo = fieldInfo.getEncodedFieldInfo();
        if (encodedFieldInfo == partFieldInfo) {
          if (obj != null) {
            readAndSetFieldValue(fieldInfo, buffer, obj);
          } else {
            vals[startIndex + i] = readFieldValue(fieldInfo, buffer);
          }
          partFieldInfo = buffer.readInt();
        } else {
          if ((partFieldInfo & 0b11) == FieldResolver.EMBED_TYPES_4_FLAG) {
            if (partFieldInfo < encodedFieldInfo) {
              long part = fieldResolver.skipDataBy4(buffer, (int) partFieldInfo);
              if (part != partFieldInfo) {
                return part;
              }
              partFieldInfo = buffer.readInt();
              i--;
            }
          } else {
            break;
          }
        }
      }
    }
    while ((partFieldInfo & 0b11) == FieldResolver.EMBED_TYPES_4_FLAG) {
      long part = fieldResolver.skipDataBy4(buffer, (int) partFieldInfo);
      if (part != partFieldInfo) {
        return part;
      }
      partFieldInfo = buffer.readInt();
    }
    return partFieldInfo;
  }

  private long readEmbedTypes9Fields(
      MemoryBuffer buffer, long partFieldInfo, Object obj, Object[] vals, int startIndex) {
    FieldResolver.FieldInfo[] embedTypes9Fields = fieldResolver.getEmbedTypes9Fields();
    if (embedTypes9Fields.length > 0) {
      long minFieldInfo = embedTypes9Fields[0].getEncodedFieldInfo();
      while ((partFieldInfo & 0b111) == FieldResolver.EMBED_TYPES_9_FLAG
          && partFieldInfo < minFieldInfo) {
        long part = fieldResolver.skipDataBy8(buffer, partFieldInfo);
        if (part != partFieldInfo) {
          return part;
        }
        partFieldInfo = buffer.readLong();
      }

      for (int i = 0; i < embedTypes9Fields.length; i++) {
        FieldResolver.FieldInfo fieldInfo = embedTypes9Fields[i];
        long encodedFieldInfo = fieldInfo.getEncodedFieldInfo();
        if (encodedFieldInfo == partFieldInfo) {
          if (obj != null) {
            readAndSetFieldValue(fieldInfo, buffer, obj);
          } else {
            vals[startIndex + i] = readFieldValue(fieldInfo, buffer);
          }
          partFieldInfo = buffer.readLong();
        } else {
          if ((partFieldInfo & 0b111) == FieldResolver.EMBED_TYPES_9_FLAG) {
            if (partFieldInfo < encodedFieldInfo) {
              long part = fieldResolver.skipDataBy8(buffer, partFieldInfo);
              if (part != partFieldInfo) {
                return part;
              }
              partFieldInfo = buffer.readLong();
              i--;
            }
          } else {
            break;
          }
        }
      }
    }
    while ((partFieldInfo & 0b111) == FieldResolver.EMBED_TYPES_9_FLAG) {
      long part = fieldResolver.skipDataBy8(buffer, partFieldInfo);
      if (part != partFieldInfo) {
        return part;
      }
      partFieldInfo = buffer.readLong();
    }
    return partFieldInfo;
  }

  private long readEmbedTypesHashFields(
      MemoryBuffer buffer, long partFieldInfo, Object obj, Object[] vals, int startIndex) {
    FieldResolver.FieldInfo[] embedTypesHashFields = fieldResolver.getEmbedTypesHashFields();
    if (embedTypesHashFields.length > 0) {
      long minFieldInfo = embedTypesHashFields[0].getEncodedFieldInfo();
      while ((partFieldInfo & 0b111) == FieldResolver.EMBED_TYPES_HASH_FLAG
          && partFieldInfo < minFieldInfo) {
        long part = fieldResolver.skipDataBy8(buffer, partFieldInfo);
        if (part != partFieldInfo) {
          return part;
        }
        partFieldInfo = buffer.readLong();
      }

      for (int i = 0; i < embedTypesHashFields.length; i++) {
        FieldResolver.FieldInfo fieldInfo = embedTypesHashFields[i];
        long encodedFieldInfo = fieldInfo.getEncodedFieldInfo();
        if (encodedFieldInfo == partFieldInfo) {
          if (obj != null) {
            readAndSetFieldValue(fieldInfo, buffer, obj);
          } else {
            vals[startIndex + i] = readFieldValue(fieldInfo, buffer);
          }
          partFieldInfo = buffer.readLong();
        } else {
          if ((partFieldInfo & 0b111) == FieldResolver.EMBED_TYPES_HASH_FLAG) {
            if (partFieldInfo < encodedFieldInfo) {
              long part = fieldResolver.skipDataBy8(buffer, partFieldInfo);
              if (part != partFieldInfo) {
                return part;
              }
              partFieldInfo = buffer.readLong();
              i--;
            }
          } else {
            break;
          }
        }
      }
    }
    while ((partFieldInfo & 0b111) == FieldResolver.EMBED_TYPES_HASH_FLAG) {
      long part = fieldResolver.skipDataBy8(buffer, partFieldInfo);
      if (part != partFieldInfo) {
        return part;
      }
      partFieldInfo = buffer.readLong();
    }
    return partFieldInfo;
  }

  private void readSeparateTypesHashField(
      MemoryBuffer buffer, long partFieldInfo, Object obj, Object[] vals, int startIndex) {
    FieldResolver.FieldInfo[] separateTypesHashFields = fieldResolver.getSeparateTypesHashFields();
    if (separateTypesHashFields.length > 0) {
      long minFieldInfo = separateTypesHashFields[0].getEncodedFieldInfo();
      while ((partFieldInfo & 0b11) == FieldResolver.SEPARATE_TYPES_HASH_FLAG
          && partFieldInfo < minFieldInfo) {
        long part = fieldResolver.skipDataBy8(buffer, partFieldInfo);
        if (part != partFieldInfo) {
          return;
        }
        partFieldInfo = buffer.readLong();
      }
      for (int i = 0; i < separateTypesHashFields.length; i++) {
        FieldResolver.FieldInfo fieldInfo = separateTypesHashFields[i];
        long encodedFieldInfo = fieldInfo.getEncodedFieldInfo();
        if (encodedFieldInfo == partFieldInfo) {
          if (obj != null) {
            readAndSetFieldValue(fieldInfo, buffer, obj);
          } else {
            vals[startIndex + i] = readFieldValue(fieldInfo, buffer);
          }
          partFieldInfo = buffer.readLong();
        } else {
          if ((partFieldInfo & 0b11) == FieldResolver.SEPARATE_TYPES_HASH_FLAG) {
            if (partFieldInfo < encodedFieldInfo) {
              long part = fieldResolver.skipDataBy8(buffer, partFieldInfo);
              if (part != partFieldInfo) {
                return;
              }
              partFieldInfo = buffer.readLong();
              i--;
            }
          } else {
            break;
          }
        }
      }
    }
    fieldResolver.skipEndFields(buffer, partFieldInfo);
  }

  private void readAndSetFieldValue(
      FieldResolver.FieldInfo fieldInfo, MemoryBuffer buffer, Object targetObject) {
    FieldAccessor fieldAccessor = fieldInfo.getFieldAccessor();
    short classId = fieldInfo.getEmbeddedClassId();
    if (ObjectSerializer.readPrimitiveFieldValueFailed(
            fury, buffer, targetObject, fieldAccessor, classId)
        && ObjectSerializer.readBasicObjectFieldValueFailed(
            fury, buffer, targetObject, fieldAccessor, classId)) {
      if (classId == ClassResolver.NO_CLASS_ID) {
        // SEPARATE_TYPES_HASH
        Object fieldValue = fieldResolver.readObjectField(buffer, fieldInfo);
        fieldAccessor.putObject(targetObject, fieldValue);
      } else {
        ClassInfo classInfo = fieldInfo.getClassInfo(classId);
        Serializer<Object> serializer = classInfo.getSerializer();
        fieldAccessor.putObject(targetObject, fury.readRef(buffer, serializer));
      }
    }
  }

  private Object readFieldValue(FieldResolver.FieldInfo fieldInfo, MemoryBuffer buffer) {
    short classId = fieldInfo.getEmbeddedClassId();
    // PRIMITIVE fields, not need for null check.
    switch (classId) {
      case ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID:
        return buffer.readBoolean();
      case ClassResolver.PRIMITIVE_BYTE_CLASS_ID:
        return buffer.readByte();
      case ClassResolver.PRIMITIVE_CHAR_CLASS_ID:
        return buffer.readChar();
      case ClassResolver.PRIMITIVE_SHORT_CLASS_ID:
        return buffer.readShort();
      case ClassResolver.PRIMITIVE_INT_CLASS_ID:
        if (compressNumber) {
          return buffer.readVarInt();
        } else {
          return buffer.readInt();
        }
      case ClassResolver.PRIMITIVE_FLOAT_CLASS_ID:
        return buffer.readFloat();
      case ClassResolver.PRIMITIVE_LONG_CLASS_ID:
        if (compressNumber) {
          return buffer.readVarLong();
        } else {
          return buffer.readLong();
        }
      case ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID:
        return buffer.readDouble();
      case ClassResolver.STRING_CLASS_ID:
        return fury.readJavaStringRef(buffer);
      case ClassResolver.NO_CLASS_ID:
        return fieldResolver.readObjectField(buffer, fieldInfo);
      default:
        {
          ClassInfo classInfo = fieldInfo.getClassInfo(classId);
          Serializer<Object> serializer = classInfo.getSerializer();
          return fury.readRef(buffer, serializer);
        }
    }
  }

  private Object newBean() {
    if (constructor != null) {
      try {
        return constructor.invoke();
      } catch (Throwable e) {
        Platform.throwException(e);
      }
    }
    return Platform.newInstance(type);
  }
}
