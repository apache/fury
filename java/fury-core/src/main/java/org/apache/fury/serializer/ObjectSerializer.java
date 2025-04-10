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
import org.apache.fury.resolver.*;
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
    writeOtherFields(buffer, value);
    writeContainerFields(buffer, value, fury, refResolver, typeResolver);
  }

  private void writeOtherFields(MemoryBuffer buffer, T value) {
    for (GenericTypeField fieldInfo : otherFields) {
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      boolean nonNull = fieldInfo.furyFieldInfo.nonNull;
      Object fieldValue = fieldAccessor.getObject(value);
      if (fieldInfo.trackingRef) {
        binding.writeRef(buffer, fieldValue, fieldInfo.classInfoHolder);
      } else {
        writeNullable(binding, buffer, fieldValue, fieldInfo.classInfoHolder, nonNull);
      }
    }
  }

  private void writeFinalFields(
      MemoryBuffer buffer, T value, Fury fury, RefResolver refResolver, TypeResolver typeResolver) {
    FinalTypeField[] finalFields = this.finalFields;
    boolean metaShareEnabled = fury.getConfig().isMetaShareEnabled();
    for (int i = 0; i < finalFields.length; i++) {
      FinalTypeField fieldInfo = finalFields[i];
      FieldAccessor fieldAccessor = fieldInfo.fieldAccessor;
      FuryFieldInfo furyFieldInfo = fieldInfo.furyFieldInfo;
      boolean nonNull = furyFieldInfo.nonNull;
      short classId = fieldInfo.classId;
      if (writePrimitiveFieldValueFailed(fury, buffer, value, fieldAccessor, classId)) {
        Object fieldValue = fieldAccessor.getObject(value);
        if (writeBasicObjectFieldValueFailed(fury, buffer, fieldValue, classId, furyFieldInfo)) {
          Serializer<Object> serializer = fieldInfo.classInfo.getSerializer();
          if (!metaShareEnabled || isFinal[i]) {
            if (!fieldInfo.trackingRef) {
              writeNullable(binding, buffer, fieldValue, serializer, nonNull);
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
              writeNullable(binding, buffer, fieldValue, serializer, nonNull);
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
      boolean nonNull = fieldInfo.furyFieldInfo.nonNull;
      if (!nonNull) {
        if (fieldValue == null) {
          buffer.writeByte(Fury.NULL_FLAG);
          return;
        } else {
          buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        }
      }
      generics.pushGenericType(fieldInfo.genericType);
      binding.writeNonRef(
          buffer,
          fieldValue,
          typeResolver.getClassInfo(fieldValue.getClass(), fieldInfo.classInfoHolder));
      generics.popGenericType();
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
      FuryFieldInfo furyFieldInfo = fieldInfo.furyFieldInfo;
      short classId = fieldInfo.classId;
      if (readPrimitiveFieldValueFailed(fury, buffer, obj, fieldAccessor, classId)
          && readBasicObjectFieldValueFailed(
              fury, buffer, obj, fieldAccessor, classId, furyFieldInfo)) {
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
