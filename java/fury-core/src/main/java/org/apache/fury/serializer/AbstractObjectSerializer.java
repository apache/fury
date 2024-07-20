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

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.List;
import org.apache.fury.Fury;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.collection.Tuple3;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.FieldAccessor;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.reflect.TypeRef;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.FieldResolver.FieldInfo;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.DescriptorGrouper;
import org.apache.fury.type.FinalObjectTypeStub;
import org.apache.fury.type.GenericType;
import org.apache.fury.util.record.RecordUtils;

import static org.apache.fury.type.TypeUtils.getRawType;

public abstract class AbstractObjectSerializer<T> extends Serializer<T> {

  protected final RefResolver refResolver;
  protected final ClassResolver classResolver;
  protected final boolean isRecord;
  protected final MethodHandle constructor;

  public AbstractObjectSerializer(Fury fury, Class<T> type) {
    super(fury, type);
    this.refResolver = fury.getRefResolver();
    this.classResolver = fury.getClassResolver();
    this.isRecord = RecordUtils.isRecord(type);
    if (isRecord) {
      this.constructor = RecordUtils.getRecordConstructor(type).f1;
    } else {
      this.constructor = ReflectionUtils.getCtrHandle(type, false);
    }
  }

  public AbstractObjectSerializer(Fury fury, Class<T> type, MethodHandle constructor) {
    super(fury, type);
    this.refResolver = fury.getRefResolver();
    this.classResolver = fury.getClassResolver();
    this.isRecord = RecordUtils.isRecord(type);
    this.constructor = constructor;
  }

  @Override
  public T copy(T originObj) {
    if (immutable) {
      return originObj;
    }
    if (isRecord) {
      Object[] fieldValues = copyFields(originObj);
      try {
        return (T) constructor.invokeWithArguments(fieldValues);
      } catch (Throwable e) {
        Platform.throwException(e);
      }
      return originObj;
    }
    T newObj;
    if (needToCopyRef) {
      T copyObject = (T) fury.getCopyObject(originObj);
      if (copyObject != null) {
        return copyObject;
      }
      newObj = newBean();
      fury.reference(originObj, newObj);
    } else {
      newObj = newBean();
    }
    copyFields(originObj, newObj);
    return newObj;
  }

  private Object[] copyFields(T originObj) {
    return classResolver.getFieldResolver(type).getAllFieldsList().stream()
        .map(
            fieldInfo -> {
              FieldAccessor fieldAccessor = fieldInfo.getFieldAccessor();
              if (classResolver.isPrimitive(fieldInfo.getEmbeddedClassId())) {
                return fieldAccessor.get(originObj);
              }
              return fury.copyObject(fieldAccessor.get(originObj));
            })
        .toArray();
  }

  private void copyFields(T originObj, T newObj) {
    List<FieldInfo> fieldsList = classResolver.getFieldResolver(type).getAllFieldsList();
    for (FieldInfo info : fieldsList) {
      FieldAccessor fieldAccessor = info.getFieldAccessor();
      long offset = fieldAccessor.getFieldOffset();
      switch (info.getEmbeddedClassId()) {
        case ClassResolver.PRIMITIVE_BYTE_CLASS_ID:
          Platform.putByte(newObj, offset, Platform.getByte(originObj, offset));
          break;
        case ClassResolver.PRIMITIVE_CHAR_CLASS_ID:
          Platform.putChar(newObj, offset, Platform.getChar(originObj, offset));
          break;
        case ClassResolver.PRIMITIVE_SHORT_CLASS_ID:
          Platform.putShort(newObj, offset, Platform.getShort(originObj, offset));
          break;
        case ClassResolver.PRIMITIVE_INT_CLASS_ID:
          Platform.putInt(newObj, offset, Platform.getInt(originObj, offset));
          break;
        case ClassResolver.PRIMITIVE_LONG_CLASS_ID:
          Platform.putLong(newObj, offset, Platform.getLong(originObj, offset));
          break;
        case ClassResolver.PRIMITIVE_FLOAT_CLASS_ID:
          Platform.putFloat(newObj, offset, Platform.getFloat(originObj, offset));
          break;
        case ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID:
          Platform.putDouble(newObj, offset, Platform.getDouble(originObj, offset));
          break;
        case ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID:
          Platform.putBoolean(newObj, offset, Platform.getBoolean(originObj, offset));
          break;
        default:
          Platform.putObject(
              newObj, offset, fury.copyObject(Platform.getObject(originObj, offset)));
      }
    }
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
      d.getTypeRef(),
      d.getDeclaringClass() + "." + d.getName(),
      d.getField() != null ? FieldAccessor.createAccessor(d.getField()) : null,
      fury);
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
    final GenericType genericType;
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
      TypeRef<?> typeRef, String qualifiedFieldName, FieldAccessor accessor, Fury fury) {
      super(getRegisteredClassId(fury, getRawType(typeRef)), qualifiedFieldName, accessor);
      // TODO support generics <T> in Pojo<T>, see ComplexObjectSerializer.getGenericTypes
      genericType = fury.getClassResolver().buildGenericType(typeRef);
      classInfoHolder = fury.getClassResolver().nilClassInfoHolder();
      trackingRef = fury.getClassResolver().needToWriteRef(getRawType(typeRef));
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
