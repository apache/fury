package org.apache.fury.serializer;

import java.lang.invoke.MethodHandle;
import java.util.List;
import org.apache.fury.Fury;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.FieldAccessor;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.FieldResolver.FieldInfo;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.util.record.RecordUtils;

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
    T newObj = newBean();
    if (needToCopyRef) {
      T copyObject = (T) fury.getCopyObject(originObj);
      if (copyObject != null) {
        return copyObject;
      }
      fury.reference(originObj, newObj);
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
              return fury.copy(fieldAccessor.get(originObj));
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
          Platform.putObject(newObj, offset, fury.copy(Platform.getObject(originObj, offset)));
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
}
