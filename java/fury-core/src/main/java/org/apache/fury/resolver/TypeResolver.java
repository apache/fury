package org.apache.fury.resolver;

import org.apache.fury.annotation.Internal;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.reflect.TypeRef;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.type.GenericType;

import java.lang.reflect.Type;

// Internal type dispatcher.
// Do not use this interface outside of fury package
@Internal
public interface TypeResolver {
  boolean needToWriteRef(TypeRef<?> typeRef);

  ClassInfo getClassInfo(Class<?> cls, ClassInfoHolder classInfoHolder);

  void writeClassInfo(MemoryBuffer buffer, ClassInfo classInfo);

  ClassInfo readClassInfo(MemoryBuffer buffer, ClassInfoHolder classInfoHolder);

  <T> Serializer<T> getSerializer(Class<T> cls);

  ClassInfo nilClassInfo();

  ClassInfoHolder nilClassInfoHolder();

  GenericType buildGenericType(TypeRef<?> typeRef);

  GenericType buildGenericType(Type type);

  void initialize();
}
