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

package org.apache.fury.resolver;

import java.lang.reflect.Type;
import org.apache.fury.annotation.Internal;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.reflect.TypeRef;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.type.GenericType;

// Internal type dispatcher.
// Do not use this interface outside of fury package
@Internal
public interface TypeResolver {
  boolean needToWriteRef(TypeRef<?> typeRef);

  ClassInfo getClassInfo(Class<?> cls, ClassInfoHolder classInfoHolder);

  void writeClassInfo(MemoryBuffer buffer, ClassInfo classInfo);

  ClassInfo readClassInfo(MemoryBuffer buffer, ClassInfoHolder classInfoHolder);

  ClassInfo readClassInfo(MemoryBuffer buffer, ClassInfo classInfoCache);

  <T> Serializer<T> getSerializer(Class<T> cls);

  ClassInfo nilClassInfo();

  ClassInfoHolder nilClassInfoHolder();

  GenericType buildGenericType(TypeRef<?> typeRef);

  GenericType buildGenericType(Type type);

  void initialize();
}
