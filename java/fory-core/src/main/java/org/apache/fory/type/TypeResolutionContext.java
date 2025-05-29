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

package org.apache.fory.type;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.fory.annotation.Internal;
import org.apache.fory.reflect.TypeRef;

@Internal
public class TypeResolutionContext {
  private final CustomTypeRegistry customTypeRegistry;
  private final LinkedHashSet<TypeRef<?>> walkedTypePath;
  private final Set<Class<?>> synthesizedBeanTypes;

  public TypeResolutionContext(CustomTypeRegistry customTypeRegistry) {
    this.customTypeRegistry = customTypeRegistry;
    walkedTypePath = new LinkedHashSet<>();
    synthesizedBeanTypes = Collections.emptySet();
  }

  public TypeResolutionContext(
      CustomTypeRegistry customTypeRegistry,
      LinkedHashSet<TypeRef<?>> walkedTypePath,
      Set<Class<?>> synthesizedBeanTypes) {
    this.customTypeRegistry = customTypeRegistry;
    this.walkedTypePath = walkedTypePath;
    this.synthesizedBeanTypes = synthesizedBeanTypes;
  }

  public CustomTypeRegistry getCustomTypeRegistry() {
    return customTypeRegistry;
  }

  public LinkedHashSet<TypeRef<?>> getWalkedTypePath() {
    return walkedTypePath;
  }

  public Set<Class<?>> getSynthesizedBeanTypes() {
    return synthesizedBeanTypes;
  }

  public TypeRef<?> getEnclosingType() {
    TypeRef<?> result = TypeRef.of(Object.class);
    for (TypeRef<?> type : walkedTypePath) { // .getLast()
      result = type;
    }
    return result;
  }

  public TypeResolutionContext appendTypePath(TypeRef<?>... typeRef) {
    LinkedHashSet<TypeRef<?>> newWalkedTypePath = new LinkedHashSet<>(walkedTypePath);
    newWalkedTypePath.addAll(Arrays.asList(typeRef));
    return new TypeResolutionContext(customTypeRegistry, newWalkedTypePath, synthesizedBeanTypes);
  }

  public TypeResolutionContext appendTypePath(Class<?> clz) {
    return appendTypePath(TypeRef.of(clz));
  }

  public TypeResolutionContext withSynthesizedBeanType(Class<?> clz) {
    Set<Class<?>> newSynthesizedBeanTypes = new HashSet<>(synthesizedBeanTypes);
    newSynthesizedBeanTypes.add(clz);
    return new TypeResolutionContext(customTypeRegistry, walkedTypePath, newSynthesizedBeanTypes);
  }

  public boolean isSynthesizedBeanType(Class<?> cls) {
    return synthesizedBeanTypes.contains(cls);
  }

  public void checkNoCycle(Class<?> clz) {
    checkNoCycle(TypeRef.of(clz));
  }

  public void checkNoCycle(TypeRef<?> typeRef) {
    if (walkedTypePath.contains(typeRef)
        || walkedTypePath.contains(TypeRef.of(typeRef.getRawType()))) {
      throw new UnsupportedOperationException(
          "cyclic type is not supported. walkedTypePath: "
              + walkedTypePath
              + " seen type: "
              + typeRef);
    }
  }
}
