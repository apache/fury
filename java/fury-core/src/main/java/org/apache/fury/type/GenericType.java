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

package org.apache.fury.type;

import static org.apache.fury.type.TypeUtils.getRawType;

import com.google.common.reflect.TypeToken;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.util.ReflectionUtils;

/** GenericType for building java generics as a tree and binding with fury serializers. */
// TODO(chaokunyang) refine generics which can be inspired by spring ResolvableType.
public class GenericType {
  private static final Predicate<Type> defaultFinalPredicate =
      type -> {
        if (type.getClass() == Class.class) {
          return ReflectionUtils.isMonomorphic(((Class<?>) type));
        } else {
          return ReflectionUtils.isMonomorphic((getRawType(type)));
        }
      };

  final TypeToken<?> typeToken;
  final Class<?> cls;
  final GenericType[] typeParameters;
  final int typeParametersCount;
  final GenericType typeParameter0;
  final GenericType typeParameter1;
  final boolean hasGenericParameters;
  final boolean isMonomorphic;
  // Used to cache serializer for final class to avoid hash lookup for serializer.
  Serializer<?> serializer;
  private Boolean trackingRef;

  public GenericType(TypeToken<?> typeToken, boolean isMonomorphic, GenericType... typeParameters) {
    this.typeToken = typeToken;
    this.cls = getRawType(typeToken);
    this.typeParameters = typeParameters;
    typeParametersCount = typeParameters.length;
    hasGenericParameters = typeParameters.length > 0;
    this.isMonomorphic = isMonomorphic;
    if (typeParameters.length > 0) {
      typeParameter0 = typeParameters[0];
    } else {
      typeParameter0 = null;
    }
    if (typeParameters.length > 1) {
      typeParameter1 = typeParameters[1];
    } else {
      typeParameter1 = null;
    }
  }

  public static GenericType build(TypeToken<?> type) {
    return build(type.getType());
  }

  public static GenericType build(Type type) {
    return build(type, defaultFinalPredicate);
  }

  /**
   * Build generics based on an {@code context} which capture generic type information.
   *
   * <pre>{@code
   * class A<T> {
   *   List<String> f1;
   *   List<T> f2;
   *   List<T>[] f3;
   *   T[] f4;
   *   SomeClass<T> f5;
   *   Map<T, List<SomeClass<T>>> f6;
   *   Map f7;
   * }
   *
   * class B extends A<Long> {}
   * }</pre>
   */
  public static GenericType build(TypeToken<?> context, Type type) {
    return build(context, type, defaultFinalPredicate);
  }

  public static GenericType build(Class<?> context, Type type) {
    return build(context, type, defaultFinalPredicate);
  }

  public static GenericType build(Class<?> context, Type type, Predicate<Type> finalPredicate) {
    return build(TypeToken.of(context), type, finalPredicate);
  }

  public static GenericType build(TypeToken<?> context, Type type, Predicate<Type> finalPredicate) {
    return build(context.resolveType(type).getType(), finalPredicate);
  }

  @SuppressWarnings("rawtypes")
  public static GenericType build(Type type, Predicate<Type> finalPredicate) {
    if (type instanceof ParameterizedType) {
      // List<String>, List<T>, Map<String, List<String>>, SomeClass<T>
      Type[] actualTypeArguments = ((ParameterizedType) type).getActualTypeArguments();
      List<GenericType> list = new ArrayList<>();
      for (Type t : actualTypeArguments) {
        GenericType build = GenericType.build(t, finalPredicate);
        list.add(build);
      }
      GenericType[] genericTypes = list.toArray(new GenericType[0]);
      return new GenericType(TypeToken.of(type), finalPredicate.test(type), genericTypes);
    } else if (type instanceof GenericArrayType) { // List<String>[] or T[]
      Type componentType = ((GenericArrayType) type).getGenericComponentType();
      return new GenericType(TypeToken.of(type), finalPredicate.test(type), build(componentType));
    } else if (type instanceof TypeVariable) { // T
      TypeVariable typeVariable = (TypeVariable) type;
      Type typeVariableBound =
          typeVariable.getBounds()[0]; // Bound 0 are a class, other bounds are interface.
      return new GenericType(TypeToken.of(typeVariableBound), finalPredicate.test(type));
    } else if (type instanceof WildcardType) {
      // WildcardType: `T extends Number`, not a type, just an expression.
      // `? extends java.util.Collection<? extends java.util.Collection<java.lang.Integer>>`
      Type upperBound = ((WildcardType) type).getUpperBounds()[0];
      if (upperBound instanceof ParameterizedType) {
        return build(upperBound);
      } else {
        return new GenericType(TypeToken.of(upperBound), finalPredicate.test(type));
      }
    } else {
      // Class type: String, Integer
      return new GenericType(TypeToken.of(type), finalPredicate.test(type));
    }
  }

  public TypeToken<?> getTypeToken() {
    return typeToken;
  }

  public Class<?> getCls() {
    return cls;
  }

  public GenericType[] getTypeParameters() {
    return typeParameters;
  }

  public int getTypeParametersCount() {
    return typeParametersCount;
  }

  public GenericType getTypeParameter0() {
    return typeParameter0;
  }

  public GenericType getTypeParameter1() {
    return typeParameter1;
  }

  public Serializer<?> getSerializer(ClassResolver classResolver) {
    Serializer<?> serializer = this.serializer;
    if (serializer == null) {
      serializer = classResolver.getSerializer(cls);
      this.serializer = serializer;
    }
    return serializer;
  }

  public boolean isMonomorphic() {
    return isMonomorphic;
  }

  public Serializer<?> getSerializerOrNull(ClassResolver classResolver) {
    if (isMonomorphic) {
      return getSerializer(classResolver);
    } else {
      return null;
    }
  }

  public boolean trackingRef(ClassResolver classResolver) {
    Boolean trackingRef = this.trackingRef;
    if (trackingRef == null) {
      trackingRef = this.trackingRef = classResolver.needToWriteRef(cls);
    }
    return trackingRef;
  }

  public boolean hasGenericParameters() {
    return hasGenericParameters;
  }

  @Override
  public String toString() {
    return "GenericType{" + typeToken.toString() + '}';
  }

  public static boolean isFinalByDefault(Class<?> cls) {
    return defaultFinalPredicate.test(cls);
  }
}
