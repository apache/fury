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

package org.apache.fury.reflect;

import static org.apache.fury.reflect.Types.newArrayType;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WildcardCapturer {
  private static final WildcardCapturer INSTANCE = new WildcardCapturer();

  static Type capture(Type type) {
    return INSTANCE.capture0(type);
  }

  final Type capture0(Type type) {
    if (type instanceof Class) {
      return type;
    }
    if (type instanceof TypeVariable) {
      return type;
    }
    if (type instanceof GenericArrayType) {
      GenericArrayType arrayType = (GenericArrayType) type;
      return newArrayType(capture0(arrayType.getGenericComponentType()));
    }
    if (type instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType) type;
      Class<?> rawType = (Class<?>) parameterizedType.getRawType();
      TypeVariable<?>[] typeVars = rawType.getTypeParameters();
      Type[] typeArgs = parameterizedType.getActualTypeArguments();
      for (int i = 0; i < typeArgs.length; i++) {
        typeArgs[i] = forTypeVariable(typeVars[i]).capture0(typeArgs[i]);
      }
      Type ownerType = parameterizedType.getOwnerType();
      return new Types.ParameterizedTypeImpl(
          ownerType == null ? null : capture0(ownerType), rawType, typeArgs);
    }
    if (type instanceof WildcardType) {
      WildcardType wildcardType = (WildcardType) type;
      Type[] lowerBounds = wildcardType.getLowerBounds();
      return lowerBounds.length == 0 // ? extends something changes to capture-of
          ? captureAsTypeVariable(wildcardType.getUpperBounds())
          : type;
    }
    throw new AssertionError("must have been one of the known types");
  }

  TypeVariable<?> captureAsTypeVariable(Type[] upperBounds) {
    String name =
        "capture of ? extends "
            + Stream.of(upperBounds).map(Type::toString).collect(Collectors.joining("&"));
    return new Types.TypeVariableImpl<>(WildcardCapturer.class, name, upperBounds);
  }

  private WildcardCapturer forTypeVariable(TypeVariable<?> typeParam) {
    return new WildcardCapturer() {
      @Override
      TypeVariable<?> captureAsTypeVariable(Type[] upperBounds) {
        // Since this is an artificially generated type variable, we don't bother checking
        // subtyping between declared type bound and actual type bound. So it's possible that we
        // may generate something like <capture#1-of ? extends Foo&SubFoo>.
        // Checking subtype between declared and actual type bounds
        // adds recursive isSubtypeOf() call and feels complicated.
        // There is no contract one way or another as long as isSubtypeOf() works as expected.
        Type[] typeParamBounds = typeParam.getBounds();
        Type[] combinedUpperBounds = upperBounds;
        if (typeParamBounds.length > 0) {
          int upperBoundsLength = upperBounds.length;
          combinedUpperBounds = new Type[typeParamBounds.length + upperBoundsLength];
          int i = 0;
          for (; i < upperBoundsLength; i++) {
            combinedUpperBounds[i] = upperBounds[i];
          }
          rootFor:
          for (; i < combinedUpperBounds.length; i++) {
            Type typeParamBound = typeParamBounds[i - upperBoundsLength];
            for (Type upperBound : upperBounds) {
              if (upperBound.equals(typeParamBound)) {
                continue rootFor;
              }
            }
            combinedUpperBounds[i] = typeParamBound;
          }
        }
        return super.captureAsTypeVariable(combinedUpperBounds);
      }
    };
  }
}
