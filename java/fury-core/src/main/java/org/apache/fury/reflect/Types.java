/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fury.reflect;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.GenericDeclaration;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.CheckForNull;

// Derived from Guava 32.1.2 com.google.common.reflect.Types
// https://github.com/google/guava/blob/9f6a3840/guava/src/com/google/common/reflect/Types.java
class Types {
  public static Type newArrayType(Type componentType) {
    if (componentType instanceof WildcardType) {
      WildcardType wildcard = (WildcardType) componentType;
      Type[] lowerBounds = wildcard.getLowerBounds();
      if (lowerBounds.length == 1) {
        return new WildcardTypeImpl(new Type[] {lowerBounds[0]}, new Type[] {Object.class});
      } else {
        Type[] upperBounds = wildcard.getUpperBounds();
        return new WildcardTypeImpl(new Type[0], new Type[] {upperBounds[0]});
      }
    }
    return componentType instanceof Class
        ? Array.newInstance((Class<?>) componentType, 0).getClass()
        : new GenericArrayTypeImpl(componentType);
  }

  public static boolean typeVariablesEquals(TypeVariable<?> a, Object bobj) {
    if (bobj instanceof TypeVariable) {
      TypeVariable<?> b = (TypeVariable<?>) bobj;
      return a.getGenericDeclaration().equals(b.getGenericDeclaration())
          && a.getName().equals(b.getName());
    } else {
      return false;
    }
  }

  public static TypeVariableKey asTypeVariableKeyOrNull(Type t) {
    if (t instanceof TypeVariable<?>) {
      return new TypeVariableKey((TypeVariable<?>) t);
    }
    return null;
  }

  private static Type getOwnerTypeFromRawType(Class<?> rawType) {
    return ClassOwnership.JVM_BEHAVIOR.getOwnerType(rawType);
  }

  private static String resolveTypeName(Type type) {
    return type instanceof Class ? ((Class<?>) type).getName() : type.toString();
  }

  public static class ParameterizedTypeImpl implements ParameterizedType {
    private final Type[] actualTypeArguments;
    private final Type rawType;
    private final Type ownerType;

    public ParameterizedTypeImpl(Type ownerType, Type rawType, Type[] actualTypeArguments) {
      if (ownerType == null) {
        this.ownerType = getOwnerTypeFromRawType((Class<?>) rawType);
      } else {
        this.ownerType = ownerType;
      }
      this.rawType = rawType;
      this.actualTypeArguments = actualTypeArguments;
    }

    @Override
    public Type getOwnerType() {
      return ownerType;
    }

    @Override
    public Type getRawType() {
      return rawType;
    }

    @Override
    public Type[] getActualTypeArguments() {
      return actualTypeArguments;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ParameterizedType)) {
        return false;
      }

      ParameterizedType that = (ParameterizedType) o;

      if (!Arrays.equals(actualTypeArguments, that.getActualTypeArguments())) {
        return false;
      }
      if (!Objects.equals(rawType, that.getRawType())) {
        return false;
      }
      return Objects.equals(ownerType, that.getOwnerType());
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(actualTypeArguments);
      result = 31 * result + (rawType != null ? rawType.hashCode() : 0);
      result = 31 * result + (ownerType != null ? ownerType.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append(typeName(rawType)).append('<');
      int i = 0;
      for (Type typeArgument : actualTypeArguments) {
        if (i++ != 0) {
          builder.append(", ");
        }
        builder.append(typeName(typeArgument));
      }
      return builder.append('>').toString();
    }
  }

  static String typeName(Type type) {
    return (type instanceof Class) ? ((Class<?>) type).getName() : type.toString();
  }

  public static class GenericArrayTypeImpl implements GenericArrayType {
    private final Type genericComponentType;

    public GenericArrayTypeImpl(Type genericComponentType) {
      this.genericComponentType = genericComponentType;
    }

    @Override
    public Type getGenericComponentType() {
      return genericComponentType;
    }

    @Override
    public String toString() {
      return resolveTypeName(genericComponentType) + "[]";
    }

    @Override
    public int hashCode() {
      return genericComponentType.hashCode();
    }

    @Override
    public boolean equals(@CheckForNull Object obj) {
      if (obj instanceof GenericArrayType) {
        GenericArrayType that = (GenericArrayType) obj;
        return Objects.equals(getGenericComponentType(), that.getGenericComponentType());
      }
      return false;
    }
  }

  public static class WildcardTypeImpl implements WildcardType {
    private final Type[] upperBounds;
    private final Type[] lowerBounds;

    public WildcardTypeImpl(Type[] upperBounds, Type[] lowerBounds) {
      this.upperBounds = upperBounds;
      this.lowerBounds = lowerBounds;
    }

    @Override
    public Type[] getUpperBounds() {
      return upperBounds;
    }

    @Override
    public Type[] getLowerBounds() {
      return lowerBounds;
    }

    @Override
    public boolean equals(@CheckForNull Object obj) {
      if (obj instanceof WildcardType) {
        WildcardType that = (WildcardType) obj;
        return Arrays.equals(lowerBounds, that.getLowerBounds())
            && Arrays.equals(upperBounds, that.getUpperBounds());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(lowerBounds) ^ Arrays.hashCode(upperBounds);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder("?");
      for (Type lowerBound : lowerBounds) {
        builder.append(" super ").append(resolveTypeName(lowerBound));
      }
      for (Type upperBound : upperBounds) {
        if (!upperBound.equals(Object.class)) {
          builder.append(" extends ").append(resolveTypeName(upperBound));
        }
      }
      return builder.toString();
    }
  }

  public static class TypeVariableImpl<D extends GenericDeclaration> implements TypeVariable<D> {
    private final D genericDeclaration;
    private final String name;
    private final Type[] upperBounds;

    TypeVariableImpl(D genericDeclaration, String name, Type[] upperBounds) {
      this.genericDeclaration = genericDeclaration;
      this.name = name;
      this.upperBounds = upperBounds;
    }

    @Override
    public Type[] getBounds() {
      return upperBounds;
    }

    @Override
    public D getGenericDeclaration() {
      return genericDeclaration;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public AnnotatedType[] getAnnotatedBounds() {
      return new AnnotatedType[0];
    }

    @Override
    public <T extends Annotation> T getAnnotation(Class<T> annotationClass) {
      return null;
    }

    @Override
    public Annotation[] getAnnotations() {
      return new Annotation[0];
    }

    @Override
    public Annotation[] getDeclaredAnnotations() {
      return new Annotation[0];
    }

    @Override
    public String toString() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TypeVariable)) {
        return false;
      }

      TypeVariable<?> that = (TypeVariable<?>) o;
      return Objects.equals(genericDeclaration, that.getGenericDeclaration())
          && Objects.equals(name, that.getName())
          && Arrays.equals(upperBounds, that.getBounds());
    }

    @Override
    public int hashCode() {
      int result = genericDeclaration != null ? genericDeclaration.hashCode() : 0;
      result = 31 * result + (name != null ? name.hashCode() : 0);
      result = 31 * result + Arrays.hashCode(upperBounds);
      return result;
    }
  }

  static class TypeVariableKey {
    private final TypeVariable<?> typeVariable;

    public TypeVariableKey(TypeVariable<?> typeVariable) {
      this.typeVariable = typeVariable;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof TypeVariableKey
          && Types.typeVariablesEquals(typeVariable, ((TypeVariableKey) o).typeVariable);
    }

    @Override
    public int hashCode() {
      Annotation[] declaredAnnotations = typeVariable.getDeclaredAnnotations();
      String name = typeVariable.getName();

      int result = 1;
      result =
          31 * result + (declaredAnnotations != null ? Arrays.hashCode(declaredAnnotations) : 0);
      result = 31 * result + (name != null ? name.hashCode() : 0);
      return result;
    }
  }

  private enum ClassOwnership {
    OWNED_BY_ENCLOSING_CLASS {
      @Override
      Class<?> getOwnerType(Class<?> rawType) {
        return rawType.getEnclosingClass();
      }
    },
    LOCAL_CLASS_HAS_NO_OWNER {
      @Override
      Class<?> getOwnerType(Class<?> rawType) {
        return rawType.isLocalClass() ? null : rawType.getEnclosingClass();
      }
    };

    abstract Class<?> getOwnerType(Class<?> rawType);

    static final ClassOwnership JVM_BEHAVIOR = detectJvmBehaviour();

    private static ClassOwnership detectJvmBehaviour() {
      class LocalClass<T> {}

      LocalClass<String> localClassInstance = new LocalClass<String>() {};
      Class<?> subclass = localClassInstance.getClass();
      ParameterizedType parameterizedType = (ParameterizedType) subclass.getGenericSuperclass();
      for (ClassOwnership behavior : ClassOwnership.values()) {
        if (behavior.getOwnerType(LocalClass.class) == parameterizedType.getOwnerType()) {
          return behavior;
        }
      }
      throw new AssertionError();
    }
  }
}
