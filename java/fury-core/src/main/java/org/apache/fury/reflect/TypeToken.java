/*
 * Copyright (C) 2006 The Guava Authors
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

import com.google.common.reflect.TypeParameter;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.fury.type.TypeUtils;

public class TypeToken<T> {

  private final Type type;

  /**
   * Constructs a new type token of {@code T}.
   *
   * <p>Clients create an empty anonymous subclass. This embeds the type parameter in the anonymous
   * class's type hierarchy, so we can reconstitute it at runtime despite erasure.
   *
   * <p>For example:
   *
   * <pre>{@code
   * TypeToken<List<String>> t = new TypeToken<List<String>>() {};
   * }</pre>
   */
  protected TypeToken() {
    this.type = capture();
  }

  private TypeToken(Class<T> declaringClass) {
    this.type = declaringClass;
  }

  private TypeToken(Type type) {
    this.type = type;
  }

  /** Returns an instance of type token that wraps {@code type}. */
  public static <T> TypeToken<T> of(Class<T> clazz) {
    return new TypeToken<T>(clazz);
  }

  /** Returns an instance of type token that wraps {@code type}. */
  public static <T> TypeToken<T> of(Type type) {
    return new TypeToken<T>(type);
  }

  /** Returns the captured type. */
  private Type capture() {
    final Type superclass = getClass().getGenericSuperclass();
    if (!(superclass instanceof ParameterizedType)) {
      throw new IllegalArgumentException(superclass + " isn't parameterized");
    }
    return ((ParameterizedType) superclass).getActualTypeArguments()[0];
  }

  /** Returns the represented type. */
  public Type getType() {
    return type;
  }

  /**
   * Returns the raw type of {@code T}. Formally speaking, if {@code T} is returned by {@link
   * java.lang.reflect.Method#getGenericReturnType}, the raw type is what's returned by {@link
   * java.lang.reflect.Method#getReturnType} of the same method object. Specifically:
   *
   * <ul>
   *   <li>If {@code T} is a {@code Class} itself, {@code T} itself is returned.
   *   <li>If {@code T} is a {@link ParameterizedType}, the raw type of the parameterized type is
   *       returned.
   *   <li>If {@code T} is a {@link GenericArrayType}, the returned type is the corresponding array
   *       class. For example: {@code List<Integer>[] => List[]}.
   *   <li>If {@code T} is a type variable or a wildcard type, the raw type of the first upper bound
   *       is returned. For example: {@code <X extends Foo> => Foo}.
   * </ul>
   */
  public Class<? super T> getRawType() {
    // For wildcard or type variable, the first bound determines the runtime type.
    Class<?> rawType = getRawTypes(type).iterator().next();
    @SuppressWarnings("unchecked") // raw type is |T|
    Class<? super T> result = (Class<? super T>) rawType;
    return result;
  }

  private static Set<Class<?>> getRawTypes(Type... types) {
    Set<Class<?>> set = new HashSet<>();
    for (Type type : types) {
      if (type instanceof TypeVariable) {
        return getRawTypes(((TypeVariable<?>) type).getBounds());
      } else if (type instanceof WildcardType) {
        return getRawTypes(((WildcardType) type).getUpperBounds());
      } else if (type instanceof ParameterizedType) {
        set.add((Class<?>) ((ParameterizedType) type).getRawType());
      } else if (type instanceof Class) {
        set.add((Class<?>) type);
      } else if (type instanceof GenericArrayType) {
        set.add(
            getArrayClass(of(((GenericArrayType) type).getGenericComponentType()).getRawType()));
      } else {
        throw new AssertionError("Unknown type: " + type);
      }
    }
    return Collections.unmodifiableSet(set);
  }

  /** Returns true if this type is one of the primitive types (including {@code void}). */
  public boolean isPrimitive() {
    return type instanceof Class && ((Class<?>) type).isPrimitive();
  }

  /**
   * Returns true if this type is known to be an array type, such as {@code int[]}, {@code T[]},
   * {@code <? extends Map<String, Integer>[]>} etc.
   */
  public boolean isArray() {
    return getComponentType(type) != null;
  }

  /**
   * Returns the array component type if this type represents an array ({@code int[]}, {@code T[]},
   * {@code <? extends Map<String, Integer>[]>} etc.), or else {@code null} is returned.
   */
  public TypeToken<?> getComponentType() {
    return of(getComponentType(type));
  }

  /**
   * Returns the array component type if this type represents an array ({@code int[]}, {@code T[]},
   * {@code <? extends Map<String, Integer>[]>} etc.), or else {@code null} is returned.
   */
  private static Type getComponentType(Type type) {
    if (type == null) {
      return null;
    }
    if (type instanceof TypeVariable) {
      return subtypeOfComponentType(((TypeVariable<?>) type).getBounds());
    } else if (type instanceof WildcardType) {
      return subtypeOfComponentType(((WildcardType) type).getUpperBounds());
    } else if (type instanceof Class) {
      return ((Class<?>) type).getComponentType();
    } else if (type instanceof GenericArrayType) {
      return ((GenericArrayType) type).getGenericComponentType();
    }
    return null;
  }

  /**
   * Returns {@code ? extends X} if any of {@code bounds} is a subtype of {@code X[]}; or null
   * otherwise.
   */
  private static Type subtypeOfComponentType(Type[] bounds) {
    for (Type bound : bounds) {
      final Type componentType = getComponentType(bound);
      if (componentType != null) {
        // Only the first bound can be a class or array.
        // Bounds after the first can only be interfaces.
        if (componentType instanceof Class) {
          final Class<?> componentClass = (Class<?>) componentType;
          if (componentClass.isPrimitive()) {
            return componentClass;
          }
        }
        return componentType;
      }
    }
    return null;
  }

  /**
   * Do not use this method. This is used only for code refactoring during Guava dependency removal.
   */
  public com.google.common.reflect.TypeToken<?> getGuavaTypeToken() {
    return com.google.common.reflect.TypeToken.of(type);
  }

  /**
   * Resolves the given {@code type} against the type context represented by this type. For example:
   *
   * <pre>{@code
   * new TypeToken<List<String>>() {}.resolveType(
   *     List.class.getMethod("get", int.class).getGenericReturnType())
   * => String.class
   * }</pre>
   */
  public TypeToken<?> resolveType(Type iteratorReturnType) {
    // TODO: Remove guava dependency.
    return TypeToken.of(getGuavaTypeToken().resolveType(iteratorReturnType).getType());
  }

  /**
   * Returns the generic form of {@code superclass}. For example, if this is {@code
   * ArrayList<String>}, {@code Iterable<String>} is returned given the input {@code
   * Iterable.class}.
   */
  public TypeToken<? super T> getSupertype(Class<? super T> superclass) {
    // TODO: Remove guava dependency.
    return TypeToken.of(
        ((com.google.common.reflect.TypeToken<? extends T>) getGuavaTypeToken())
            .getSupertype(superclass)
            .getType());
  }

  /**
   * Returns subtype of {@code this} with {@code subclass} as the raw class. For example, if this is
   * {@code Iterable<String>} and {@code subclass} is {@code List}, {@code List<String>} is
   * returned.
   */
  public final TypeToken<? extends T> getSubtype(Class<?> subclass) {
    // TODO: Remove guava dependency.
    return of(getGuavaTypeToken().getSubtype(subclass).getType());
  }

  /** Returns true if this type is a subtype of the given {@code type}. */
  public boolean isSubtypeOf(TypeToken<?> type) {
    return isSubtypeOf(type.getType());
  }

  /** Returns true if this type is a subtype of the given {@code type}. */
  public final boolean isSubtypeOf(Type supertype) {
    // TODO: Remove guava dependency.
    return getGuavaTypeToken().isSubtypeOf(supertype);
  }

  /** Returns true if this type is a supertype of the given {@code type}. */
  public final boolean isSupertypeOf(Type type) {
    // TODO: Remove guava dependency.
    return of(type).getGuavaTypeToken().isSubtypeOf(getType());
  }

  /** Returns true if this type is a supertype of the given {@code type}. */
  public final boolean isSupertypeOf(TypeToken<?> type) {
    // TODO: Remove guava dependency.
    return type.getGuavaTypeToken().isSubtypeOf(getType());
  }

  /**
   * Returns the corresponding wrapper type if this is a primitive type; otherwise returns {@code
   * this} itself.
   */
  public final TypeToken<T> wrap() {
    if (isPrimitive()) {
      @SuppressWarnings("unchecked")
      final Class<T> clazz = (Class<T>) type;
      // cast is safe: long.class and Long.class are both of type Class<Long>
      @SuppressWarnings("unchecked")
      final Class<T> wrapped = (Class<T>) TypeUtils.wrap(clazz);
      return of(wrapped);
    }
    return this;
  }

  /**
   * Returns the corresponding primitive type if this is a wrapper type; otherwise returns {@code
   * this} itself.
   */
  public final TypeToken<T> unwrap() {
    if (isWrapper()) {
      @SuppressWarnings("unchecked") // this is a wrapper class
      final Class<T> clazz = (Class<T>) type;
      // cast is safe: long.class and Long.class are both of type Class<Long>
      @SuppressWarnings("unchecked")
      final Class<T> unwrapped = (Class<T>) TypeUtils.unwrap(clazz);
      return of(unwrapped);
    }
    return this;
  }

  public <X> TypeToken<T> where(TypeParameter<X> typeParam, Class<X> typeArg) {
    return where(typeParam, of(typeArg));
  }

  public final <X> TypeToken<T> where(TypeParameter<X> typeParam, TypeToken<X> typeArg) {
    // TODO: Remove guava dependency.
    return of(
        getGuavaTypeToken()
            .where(
                typeParam,
                (com.google.common.reflect.TypeToken<X>)
                    com.google.common.reflect.TypeToken.of(typeArg.getType()))
            .getType());
  }

  private boolean isWrapper() {
    if (type instanceof Class<?>) {
      return TypeUtils.isBoxed((Class<?>) type);
    }
    return false;
  }

  /**
   * Returns true if {@code o} is another {@code TypeToken} that represents the same {@link Type}.
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof TypeToken) {
      final TypeToken<?> that = (TypeToken<?>) o;
      return type.equals(that.type);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return type.hashCode();
  }

  @Override
  public String toString() {
    return (type instanceof Class) ? ((Class<?>) type).getName() : type.toString();
  }

  /** Returns the {@code Class} object of arrays with {@code componentType}. */
  private static Class<?> getArrayClass(Class<?> componentType) {
    return Array.newInstance(componentType, 0).getClass();
  }
}
