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

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.fury.collection.IdentityMap;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;

/** Type utils for common type inference and extraction. */
@SuppressWarnings({"UnstableApiUsage", "unchecked"})
public class TypeUtils {
  public static final String JAVA_BOOLEAN = "boolean";
  public static final String JAVA_BYTE = "byte";
  public static final String JAVA_SHORT = "short";
  public static final String JAVA_INT = "int";
  public static final String JAVA_LONG = "long";
  public static final String JAVA_FLOAT = "float";
  public static final String JAVA_DOUBLE = "double";
  public static final TypeToken<?> PRIMITIVE_VOID_TYPE = TypeToken.of(void.class);
  public static final TypeToken<?> VOID_TYPE = TypeToken.of(Void.class);
  public static final TypeToken<?> PRIMITIVE_BYTE_TYPE = TypeToken.of(byte.class);
  public static final TypeToken<?> PRIMITIVE_BOOLEAN_TYPE = TypeToken.of(boolean.class);
  public static final TypeToken<?> PRIMITIVE_CHAR_TYPE = TypeToken.of(char.class);
  public static final TypeToken<?> PRIMITIVE_SHORT_TYPE = TypeToken.of(short.class);
  public static final TypeToken<?> PRIMITIVE_INT_TYPE = TypeToken.of(int.class);
  public static final TypeToken<?> PRIMITIVE_LONG_TYPE = TypeToken.of(long.class);
  public static final TypeToken<?> PRIMITIVE_FLOAT_TYPE = TypeToken.of(float.class);
  public static final TypeToken<?> PRIMITIVE_DOUBLE_TYPE = TypeToken.of(double.class);
  public static final TypeToken<?> BYTE_TYPE = TypeToken.of(Byte.class);
  public static final TypeToken<?> BOOLEAN_TYPE = TypeToken.of(Boolean.class);
  public static final TypeToken<?> CHAR_TYPE = TypeToken.of(Character.class);
  public static final TypeToken<?> SHORT_TYPE = TypeToken.of(Short.class);
  public static final TypeToken<?> INT_TYPE = TypeToken.of(Integer.class);
  public static final TypeToken<?> LONG_TYPE = TypeToken.of(Long.class);
  public static final TypeToken<?> FLOAT_TYPE = TypeToken.of(Float.class);
  public static final TypeToken<?> DOUBLE_TYPE = TypeToken.of(Double.class);
  public static final TypeToken<?> STRING_TYPE = TypeToken.of(String.class);
  public static final TypeToken<?> BIG_DECIMAL_TYPE = TypeToken.of(BigDecimal.class);
  public static final TypeToken<?> BIG_INTEGER_TYPE = TypeToken.of(BigInteger.class);
  public static final TypeToken<?> DATE_TYPE = TypeToken.of(Date.class);
  public static final TypeToken<?> LOCAL_DATE_TYPE = TypeToken.of(LocalDate.class);
  public static final TypeToken<?> TIMESTAMP_TYPE = TypeToken.of(Timestamp.class);
  public static final TypeToken<?> INSTANT_TYPE = TypeToken.of(Instant.class);
  public static final TypeToken<?> BINARY_TYPE = TypeToken.of(byte[].class);
  public static final TypeToken<?> ITERABLE_TYPE = TypeToken.of(Iterable.class);
  public static final TypeToken<?> COLLECTION_TYPE = TypeToken.of(Collection.class);
  public static final TypeToken<?> LIST_TYPE = TypeToken.of(List.class);
  public static final TypeToken<?> ARRAYLIST_TYPE = TypeToken.of(ArrayList.class);
  public static final TypeToken<?> SET_TYPE = TypeToken.of(Set.class);
  public static final TypeToken<?> HASHSET_TYPE = TypeToken.of(HashSet.class);
  public static final TypeToken<?> MAP_TYPE = TypeToken.of(Map.class);
  public static final TypeToken<?> HASHMAP_TYPE = TypeToken.of(HashMap.class);
  public static final TypeToken<?> OBJECT_TYPE = TypeToken.of(Object.class);

  public static Type ITERATOR_RETURN_TYPE;
  public static Type NEXT_RETURN_TYPE;
  public static Type KEY_SET_RETURN_TYPE;
  public static Type VALUES_RETURN_TYPE;

  public static final TypeToken<?> PRIMITIVE_BYTE_ARRAY_TYPE = TypeToken.of(byte[].class);
  public static final TypeToken<?> PRIMITIVE_BOOLEAN_ARRAY_TYPE = TypeToken.of(boolean[].class);
  public static final TypeToken<?> PRIMITIVE_SHORT_ARRAY_TYPE = TypeToken.of(short[].class);
  public static final TypeToken<?> PRIMITIVE_CHAR_ARRAY_TYPE = TypeToken.of(char[].class);
  public static final TypeToken<?> PRIMITIVE_INT_ARRAY_TYPE = TypeToken.of(int[].class);
  public static final TypeToken<?> PRIMITIVE_LONG_ARRAY_TYPE = TypeToken.of(long[].class);
  public static final TypeToken<?> PRIMITIVE_FLOAT_ARRAY_TYPE = TypeToken.of(float[].class);
  public static final TypeToken<?> PRIMITIVE_DOUBLE_ARRAY_TYPE = TypeToken.of(double[].class);
  public static final TypeToken<?> OBJECT_ARRAY_TYPE = TypeToken.of(Object[].class);

  public static final TypeToken<?> CLASS_TYPE = TypeToken.of(Class.class);

  /**
   * bean fields should all be in SUPPORTED_TYPES, enum, array/ITERABLE_TYPE/MAP_TYPE type, bean
   * type.
   *
   * <p>If bean fields is ITERABLE_TYPE/MAP_TYPE, the type should be super class(inclusive) of
   * List/Set/Map, or else should be a no arg constructor.
   */
  public static Set<TypeToken<?>> SUPPORTED_TYPES = new HashSet<>();

  static {
    SUPPORTED_TYPES.add(PRIMITIVE_BYTE_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_BOOLEAN_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_CHAR_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_SHORT_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_INT_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_LONG_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_FLOAT_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_DOUBLE_TYPE);

    SUPPORTED_TYPES.add(BYTE_TYPE);
    SUPPORTED_TYPES.add(BOOLEAN_TYPE);
    SUPPORTED_TYPES.add(CHAR_TYPE);
    SUPPORTED_TYPES.add(SHORT_TYPE);
    SUPPORTED_TYPES.add(INT_TYPE);
    SUPPORTED_TYPES.add(LONG_TYPE);
    SUPPORTED_TYPES.add(FLOAT_TYPE);
    SUPPORTED_TYPES.add(DOUBLE_TYPE);

    SUPPORTED_TYPES.add(STRING_TYPE);
    SUPPORTED_TYPES.add(BIG_DECIMAL_TYPE);
    // SUPPORTED_TYPES.add(BIG_INTEGER_TYPE);
    SUPPORTED_TYPES.add(DATE_TYPE);
    SUPPORTED_TYPES.add(LOCAL_DATE_TYPE);
    SUPPORTED_TYPES.add(TIMESTAMP_TYPE);
    SUPPORTED_TYPES.add(INSTANT_TYPE);
  }

  static {
    try {
      ITERATOR_RETURN_TYPE = Iterable.class.getMethod("iterator").getGenericReturnType();
      NEXT_RETURN_TYPE = Iterator.class.getMethod("next").getGenericReturnType();
      KEY_SET_RETURN_TYPE = Map.class.getMethod("keySet").getGenericReturnType();
      VALUES_RETURN_TYPE = Map.class.getMethod("values").getGenericReturnType();
    } catch (NoSuchMethodException e) {
      throw new Error(e); // should be impossible
    }
  }

  // sorted by size
  private static final List<Class<?>> sortedPrimitiveClasses =
      ImmutableList.of(
          void.class,
          boolean.class,
          byte.class,
          char.class,
          short.class,
          int.class,
          float.class,
          long.class,
          double.class);
  private static final List<Class<?>> sortedBoxedClasses =
      ImmutableList.of(
          Void.class,
          Boolean.class,
          Byte.class,
          Character.class,
          Short.class,
          Integer.class,
          Float.class,
          Long.class,
          Double.class);
  private static final int[] sortedSizes = new int[] {0, 1, 1, 2, 2, 4, 4, 8, 8};
  private static final IdentityMap<Class<?>, Class<?>> primToWrap = new IdentityMap<>(18);
  private static final IdentityMap<Class<?>, Class<?>> wrapToPrim = new IdentityMap<>(18);

  static {
    add(primToWrap, wrapToPrim, boolean.class, Boolean.class);
    add(primToWrap, wrapToPrim, byte.class, Byte.class);
    add(primToWrap, wrapToPrim, char.class, Character.class);
    add(primToWrap, wrapToPrim, double.class, Double.class);
    add(primToWrap, wrapToPrim, float.class, Float.class);
    add(primToWrap, wrapToPrim, int.class, Integer.class);
    add(primToWrap, wrapToPrim, long.class, Long.class);
    add(primToWrap, wrapToPrim, short.class, Short.class);
    add(primToWrap, wrapToPrim, void.class, Void.class);
  }

  private static void add(
      IdentityMap<Class<?>, Class<?>> forward,
      IdentityMap<Class<?>, Class<?>> backward,
      Class<?> key,
      Class<?> value) {
    forward.put(key, value);
    backward.put(value, key);
  }

  public static boolean isNullable(Class<?> clz) {
    return !isPrimitive(clz);
  }

  public static boolean isPrimitive(Class<?> clz) {
    return clz.isPrimitive();
  }

  public static boolean isBoxed(Class<?> clz) {
    return wrapToPrim.containsKey(clz);
  }

  public static Class<?> wrap(Class<?> clz) {
    return boxedType(clz);
  }

  public static Class<?> unwrap(Class<?> clz) {
    if (clz.isPrimitive()) {
      return clz;
    }
    return wrapToPrim.get(clz, clz);
  }

  public static Class<?> boxedType(Class<?> clz) {
    if (!clz.isPrimitive()) {
      return clz;
    }
    return primToWrap.get(clz);
  }

  public static List<Class<?>> getSortedPrimitiveClasses() {
    return sortedPrimitiveClasses;
  }

  public static List<Class<?>> getSortedBoxedClasses() {
    return sortedBoxedClasses;
  }

  /** Returns a primitive type class that has has max size between numericTypes. */
  public static Class<?> maxType(Class<?>... numericTypes) {
    Preconditions.checkArgument(numericTypes.length >= 2);
    int maxIndex = 0;
    for (Class<?> numericType : numericTypes) {
      int index;
      if (isPrimitive(numericType)) {
        index = sortedPrimitiveClasses.indexOf(numericType);
      } else {
        index = sortedBoxedClasses.indexOf(numericType);
      }
      if (index == -1) {
        throw new IllegalArgumentException(
            String.format("Wrong numericTypes %s", Arrays.toString(numericTypes)));
      }
      maxIndex = Math.max(maxIndex, index);
    }
    return sortedPrimitiveClasses.get(maxIndex);
  }

  /** Returns size of primitive type. */
  public static int getSizeOfPrimitiveType(TypeToken<?> numericType) {
    return getSizeOfPrimitiveType(getRawType(numericType));
  }

  public static int getSizeOfPrimitiveType(Class<?> numericType) {
    if (isPrimitive(numericType)) {
      int index = sortedPrimitiveClasses.indexOf(numericType);
      return sortedSizes[index];
    } else {
      String msg = String.format("Class %s must be primitive", numericType);
      throw new IllegalArgumentException(msg);
    }
  }

  /** Returns default value of class. */
  public static String defaultValue(Class<?> type) {
    return defaultValue(type.getSimpleName(), false);
  }

  /** Returns default value of class. */
  public static String defaultValue(String type) {
    return defaultValue(type, false);
  }

  /**
   * Returns the representation of default value for a given Java Type.
   *
   * @param type the string name of the Java type
   * @param typedNull if true, for null literals, return a typed (with a cast) version
   */
  public static String defaultValue(String type, boolean typedNull) {
    switch (type) {
      case JAVA_BOOLEAN:
        return "false";
      case JAVA_BYTE:
        return "(byte)0";
      case JAVA_SHORT:
        return "(short)0";
      case JAVA_INT:
        return "0";
      case JAVA_LONG:
        return "0L";
      case JAVA_FLOAT:
        return "0.0f";
      case JAVA_DOUBLE:
        return "0.0";
      default:
        if (typedNull) {
          return String.format("((%s)null)", type);
        } else {
          return "null";
        }
    }
  }

  /** Faster method to get raw type from {@link TypeToken} than {@link TypeToken#getRawType}. */
  public static Class<?> getRawType(TypeToken<?> typeToken) {
    Type type = typeToken.getType();
    if (type.getClass() == Class.class) {
      return (Class<?>) type;
    } else {
      return getRawType(typeToken.getType());
    }
  }

  /** Faster method to get raw type from {@link TypeToken} than {@link TypeToken#getRawType}. */
  public static Class<?> getRawType(Type type) {
    if (type instanceof TypeVariable) {
      return getRawType(((TypeVariable<?>) type).getBounds()[0]);
    } else if (type instanceof WildcardType) {
      return getRawType(((WildcardType) type).getUpperBounds()[0]);
    } else if (type instanceof ParameterizedType) {
      return (Class<?>) ((ParameterizedType) type).getRawType();
    } else if (type instanceof Class) {
      return ((Class<?>) type);
    } else if (type instanceof GenericArrayType) {
      Type componentType = ((GenericArrayType) type).getGenericComponentType();
      return Array.newInstance(getRawType(TypeToken.of(componentType)), 0).getClass();
    } else {
      throw new AssertionError("Unknown type: " + type);
    }
  }

  /** Returns dimensions of multi-dimension array. */
  public static int getArrayDimensions(TypeToken<?> type) {
    return getArrayDimensions(getRawType(type));
  }

  /** Returns dimensions of multi-dimension array. */
  public static int getArrayDimensions(Class<?> type) {
    return getArrayComponentInfo(type).f1;
  }

  public static Class<?> getArrayComponent(Class<?> type) {
    return getArrayComponentInfo(type).f0;
  }

  public static Tuple2<Class<?>, Integer> getArrayComponentInfo(Class<?> type) {
    Preconditions.checkArgument(type.isArray());
    Class<?> t = type;
    int dimension = 0;
    while (t != null && t.isArray()) {
      dimension++;
      t = t.getComponentType();
    }
    return Tuple2.of(t, dimension);
  }

  /** Returns s string that represents array type declaration of type. */
  public static String getArrayType(TypeToken<?> type) {
    return getArrayType(getRawType(type));
  }

  /** Returns s string that represents array type declaration of type. */
  public static String getArrayType(Class<?> type) {
    Tuple2<Class<?>, Integer> info = getArrayComponentInfo(type);
    StringBuilder typeBuilder = new StringBuilder(ReflectionUtils.getCanonicalName(info.f0));
    for (int i = 0; i < info.f1; i++) {
      typeBuilder.append("[]");
    }
    return typeBuilder.toString();
  }

  /** Create an array type declaration from elemType and dimensions. */
  public static String getArrayType(Class<?> elemType, int[] dimensions) {
    StringBuilder typeBuilder = new StringBuilder(ReflectionUtils.getCanonicalName(elemType));
    for (int i = 0; i < dimensions.length; i++) {
      typeBuilder.append('[').append(dimensions[i]).append(']');
    }
    return typeBuilder.toString();
  }

  /**
   * Get element type of multi dimension array.
   *
   * @param type array type
   * @return element type of multi-dimension array
   */
  public static TypeToken<?> getMultiDimensionArrayElementType(TypeToken<?> type) {
    TypeToken<?> t = type;
    while (t != null && t.isArray()) {
      t = t.getComponentType();
    }
    return t;
  }

  /** Returns element type of iterable. */
  public static TypeToken<?> getElementType(TypeToken<?> typeToken) {
    Type type = typeToken.getType();
    if (type instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType) type;
      if (parameterizedType.getRawType() == List.class) { // fastpath
        Type[] actualTypeArguments = (parameterizedType).getActualTypeArguments();
        Preconditions.checkState(actualTypeArguments.length == 1);
        Type t = actualTypeArguments[0];
        if (t.getClass() == Class.class) { // if t is wild type, upper should be parsed.
          return TypeToken.of(t);
        }
      }
    }
    if (typeToken.getType().getTypeName().startsWith("scala.collection")) {
      return ScalaTypes.getElementType(typeToken);
    }
    TypeToken<?> supertype =
        ((TypeToken<? extends Iterable<?>>) typeToken).getSupertype(Iterable.class);
    return supertype.resolveType(ITERATOR_RETURN_TYPE).resolveType(NEXT_RETURN_TYPE);
  }

  public static TypeToken<?> getCollectionType(TypeToken<?> typeToken) {
    @SuppressWarnings("unchecked")
    TypeToken<?> supertype =
        ((TypeToken<? extends Iterable<?>>) typeToken).getSupertype(Iterable.class);
    return supertype.getSubtype(Collection.class);
  }

  /** Returns key/value type of map. */
  public static Tuple2<TypeToken<?>, TypeToken<?>> getMapKeyValueType(TypeToken<?> typeToken) {
    Type type = typeToken.getType();
    if (type instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType) type;
      if (parameterizedType.getRawType() == Map.class) { // fastpath
        Type[] actualTypeArguments = (parameterizedType).getActualTypeArguments();
        Preconditions.checkState(actualTypeArguments.length == 2);
        if (actualTypeArguments[0].getClass() == Class.class
            && actualTypeArguments[1].getClass() == Class.class) {
          // if actualTypeArguments are wild type, upper should be parsed.
          return Tuple2.of(
              TypeToken.of(actualTypeArguments[0]), TypeToken.of(actualTypeArguments[1]));
        }
      }
    }
    if (typeToken.getType().getTypeName().startsWith("scala.collection")) {
      return ScalaTypes.getMapKeyValueType(typeToken);
    }
    @SuppressWarnings("unchecked")
    TypeToken<?> supertype = ((TypeToken<? extends Map<?, ?>>) typeToken).getSupertype(Map.class);
    TypeToken<?> keyType = getElementType(supertype.resolveType(KEY_SET_RETURN_TYPE));
    TypeToken<?> valueType = getElementType(supertype.resolveType(VALUES_RETURN_TYPE));
    return Tuple2.of(keyType, valueType);
  }

  public static <E> TypeToken<ArrayList<E>> arrayListOf(Class<E> elemType) {
    return new TypeToken<ArrayList<E>>() {}.where(new TypeParameter<E>() {}, elemType);
  }

  public static <E> TypeToken<List<E>> listOf(Class<E> elemType) {
    return new TypeToken<List<E>>() {}.where(new TypeParameter<E>() {}, elemType);
  }

  public static <E> TypeToken<Collection<E>> collectionOf(Class<E> elemType) {
    return collectionOf(TypeToken.of(elemType));
  }

  public static <E> TypeToken<Collection<E>> collectionOf(TypeToken<E> elemType) {
    return new TypeToken<Collection<E>>() {}.where(new TypeParameter<E>() {}, elemType);
  }

  public static <K, V> TypeToken<Map<K, V>> mapOf(Class<K> keyType, Class<V> valueType) {
    return mapOf(TypeToken.of(keyType), TypeToken.of(valueType));
  }

  public static <K, V> TypeToken<Map<K, V>> mapOf(TypeToken<K> keyType, TypeToken<V> valueType) {
    return new TypeToken<Map<K, V>>() {}.where(new TypeParameter<K>() {}, keyType)
        .where(new TypeParameter<V>() {}, valueType);
  }

  public static <K, V> TypeToken<? extends Map<K, V>> mapOf(
      Class<?> mapType, TypeToken<K> keyType, TypeToken<V> valueType) {
    TypeToken<Map<K, V>> mapTypeToken = mapOf(keyType, valueType);
    return mapTypeToken.getSubtype(mapType);
  }

  public static <K, V> TypeToken<? extends Map<K, V>> mapOf(
      Class<?> mapType, Class<K> keyType, Class<V> valueType) {
    TypeToken<Map<K, V>> mapTypeToken = mapOf(keyType, valueType);
    return mapTypeToken.getSubtype(mapType);
  }

  public static <K, V> TypeToken<HashMap<K, V>> hashMapOf(Class<K> keyType, Class<V> valueType) {
    return new TypeToken<HashMap<K, V>>() {}.where(new TypeParameter<K>() {}, keyType)
        .where(new TypeParameter<V>() {}, valueType);
  }

  public static boolean isCollection(Class<?> cls) {
    return cls == ArrayList.class || Collection.class.isAssignableFrom(cls);
  }

  public static boolean isMap(Class<?> cls) {
    return cls == HashMap.class || Map.class.isAssignableFrom(cls);
  }

  public static boolean isBean(Type type) {
    return isBean(TypeToken.of(type));
  }

  public static boolean isBean(Class<?> clz) {
    return isBean(TypeToken.of(clz));
  }

  /**
   * Returns true if class is not array/iterable/map, and all fields is {@link
   * TypeUtils#isSupported(TypeToken)}. Bean class can't be a non-static inner class. Public static
   * nested class is ok.
   */
  public static boolean isBean(TypeToken<?> typeToken) {
    return isBean(typeToken, new LinkedHashSet<>());
  }

  private static boolean isBean(TypeToken<?> typeToken, LinkedHashSet<TypeToken> walkedTypePath) {
    Class<?> cls = getRawType(typeToken);
    if (Modifier.isAbstract(cls.getModifiers()) || Modifier.isInterface(cls.getModifiers())) {
      return false;
    }
    // since we need to access class in generated code in our package, the class must be public
    // if ReflectionUtils.hasNoArgConstructor(cls) return false, we use Unsafe to create object.
    if (Modifier.isPublic(cls.getModifiers())) {
      // bean class can be static nested class, but can't be not a non-static inner class
      if (cls.getEnclosingClass() != null && !Modifier.isStatic(cls.getModifiers())) {
        return false;
      }
      LinkedHashSet<TypeToken> newTypePath = new LinkedHashSet<>(walkedTypePath);
      newTypePath.add(typeToken);
      if (cls == Object.class) {
        // return false for typeToken that point to un-specialized generic type.
        return false;
      }
      boolean maybe =
          !SUPPORTED_TYPES.contains(typeToken)
              && !typeToken.isArray()
              && !cls.isEnum()
              && !ITERABLE_TYPE.isSupertypeOf(typeToken)
              && !MAP_TYPE.isSupertypeOf(typeToken);
      if (maybe) {
        return Descriptor.getDescriptors(cls).stream()
            .allMatch(
                d -> {
                  TypeToken<?> t = d.getTypeToken();
                  // do field modifiers and getter/setter validation here, not in getDescriptors.
                  // If Modifier.isFinal(d.getModifiers()), use reflection
                  // private field that doesn't have getter/setter will be handled by reflection.
                  return isSupported(t, newTypePath) || isBean(t, newTypePath);
                });
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  /** Check if <code>typeToken</code> is supported by row-format. */
  public static boolean isSupported(TypeToken<?> typeToken) {
    return isSupported(typeToken, new LinkedHashSet<>());
  }

  private static boolean isSupported(
      TypeToken<?> typeToken, LinkedHashSet<TypeToken> walkedTypePath) {
    Class<?> cls = getRawType(typeToken);
    if (!Modifier.isPublic(cls.getModifiers())) {
      return false;
    }
    if (cls == Object.class) {
      // return true for typeToken that point to un-specialized generic type, take it as a black
      // box.
      return true;
    }
    if (SUPPORTED_TYPES.contains(typeToken)) {
      return true;
    } else if (typeToken.isArray()) {
      return isSupported(Objects.requireNonNull(typeToken.getComponentType()));
    } else if (ITERABLE_TYPE.isSupertypeOf(typeToken)) {
      boolean isSuperOfArrayList = cls.isAssignableFrom(ArrayList.class);
      boolean isSuperOfHashSet = cls.isAssignableFrom(HashSet.class);
      if ((!isSuperOfArrayList && !isSuperOfHashSet)
          && (cls.isInterface() || Modifier.isAbstract(cls.getModifiers()))) {
        return false;
      }
      return isSupported(getElementType(typeToken));
    } else if (MAP_TYPE.isSupertypeOf(typeToken)) {
      boolean isSuperOfHashMap = cls.isAssignableFrom(HashMap.class);
      if (!isSuperOfHashMap && (cls.isInterface() || Modifier.isAbstract(cls.getModifiers()))) {
        return false;
      }
      Tuple2<TypeToken<?>, TypeToken<?>> mapKeyValueType = getMapKeyValueType(typeToken);
      return isSupported(mapKeyValueType.f0) && isSupported(mapKeyValueType.f1);
    } else {
      if (walkedTypePath.contains(typeToken)) {
        throw new UnsupportedOperationException(
            "cyclic type is not supported. walkedTypePath: " + walkedTypePath);
      } else {
        LinkedHashSet<TypeToken> newTypePath = new LinkedHashSet<>(walkedTypePath);
        newTypePath.add(typeToken);
        return isBean(typeToken, newTypePath);
      }
    }
  }

  /**
   * listBeansRecursiveInclusive.
   *
   * @param beanClass beanClass
   * @return a bean classes list in this <code>beanClass</code>, all its fields and all type
   *     parameters recursively
   */
  public static LinkedHashSet<Class<?>> listBeansRecursiveInclusive(Class<?> beanClass) {
    return listBeansRecursiveInclusive(beanClass, new LinkedHashSet<>());
  }

  private static LinkedHashSet<Class<?>> listBeansRecursiveInclusive(
      Class<?> beanClass, LinkedHashSet<TypeToken<?>> walkedTypePath) {
    LinkedHashSet<Class<?>> beans = new LinkedHashSet<>();
    if (isBean(beanClass)) {
      beans.add(beanClass);
    }
    LinkedHashSet<TypeToken<?>> typeTokens = new LinkedHashSet<>();
    List<Descriptor> descriptors = Descriptor.getDescriptors(beanClass);
    for (Descriptor descriptor : descriptors) {
      TypeToken<?> typeToken = descriptor.getTypeToken();
      typeTokens.add(descriptor.getTypeToken());
      typeTokens.addAll(getAllTypeArguments(typeToken));
    }

    typeTokens.stream()
        .filter(typeToken -> isBean(getRawType(typeToken)))
        .forEach(
            typeToken -> {
              Class<?> cls = getRawType(typeToken);
              beans.add(cls);
              if (walkedTypePath.contains(typeToken)) {
                throw new UnsupportedOperationException(
                    "cyclic type is not supported. walkedTypePath: " + walkedTypePath);
              } else {
                LinkedHashSet<TypeToken<?>> newPath = new LinkedHashSet<>(walkedTypePath);
                newPath.add(typeToken);
                beans.addAll(listBeansRecursiveInclusive(cls, newPath));
              }
            });
    return beans;
  }

  public static int computeStringHash(String str) {
    byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
    long hash = 17;
    for (byte b : strBytes) {
      hash = hash * 31 + b;
      while (hash > Integer.MAX_VALUE) {
        hash = hash / 7;
      }
    }
    return (int) hash;
  }

  /** Returns generic type arguments of <code>typeToken</code>. */
  public static List<TypeToken<?>> getTypeArguments(TypeToken typeToken) {
    if (typeToken.getType() instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType) typeToken.getType();
      return Arrays.stream(parameterizedType.getActualTypeArguments())
          .map(TypeToken::of)
          .collect(Collectors.toList());
    } else {
      return new ArrayList<>();
    }
  }

  /**
   * Returns generic type arguments of <code>typeToken</code>, includes generic type arguments of
   * generic type arguments recursively.
   */
  public static List<TypeToken<?>> getAllTypeArguments(TypeToken typeToken) {
    List<TypeToken<?>> types = getTypeArguments(typeToken);
    LinkedHashSet<TypeToken<?>> allTypeArguments = new LinkedHashSet<>(types);
    for (TypeToken<?> type : types) {
      allTypeArguments.addAll(getAllTypeArguments(type));
    }

    return new ArrayList<>(allTypeArguments);
  }
}
