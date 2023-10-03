/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.util;

import static io.fury.type.TypeUtils.OBJECT_TYPE;
import static io.fury.type.TypeUtils.getRawType;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.fury.collection.Tuple3;
import io.fury.util.function.Functions;
import io.fury.util.unsafe._JDKAccess;
import java.io.ObjectStreamClass;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reflection util.
 *
 * @author chaokunyang
 */
@SuppressWarnings("UnstableApiUsage")
public class ReflectionUtils {
  public static boolean isAbstract(Class<?> clazz) {
    if (clazz.isArray()) {
      return false;
    }
    return Modifier.isAbstract(clazz.getModifiers());
  }

  public static boolean hasNoArgConstructor(Class<?> clazz) {
    return getNoArgConstructor(clazz) != null;
  }

  public static boolean hasPublicNoArgConstructor(Class<?> clazz) {
    Constructor<?> constructor = getNoArgConstructor(clazz);
    return constructor != null && Modifier.isPublic(constructor.getModifiers());
  }

  public static Constructor<?> getNoArgConstructor(Class<?> clazz) {
    if (clazz.isInterface()) {
      return null;
    }
    if (Modifier.isAbstract(clazz.getModifiers())) {
      return null;
    }
    Constructor<?>[] constructors = clazz.getDeclaredConstructors();
    if (constructors.length == 0) {
      return null;
    } else {
      return Stream.of(constructors)
          .filter((c) -> c.getParameterCount() == 0)
          .findAny()
          .orElse(null);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <T> Constructor<T> getExecutableNoArgConstructor(Class<T> cls) {
    Constructor constructor = null;
    try {
      constructor = getNoArgConstructor(cls);
      if (constructor != null && !constructor.isAccessible()) {
        // Some class may fail this for JDK9+
        constructor.setAccessible(true);
      }
    } catch (Exception e) {
      ObjectStreamClass streamClass = ObjectStreamClass.lookup(cls);
      if (streamClass != null) { // streamClass will be null if cls is not `Serializable`.
        constructor = (Constructor) ReflectionUtils.getObjectFieldValue(streamClass, "cons");
      }
    }
    return constructor;
  }

  public static <T> MethodHandle getExecutableNoArgConstructorHandle(Class<T> cls) {
    Constructor<T> ctr = getExecutableNoArgConstructor(cls);
    if (ctr == null) {
      return null;
    }
    MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(ctr.getDeclaringClass());
    try {
      return lookup.findConstructor(ctr.getDeclaringClass(), MethodType.methodType(void.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      Platform.throwException(e);
      throw new IllegalStateException("unreachable");
    }
  }

  /**
   * Returns an accessible no-argument constructor for provided class.
   *
   * @throws IllegalArgumentException if not exists or not accessible.
   */
  public static Constructor<?> newAccessibleNoArgConstructor(Class<?> clz) {
    try {
      Constructor<?> constructor = getNoArgConstructor(clz);
      if (constructor == null) {
        throw new IllegalArgumentException(
            String.format("Please an accessible no argument constructor for class %s", clz));
      }
      if (!constructor.isAccessible()) {
        constructor.setAccessible(true);
      }
      return constructor;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Please an accessible no argument constructor for class %s", clz), e);
    }
  }

  /**
   * Returns all methods named by {@code methodName}, for covariant return type, return the most
   * specific method.
   */
  public static List<Method> findMethods(Class<?> cls, String methodName) {
    List<Class<?>> classes = new ArrayList<>();
    Class<?> clazz = cls;
    while (clazz != null) {
      classes.add(clazz);
      clazz = clazz.getSuperclass();
    }
    classes.addAll(getAllInterfaces(cls));
    if (classes.indexOf(Object.class) == -1) {
      classes.add(Object.class);
    }

    LinkedHashMap<List<Class<?>>, Method> methods = new LinkedHashMap<>();
    for (Class<?> superClz : classes) {
      for (Method m : superClz.getDeclaredMethods()) {
        if (m.getName().equals(methodName)) {
          List<Class<?>> params = Arrays.asList(m.getParameterTypes());
          Method method = methods.get(params);
          if (method == null) {
            methods.put(params, m);
          } else {
            // for covariant return type, use the most specific method
            if (method.getReturnType().isAssignableFrom(m.getReturnType())) {
              methods.put(params, m);
            }
          }
        }
      }
    }
    return new ArrayList<>(methods.values());
  }

  /**
   * Gets a <code>List</code> of all interfaces implemented by the given class and its superclasses.
   *
   * <p>The order is determined by looking through each interface in turn as declared in the source
   * file and following its hierarchy up.
   */
  public static List<Class<?>> getAllInterfaces(Class<?> cls) {
    if (cls == null) {
      return null;
    }

    LinkedHashSet<Class<?>> interfacesFound = new LinkedHashSet<>();
    getAllInterfaces(cls, interfacesFound);
    return new ArrayList<>(interfacesFound);
  }

  private static void getAllInterfaces(Class<?> cls, LinkedHashSet<Class<?>> interfacesFound) {
    while (cls != null) {
      Class[] interfaces = cls.getInterfaces();
      for (Class anInterface : interfaces) {
        if (!interfacesFound.contains(anInterface)) {
          interfacesFound.add(anInterface);
          getAllInterfaces(anInterface, interfacesFound);
        }
      }

      cls = cls.getSuperclass();
    }
  }

  /** Returns true if any method named {@code methodName} has exception. */
  public static boolean hasException(Class<?> cls, String methodName) {
    List<Method> methods = findMethods(cls, methodName);
    if (methods.isEmpty()) {
      String msg = String.format("class %s doesn't have method %s", cls, methodName);
      throw new IllegalArgumentException(msg);
    }
    return methods.get(0).getExceptionTypes().length > 0;
  }

  /** Returns true if any method named {@code methodName} has checked exception. */
  public static boolean hasCheckedException(Class<?> cls, String methodName) {
    List<Method> methods = findMethods(cls, methodName);
    if (methods.isEmpty()) {
      String msg = String.format("class %s doesn't have method %s", cls, methodName);
      throw new IllegalArgumentException(msg);
    }
    for (Class<?> exceptionType : methods.get(0).getExceptionTypes()) {
      if (!RuntimeException.class.isAssignableFrom(exceptionType)) {
        return true;
      }
    }
    return false;
  }

  public static Class<?> getReturnType(Class<?> cls, String methodName) {
    List<Method> methods = findMethods(cls, methodName);
    if (methods.isEmpty()) {
      String msg = String.format("class %s doesn't have method %s", cls, methodName);
      throw new IllegalArgumentException(msg);
    }
    Set<? extends Class<?>> returnTypes =
        methods.stream().map(Method::getReturnType).collect(Collectors.toSet());
    Preconditions.checkArgument(returnTypes.size() == 1);
    return methods.get(0).getReturnType();
  }

  public static Field getDeclaredField(Class<?> cls, String fieldName) {
    try {
      return cls.getDeclaredField(fieldName);
    } catch (NoSuchFieldException e) {
      Platform.throwException(e);
      throw new IllegalStateException("Unreachable");
    }
  }

  /**
   * Return a field named <code>fieldName</code> from <code>cls</code>. Search parent class if not
   * found.
   */
  public static Field getField(Class<?> cls, String fieldName) {
    Field field = getFieldNullable(cls, fieldName);
    if (field == null) {
      String msg = String.format("class %s doesn't have field %s", cls, fieldName);
      throw new IllegalArgumentException(msg);
    }
    return field;
  }

  public static Field getFieldNullable(Class<?> cls, String fieldName) {
    Class<?> clazz = cls;
    do {
      Field[] fields = clazz.getDeclaredFields();
      for (Field field : fields) {
        if (field.getName().equals(fieldName)) {
          return field;
        }
      }
      clazz = clazz.getSuperclass();
    } while (clazz != null);
    return null;
  }

  /**
   * Get fields.
   *
   * @param cls class
   * @param searchParent true if return super classes fields.
   * @return all fields of class. And all fields of super classes in order from subclass to super
   *     classes if <code>searchParent</code> is true
   */
  public static List<Field> getFields(Class<?> cls, boolean searchParent) {
    Preconditions.checkNotNull(cls);
    List<Field> fields = new ArrayList<>();
    if (searchParent) {
      Class<?> clazz = cls;
      do {
        Collections.addAll(fields, clazz.getDeclaredFields());
        clazz = clazz.getSuperclass();
      } while (clazz != null);
    } else {
      Collections.addAll(fields, cls.getDeclaredFields());
    }
    return fields;
  }

  /** Get fields values from provided object. */
  public static List<Object> getFieldValues(Collection<Field> fields, Object o) {
    List<Object> results = new ArrayList<>(fields.size());
    for (Field field : fields) {
      // Platform.objectFieldOffset(field) can't handle primitive field.
      Object fieldValue = FieldAccessor.createAccessor(field).get(o);
      results.add(fieldValue);
    }
    return results;
  }

  public static long getFieldOffset(Field field) {
    return field == null ? -1 : Platform.objectFieldOffset(field);
  }

  public static long getFieldOffset(Class<?> cls, String fieldName) {
    Field field = getFieldNullable(cls, fieldName);
    return getFieldOffset(field);
  }

  public static long getFieldOffsetChecked(Class<?> cls, String fieldName) {
    long offset = getFieldOffset(cls, fieldName);
    Preconditions.checkArgument(offset != -1);
    return offset;
  }

  /**
   * Returns object field value with specified name or nul if not exists.
   *
   * @throws IllegalArgumentException when field type is primitive.
   */
  public static Object getObjectFieldValue(Object obj, String fieldName) {
    Class<?> cls = obj.getClass();
    Preconditions.checkArgument(!cls.isPrimitive());
    while (cls != Object.class) {
      try {
        Field field = cls.getDeclaredField(fieldName);
        long fieldOffset = Platform.objectFieldOffset(field);
        return Platform.getObject(obj, fieldOffset);
        // CHECKSTYLE.OFF:EmptyCatchBlock
      } catch (NoSuchFieldException ignored) {
      }
      // CHECKSTYLE.ON:EmptyCatchBlock
      cls = cls.getSuperclass();
    }
    return null;
  }

  public static String getClassNameWithoutPackage(Class<?> clz) {
    String className = clz.getName();
    int index = className.lastIndexOf(".");
    if (index != -1) {
      return className.substring(index + 1);
    } else {
      return className;
    }
  }

  public static boolean isPublic(TypeToken<?> targetType) {
    return Modifier.isPublic(getRawType(targetType).getModifiers());
  }

  public static boolean isPublic(Class<?> type) {
    return Modifier.isPublic(type.getModifiers());
  }

  public static boolean isPrivate(TypeToken<?> targetType) {
    return Modifier.isPrivate(getRawType(targetType).getModifiers());
  }

  public static boolean isPrivate(Class<?> cls) {
    return Modifier.isPrivate(cls.getModifiers());
  }

  public static boolean isFinal(Class<?> targetType) {
    return Modifier.isFinal(targetType.getModifiers());
  }

  public static TypeToken getPublicSuperType(TypeToken typeToken) {
    if (!isPublic(typeToken)) {
      Class<?> rawType = Objects.requireNonNull(getRawType(typeToken));
      Class<?> cls = rawType;
      while (cls != null && !isPublic(cls)) {
        cls = cls.getSuperclass();
      }
      if (cls == null) {
        for (Class<?> typeInterface : rawType.getInterfaces()) {
          if (isPublic(typeInterface)) {
            return TypeToken.of(typeInterface);
          }
        }
        return OBJECT_TYPE;
      } else {
        return TypeToken.of(cls);
      }
    } else {
      return typeToken;
    }
  }

  /** Returns package of a class. If package is absent, return empty string. */
  public static String getPackage(Class<?> cls) {
    String pkg;
    // Janino generated class's package might be null
    if (cls.getPackage() == null) {
      String className = cls.getName();
      int index = className.lastIndexOf(".");
      if (index != -1) {
        pkg = className.substring(0, index);
      } else {
        pkg = "";
      }
    } else {
      pkg = cls.getPackage().getName();
    }
    return pkg;
  }

  // Invoked by JIT.
  public static Class<?> loadClass(Class<?> neighbor, String className) {
    try {
      return neighbor.getClassLoader().loadClass(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T unsafeCopy(T obj) {
    @SuppressWarnings("unchecked")
    T newInstance = (T) Platform.newInstance(obj.getClass());
    for (Field field : getFields(obj.getClass(), true)) {
      if (!Modifier.isStatic(field.getModifiers())) {
        // Don't cache accessors by `obj.getClass()` using WeakHashMap, the `field` will reference
        // `class`, which cause circular reference.
        FieldAccessor accessor = FieldAccessor.createAccessor(field);
        accessor.set(newInstance, accessor.get(obj));
      }
    }
    return newInstance;
  }

  public static void unsafeCopy(Object from, Object to) {
    Tuple3<Set<String>, Map<String, Field>, Map<String, Field>> commonFieldsInfo =
        getCommonFields(from.getClass(), to.getClass());
    Map<String, Field> fieldMap1 = commonFieldsInfo.f1;
    Map<String, Field> fieldMap2 = commonFieldsInfo.f2;
    for (String commonField : commonFieldsInfo.f0) {
      Field field1 = fieldMap1.get(commonField);
      Field field2 = fieldMap2.get(commonField);
      FieldAccessor accessor1 = FieldAccessor.createAccessor(field1);
      FieldAccessor accessor2 = FieldAccessor.createAccessor(field2);
      accessor2.set(to, accessor1.get(from));
    }
  }

  public static boolean objectFieldsEquals(Object o1, Object o2) {
    List<Field> fields1 = getFields(o1.getClass(), true);
    List<Field> fields2 = getFields(o2.getClass(), true);
    if (fields1.size() != fields2.size()) {
      return false;
    }
    Tuple3<Set<String>, Map<String, Field>, Map<String, Field>> commonFieldsInfo =
        getCommonFields(o1.getClass(), o2.getClass());
    if (commonFieldsInfo.f1.size() != fields1.size()) {
      return false;
    }
    if (commonFieldsInfo.f1.size() != commonFieldsInfo.f2.size()) {
      return false;
    }
    return objectCommonFieldsEquals(commonFieldsInfo, o1, o2);
  }

  public static boolean objectFieldsEquals(Set<String> fields, Object o1, Object o2) {
    Tuple3<Set<String>, Map<String, Field>, Map<String, Field>> commonFieldsInfo =
        getCommonFields(o1.getClass(), o2.getClass());
    Map<String, Field> map1 =
        commonFieldsInfo.f1.entrySet().stream()
            .filter(e -> fields.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<String, Field> map2 =
        commonFieldsInfo.f2.entrySet().stream()
            .filter(e -> fields.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return objectCommonFieldsEquals(Tuple3.of(fields, map1, map2), o1, o2);
  }

  public static boolean objectCommonFieldsEquals(Object o1, Object o2) {
    Tuple3<Set<String>, Map<String, Field>, Map<String, Field>> commonFieldsInfo =
        getCommonFields(o1.getClass(), o2.getClass());
    return objectCommonFieldsEquals(commonFieldsInfo, o1, o2);
  }

  private static boolean objectCommonFieldsEquals(
      Tuple3<Set<String>, Map<String, Field>, Map<String, Field>> commonFieldsInfo,
      Object o1,
      Object o2) {

    for (String commonField : commonFieldsInfo.f0) {
      Field field1 = commonFieldsInfo.f1.get(commonField);
      Field field2 = commonFieldsInfo.f2.get(commonField);
      FieldAccessor accessor1 = FieldAccessor.createAccessor(field1);
      FieldAccessor accessor2 = FieldAccessor.createAccessor(field2);
      Object f1 = accessor1.get(o1);
      Object f2 = accessor2.get(o2);
      if (f1 == null) {
        if (f2 != null) {
          return false;
        }
      } else {
        if (field1.getType().isArray()) {
          if (field1.getType() == boolean[].class) {
            if (!Arrays.equals((boolean[]) f1, (boolean[]) f2)) {
              return false;
            }
          } else if (field1.getType() == byte[].class) {
            if (!Arrays.equals((byte[]) f1, (byte[]) f2)) {
              return false;
            }
          } else if (field1.getType() == short[].class) {
            if (!Arrays.equals((short[]) f1, (short[]) f2)) {
              return false;
            }
          } else if (field1.getType() == char[].class) {
            if (!Arrays.equals((char[]) f1, (char[]) f2)) {
              return false;
            }
          } else if (field1.getType() == int[].class) {
            if (!Arrays.equals((int[]) f1, (int[]) f2)) {
              return false;
            }
          } else if (field1.getType() == long[].class) {
            if (!Arrays.equals((long[]) f1, (long[]) f2)) {
              return false;
            }
          } else if (field1.getType() == float[].class) {
            if (!Arrays.equals((float[]) f1, (float[]) f2)) {
              return false;
            }
          } else if (field1.getType() == double[].class) {
            if (!Arrays.equals((double[]) f1, (double[]) f2)) {
              return false;
            }
          } else {
            if (!Arrays.deepEquals((Object[]) f1, (Object[]) f2)) {
              return false;
            }
          }
        } else {
          if (!f1.equals(f2)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  public static Tuple3<Set<String>, Map<String, Field>, Map<String, Field>> getCommonFields(
      Class<?> cls1, Class<?> cls2) {
    List<Field> fields1 = getFields(cls1, true);
    List<Field> fields2 = getFields(cls2, true);
    return getCommonFields(fields1, fields2);
  }

  public static Tuple3<Set<String>, Map<String, Field>, Map<String, Field>> getCommonFields(
      List<Field> fields1, List<Field> fields2) {
    Map<String, Field> fieldMap1 =
        fields1.stream()
            .collect(
                Collectors.toMap(
                    // don't use `getGenericType` since janino doesn't support generics.
                    f -> f.getDeclaringClass().getSimpleName() + f.getType() + f.getName(),
                    f -> f));
    Map<String, Field> fieldMap2 =
        fields2.stream()
            .collect(
                Collectors.toMap(
                    f -> f.getDeclaringClass().getSimpleName() + f.getType() + f.getName(),
                    f -> f));
    Set<String> commonFields = fieldMap1.keySet();
    commonFields.retainAll(fieldMap2.keySet());
    return Tuple3.of(commonFields, fieldMap1, fieldMap2);
  }

  public static boolean isJdkProxy(Class<?> clz) {
    return Proxy.isProxyClass(clz);
  }

  public static boolean isDynamicGeneratedCLass(Class<?> cls) {
    // TODO(chaokunyang) add cglib check
    return Functions.isLambda(cls) || isJdkProxy(cls);
  }
}
