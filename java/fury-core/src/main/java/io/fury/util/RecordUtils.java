package io.fury.util;

import io.fury.collection.Tuple2;
import io.fury.util.unsafe._JDKAccess;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;

/**
 * Utils for java.lang.Record subclasses.
 */
@SuppressWarnings({"rawtypes"})
public class RecordUtils {
  private static final Method IS_RECORD;
  private static final Method GET_RECORD_COMPONENTS;
  private static final Method GET_DECLARING_RECORD;
  private static final Method GET_NAME;
  private static final Method GET_TYPE;
  private static final Method GET_GENERIC_TYPE;
  private static final Method GET_ACCESSOR;

  static {
    Method isRecord;
    Method getRecordComponents;
    Method getDeclaringRecord;
    Method getName;
    Method getType;
    Method getGenericType;
    Method getAccessor;

    try {
      // use reflection to support compilation for jdk before 16
      isRecord = Class.class.getDeclaredMethod("isRecord");
      getRecordComponents = Class.class.getMethod("getRecordComponents");
      Class<?> componentClass = Class.forName("java.lang.reflect.RecordComponent");
      getName = componentClass.getMethod("getName");
      getType = componentClass.getMethod("getType");
      getDeclaringRecord = componentClass.getMethod("getDeclaringRecord");
      getGenericType = componentClass.getMethod("getGenericType");
      getAccessor = componentClass.getMethod("getAccessor");
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      isRecord = null;
      getRecordComponents = null;
      getDeclaringRecord = null;
      getName = null;
      getType = null;
      getGenericType = null;
      getAccessor = null;
    }
    // all public methods, no need to setAccessible.
    IS_RECORD = isRecord;
    GET_RECORD_COMPONENTS = getRecordComponents;
    GET_DECLARING_RECORD = getDeclaringRecord;
    GET_NAME = getName;
    GET_TYPE = getType;
    GET_GENERIC_TYPE = getGenericType;
    GET_ACCESSOR = getAccessor;
  }

  private static final ClassValue<Boolean> isRecordCache = new ClassValue<Boolean>() {
    @Override
    protected Boolean computeValue(Class<?> type) {
      try {
        return (boolean) IS_RECORD.invoke(type);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
  };

  private static final ClassValue<RecordComponent[]> recordComponentsCache =
    new ClassValue<RecordComponent[]>() {
      @Override
      protected RecordComponent[] computeValue(Class<?> type) {
        try {
          MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(type);
          Object[] components = (Object[]) GET_RECORD_COMPONENTS.invoke(type);
          RecordComponent[] recordComponents = new RecordComponent[components.length];
          for (int i = 0; i < components.length; i++) {
            Object component = components[i];
            Method accessor = (Method) GET_ACCESSOR.invoke(component);
            recordComponents[i] = new RecordComponent(
              (Class<?>) GET_DECLARING_RECORD.invoke(component),
              (String) GET_NAME.invoke(component),
              (Class<?>) GET_TYPE.invoke(component),
              (Type) GET_GENERIC_TYPE.invoke(component),
              accessor,
              lookup.unreflect(accessor)
            );
          }
          return recordComponents;
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    };


  private static final ClassValue<Tuple2<Constructor, MethodHandle>> ctrCache =
    new ClassValue<Tuple2<Constructor, MethodHandle>>() {
      @Override
      protected Tuple2<Constructor, MethodHandle> computeValue(Class<?> type) {
        RecordComponent[] components = RecordUtils.getRecordComponents(type);
        if (components == null) {
          return null;
        }
        Class<?>[] paramTypes = Arrays.stream(components).map(
          RecordComponent::getType).toArray(Class<?>[]::new);
        Constructor constructor;
        try {
          constructor = type.getDeclaredConstructor(paramTypes);
        } catch (NoSuchMethodException e) {
          throw new RuntimeException(e);
        }
        MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(type);
        if (lookup != null) {
          try {
            MethodHandle handle = lookup.findConstructor(type, MethodType.methodType(void.class, paramTypes));
            return Tuple2.of(constructor, handle);
          } catch (NoSuchMethodException | IllegalAccessException e) {
            return Tuple2.of(constructor, null);
          }
        } else {
          return Tuple2.of(constructor, null);
        }
      }
    };

  /**
   * Returns {@code true} if and only if this class is a record class.
   *
   * <p> The direct superclass of a record
   * class is {@code java.lang.Record}. A record class is {@linkplain
   * Modifier#FINAL final}. A record class has (possibly zero) record
   * components; {@link #getRecordComponents} returns a non-null but
   * possibly empty value for a record.
   *
   * <p> Note that class java.lang.Record is not a record class and thus
   * invoking this method on class {@code Record} returns {@code false}.
   *
   * @return true if and only if this class is a record class, otherwise false
   * @since 16
   */
  public static boolean isRecord(Class<?> cls) {
    if (IS_RECORD == null) {
      return false;
    }
    return isRecordCache.get(cls);
  }

  /**
   * Returns an array of {@code RecordComponent} objects representing all the
   * record components of this record class, or {@code null} if this class is
   * not a record class.
   *
   * <p> The components are returned in the same order that they are declared
   * in the record header. The array is empty if this record class has no
   * components. If the class is not a record class, that is {@link
   * #isRecord} returns {@code false}, then this method returns {@code null}.
   * Conversely, if {@link #isRecord} returns {@code true}, then this method
   * returns a non-null value.
   */
  public static RecordComponent[] getRecordComponents(Class<?> cls) {
    if (GET_RECORD_COMPONENTS == null) {
      return null;
    }
    return recordComponentsCache.get(cls);
  }

  /**
   * Returns the record canonical constructor
   */
  public static Tuple2<Constructor, MethodHandle> getRecordConstructor(Class<?> cls) {
    return ctrCache.get(cls);
  }

}
