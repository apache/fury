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

package org.apache.fury.serializer;

import java.io.Externalizable;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.util.Platform;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;
import org.apache.fury.util.unsafe._JDKAccess;

/**
 * Serializer for class which has jdk `writeReplace`/`readResolve` method defined. This serializer
 * will skip classname writing if object returned by `writeReplace` is different from current class.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class ReplaceResolveSerializer extends Serializer {
  private static final Logger LOG = LoggerFactory.getLogger(ReplaceResolveSerializer.class);

  /**
   * Mock class of all class which has `writeReplace` method defined, so we can skip serialize those
   * classnames.
   */
  public static class ReplaceStub {}

  private static final byte ORIGINAL = 0;
  private static final byte REPLACED_NEW_TYPE = 1;
  private static final byte REPLACED_SAME_TYPE = 2;

  // Extract Method Info to cache for graalvm build time lambda generation and avoid
  // generate function repeatedly too.
  private static class ReplaceResolveInfo {
    private final Method writeReplaceMethod;
    private final Method readResolveMethod;
    private final Function writeReplaceFunc;
    private final Function readResolveFunc;

    private ReplaceResolveInfo(Class<?> cls) {
      Method writeReplaceMethod, readResolveMethod;
      // In JDK17, set private jdk method accessible will fail by default, use ObjectStreamClass
      // instead, since it set accessible.
      if (Serializable.class.isAssignableFrom(cls)) {
        ObjectStreamClass objectStreamClass = ObjectStreamClass.lookup(cls);
        writeReplaceMethod =
            (Method) ReflectionUtils.getObjectFieldValue(objectStreamClass, "writeReplaceMethod");
        readResolveMethod =
            (Method) ReflectionUtils.getObjectFieldValue(objectStreamClass, "readResolveMethod");
      } else {
        // FIXME class with `writeReplace` method defined should be Serializable,
        //  but hessian ignores this check and many existing system are using hessian,
        //  so we just warn it to keep compatibility with most applications.
        writeReplaceMethod = JavaSerializer.getWriteReplaceMethod(cls);
        readResolveMethod = JavaSerializer.getReadResolveMethod(cls);
        if (writeReplaceMethod != null) {
          LOG.warn(
              "{} doesn't implement {}, but defined writeReplace method {}",
              cls,
              Serializable.class,
              writeReplaceMethod);
        }
        if (readResolveMethod != null) {
          LOG.warn(
              "{} doesn't implement {}, but defined readResolve method {}",
              cls,
              Serializable.class,
              readResolveMethod);
        }
      }
      this.writeReplaceMethod = writeReplaceMethod;
      this.readResolveMethod = readResolveMethod;
      Class<?> declaringClass =
          writeReplaceMethod != null
              ? writeReplaceMethod.getDeclaringClass()
              : (readResolveMethod != null ? readResolveMethod.getDeclaringClass() : null);
      Function writeReplaceFunc = null, readResolveFunc = null;
      if (declaringClass != null) {
        MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(declaringClass);
        try {
          if (writeReplaceMethod != null) {
            writeReplaceFunc =
                _JDKAccess.makeJDKFunction(lookup, lookup.unreflect(writeReplaceMethod));
          }
          if (readResolveMethod != null) {
            readResolveFunc =
                _JDKAccess.makeJDKFunction(lookup, lookup.unreflect(readResolveMethod));
          }
        } catch (Exception e) {
          if (writeReplaceMethod != null && !writeReplaceMethod.isAccessible()) {
            writeReplaceMethod.setAccessible(true);
          }
          if (readResolveMethod != null && !readResolveMethod.isAccessible()) {
            readResolveMethod.setAccessible(true);
          }
        }
      }
      this.writeReplaceFunc = writeReplaceFunc;
      this.readResolveFunc = readResolveFunc;
    }

    Object writeReplace(Object o) {
      if (writeReplaceFunc != null) {
        return writeReplaceFunc.apply(o);
      } else {
        try {
          return writeReplaceMethod.invoke(o);
        } catch (Exception e) {
          Platform.throwException(e);
          throw new IllegalStateException(e);
        }
      }
    }
  }

  private static final ClassValue<ReplaceResolveInfo> REPLACE_RESOLVE_INFO_CACHE =
      new ClassValue<ReplaceResolveInfo>() {
        @Override
        protected ReplaceResolveInfo computeValue(Class<?> type) {
          return new ReplaceResolveInfo(type);
        }
      };

  private static class MethodInfoCache {
    private final ReplaceResolveInfo info;

    private Serializer objectSerializer;

    public MethodInfoCache(ReplaceResolveInfo info) {
      this.info = info;
    }

    public void setObjectSerializer(Serializer objectSerializer) {
      this.objectSerializer = objectSerializer;
    }
  }

  static MethodInfoCache newJDKMethodInfoCache(Class<?> cls, Fury fury) {
    ReplaceResolveInfo replaceResolveInfo = REPLACE_RESOLVE_INFO_CACHE.get(cls);
    MethodInfoCache methodInfoCache = new MethodInfoCache(replaceResolveInfo);
    Class<? extends Serializer> serializerClass;
    if (Externalizable.class.isAssignableFrom(cls)) {
      serializerClass = ExternalizableSerializer.class;
    } else if (JavaSerializer.getReadObjectMethod(cls, true) == null
        && JavaSerializer.getWriteObjectMethod(cls, true) == null) {
      serializerClass =
          fury.getClassResolver()
              .getObjectSerializerClass(
                  cls,
                  sc -> methodInfoCache.setObjectSerializer(createDataSerializer(fury, cls, sc)));
    } else {
      serializerClass = fury.getDefaultJDKStreamSerializerType();
    }
    methodInfoCache.setObjectSerializer(createDataSerializer(fury, cls, serializerClass));
    return methodInfoCache;
  }

  /**
   * Create data serializer for `cls`. Note that `cls` may be first read by this fury, so there
   * maybe no serializer created for it, `getSerializer(cls, false)` will be null in such cases.
   *
   * @see #readObject
   */
  private static Serializer createDataSerializer(
      Fury fury, Class<?> cls, Class<? extends Serializer> sc) {
    Serializer prev = fury.getClassResolver().getSerializer(cls, false);
    Serializer serializer = Serializers.newSerializer(fury, cls, sc);
    fury.getClassResolver().resetSerializer(cls, prev);
    return serializer;
  }

  private final RefResolver refResolver;
  private final ClassResolver classResolver;
  private final MethodInfoCache jdkMethodInfoWriteCache;
  private final ClassInfo writeClassInfo;
  private final Map<Class<?>, MethodInfoCache> classClassInfoHolderMap = new HashMap<>();

  public ReplaceResolveSerializer(Fury fury, Class type) {
    super(fury, type);
    refResolver = fury.getRefResolver();
    classResolver = fury.getClassResolver();
    // `setSerializer` before `newJDKMethodInfoCache` since it query classinfo from `classResolver`,
    // which create serializer in turn.
    // ReplaceResolveSerializer is used as data serializer for ImmutableList/Map,
    // which serializer is already set.
    classResolver.setSerializerIfAbsent(type, this);
    if (type != ReplaceStub.class) {
      jdkMethodInfoWriteCache = newJDKMethodInfoCache(type, fury);
      classClassInfoHolderMap.put(type, jdkMethodInfoWriteCache);
      // FIXME new classinfo may miss serializer update in async compilation mode.
      writeClassInfo = classResolver.newClassInfo(type, this, ClassResolver.NO_CLASS_ID);
    } else {
      jdkMethodInfoWriteCache = null;
      writeClassInfo = null;
    }
  }

  @Override
  public void write(MemoryBuffer buffer, Object value) {
    MethodInfoCache jdkMethodInfoCache = this.jdkMethodInfoWriteCache;
    ReplaceResolveInfo replaceResolveInfo = jdkMethodInfoCache.info;
    Method writeReplaceMethod = replaceResolveInfo.writeReplaceMethod;
    if (writeReplaceMethod != null) {
      Object original = value;
      value = replaceResolveInfo.writeReplace(value);
      // FIXME JDK serialization will update reference table, which will change deserialized object
      // graph.
      //  If fury doesn't update reference table, deserialized object graph will be same,
      //  which is not a problem in almost every case but inconsistent with JDK serialization.
      if (value == null || value.getClass() != type) {
        buffer.writeByte(REPLACED_NEW_TYPE);
        if (!refResolver.writeRefOrNull(buffer, value)) {
          // replace original object reference id with new object reference id
          // for later object graph serialization.
          // written `REF_VALUE_FLAG`/`NOT_NULL_VALUE_FLAG` id outside this method call will be
          // ignored.
          refResolver.replaceRef(original, value);
          fury.writeNonRef(buffer, value);
        }
      } else {
        if (value != original) {
          buffer.writeByte(REPLACED_SAME_TYPE);
          if (!refResolver.writeRefOrNull(buffer, value)) {
            // replace original object reference id with new object reference id
            // for later object graph serialization,
            // written `REF_VALUE_FLAG`/`NOT_NULL_VALUE_FLAG` id outside this method call will be
            // ignored.
            refResolver.replaceRef(original, value);
            writeObject(buffer, value, jdkMethodInfoCache);
          }
        } else {
          buffer.writeByte(ORIGINAL);
          writeObject(buffer, value, jdkMethodInfoCache);
        }
      }
    } else {
      buffer.writeByte(ORIGINAL);
      writeObject(buffer, value, jdkMethodInfoCache);
    }
  }

  private void writeObject(MemoryBuffer buffer, Object value, MethodInfoCache jdkMethodInfoCache) {
    classResolver.writeClass(buffer, writeClassInfo);
    jdkMethodInfoCache.objectSerializer.write(buffer, value);
  }

  @Override
  public Object read(MemoryBuffer buffer) {
    byte flag = buffer.readByte();
    RefResolver refResolver = this.refResolver;
    if (flag == REPLACED_NEW_TYPE) {
      int outerRefId = refResolver.lastPreservedRefId();
      int nextReadRefId = refResolver.tryPreserveRefId(buffer);
      if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
        // ref value or not-null value
        Object o = fury.readData(buffer, classResolver.readClassInfo(buffer));
        refResolver.setReadObject(nextReadRefId, o);
        refResolver.setReadObject(outerRefId, o);
        return o;
      } else {
        return refResolver.getReadObject();
      }
    } else if (flag == REPLACED_SAME_TYPE) {
      int outerRefId = refResolver.lastPreservedRefId();
      int nextReadRefId = refResolver.tryPreserveRefId(buffer);
      if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
        // ref value or not-null value
        Object o = readObject(buffer);
        refResolver.setReadObject(nextReadRefId, o);
        refResolver.setReadObject(outerRefId, o);
        return o;
      } else {
        return refResolver.getReadObject();
      }
    } else {
      Preconditions.checkArgument(flag == ORIGINAL);
      return readObject(buffer);
    }
  }

  private Object readObject(MemoryBuffer buffer) {
    Class cls = classResolver.readClassInternal(buffer);
    MethodInfoCache jdkMethodInfoCache = classClassInfoHolderMap.get(cls);
    if (jdkMethodInfoCache == null) {
      jdkMethodInfoCache = newJDKMethodInfoCache(cls, fury);
      classClassInfoHolderMap.put(cls, jdkMethodInfoCache);
    }
    Object o = jdkMethodInfoCache.objectSerializer.read(buffer);
    ReplaceResolveInfo replaceResolveInfo = jdkMethodInfoCache.info;
    Method readResolveMethod = replaceResolveInfo.readResolveMethod;
    if (readResolveMethod != null) {
      if (replaceResolveInfo.readResolveFunc != null) {
        return replaceResolveInfo.readResolveFunc.apply(o);
      }
      try {
        return readResolveMethod.invoke(o);
      } catch (Exception e) {
        Platform.throwException(e);
        throw new IllegalStateException("unreachable");
      }
    } else {
      return o;
    }
  }
}
