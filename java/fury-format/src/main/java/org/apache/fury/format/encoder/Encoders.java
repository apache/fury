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

package org.apache.fury.format.encoder;

import static org.apache.fury.type.TypeUtils.getRawType;

import com.google.common.reflect.TypeToken;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fury.Fury;
import org.apache.fury.codegen.CodeGenerator;
import org.apache.fury.codegen.CompileUnit;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.exception.ClassNotCompatibleException;
import org.apache.fury.format.row.binary.BinaryArray;
import org.apache.fury.format.row.binary.BinaryMap;
import org.apache.fury.format.row.binary.BinaryRow;
import org.apache.fury.format.row.binary.writer.BinaryArrayWriter;
import org.apache.fury.format.row.binary.writer.BinaryRowWriter;
import org.apache.fury.format.type.DataTypes;
import org.apache.fury.format.type.TypeInference;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.type.TypeUtils;

/**
 * Factory to create {@link Encoder}.
 *
 * <p>, ganrunsheng
 */
public class Encoders {
  private static final Logger LOG = LoggerFactory.getLogger(Encoders.class);

  public static <T> RowEncoder<T> bean(Class<T> beanClass) {
    return bean(beanClass, 16);
  }

  public static <T> RowEncoder<T> bean(Class<T> beanClass, int initialBufferSize) {
    return bean(beanClass, null, initialBufferSize);
  }

  public static <T> RowEncoder<T> bean(Class<T> beanClass, Fury fury) {
    return bean(beanClass, fury, 16);
  }

  public static <T> RowEncoder<T> bean(Class<T> beanClass, Fury fury, int initialBufferSize) {
    Schema schema = TypeInference.inferSchema(beanClass);
    BinaryRowWriter writer = new BinaryRowWriter(schema);
    RowEncoder<T> encoder = bean(beanClass, writer, fury);
    return new RowEncoder<T>() {

      @Override
      public Schema schema() {
        return encoder.schema();
      }

      @Override
      public T fromRow(BinaryRow row) {
        return encoder.fromRow(row);
      }

      @Override
      public BinaryRow toRow(T obj) {
        writer.setBuffer(MemoryUtils.buffer(initialBufferSize));
        writer.reset();
        return encoder.toRow(obj);
      }

      @Override
      public T decode(byte[] bytes) {
        return encoder.decode(bytes);
      }

      @Override
      public byte[] encode(T obj) {
        return encoder.encode(obj);
      }
    };
  }

  public static <T> RowEncoder<T> bean(Class<T> beanClass, BinaryRowWriter writer) {
    return bean(beanClass, writer, null);
  }

  /**
   * Creates an encoder for Java Bean of type T.
   *
   * <p>T must be publicly accessible.
   *
   * <p>supported types for java bean field: - primitive types: boolean, int, double, etc. - boxed
   * types: Boolean, Integer, Double, etc. - String - java.math.BigDecimal, java.math.BigInteger -
   * time related: java.sql.Date, java.sql.Timestamp, java.time.LocalDate, java.time.Instant -
   * collection types: only array and java.util.List currently, map support is in progress - nested
   * java bean.
   */
  public static <T> RowEncoder<T> bean(Class<T> beanClass, BinaryRowWriter writer, Fury fury) {
    Schema schema = writer.getSchema();

    try {
      Class<?> rowCodecClass = loadOrGenRowCodecClass(beanClass);
      Object references = new Object[] {schema, writer, fury};
      GeneratedRowEncoder codec =
          rowCodecClass
              .asSubclass(GeneratedRowEncoder.class)
              .getConstructor(Object[].class)
              .newInstance(references);
      long schemaHash = DataTypes.computeSchemaHash(schema);

      return new RowEncoder<T>() {
        private final MemoryBuffer buffer = MemoryUtils.buffer(16);

        @Override
        public Schema schema() {
          return schema;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T fromRow(BinaryRow row) {
          return (T) codec.fromRow(row);
        }

        @Override
        public BinaryRow toRow(T obj) {
          return codec.toRow(obj);
        }

        @Override
        public T decode(byte[] bytes) {
          MemoryBuffer buffer = MemoryUtils.wrap(bytes);
          long peerSchemaHash = buffer.readInt64();
          if (peerSchemaHash != schemaHash) {
            throw new ClassNotCompatibleException(
                String.format(
                    "Schema is not consistent, encoder schema is %s. "
                        + "self/peer schema hash are %s/%s. "
                        + "Please check writer schema.",
                    schema, schemaHash, peerSchemaHash));
          }
          BinaryRow row = new BinaryRow(schema);
          row.pointTo(buffer, buffer.readerIndex(), buffer.size());
          return fromRow(row);
        }

        @Override
        public byte[] encode(T obj) {
          buffer.writerIndex(0);
          buffer.writeInt64(schemaHash);
          writer.setBuffer(buffer);
          writer.reset();
          BinaryRow row = toRow(obj);
          return buffer.getBytes(0, 8 + row.getSizeInBytes());
        }
      };
    } catch (Exception e) {
      String msg = String.format("Create encoder failed, \nbeanClass: %s", beanClass);
      throw new EncoderException(msg, e);
    }
  }

  /**
   * Supported nested list format. For instance, nest collection can be expressed as Collection in
   * Collection. Input param must explicit specified type, like this: <code>
   * new TypeToken</code> instance with Collection in Collection type.
   *
   * @param token TypeToken instance which explicit specified the type.
   * @param <T> T is a array type, can be a nested list type.
   * @return
   */
  public static <T extends Collection> ArrayEncoder<T> arrayEncoder(TypeToken<T> token) {
    return arrayEncoder(token, (Fury) null);
  }

  public static <T extends Collection> ArrayEncoder<T> arrayEncoder(TypeToken<T> token, Fury fury) {
    Schema schema = TypeInference.inferSchema(token, false);
    Field field = DataTypes.fieldOfSchema(schema, 0);
    BinaryArrayWriter writer = new BinaryArrayWriter(field);

    Set<TypeToken<?>> set = new HashSet<>();
    findBeanToken(token, set);
    if (set.isEmpty()) {
      throw new IllegalArgumentException("can not find bean class.");
    }

    TypeToken<?> typeToken = null;
    for (TypeToken<?> tt : set) {
      typeToken = set.iterator().next();
      Encoders.loadOrGenRowCodecClass(getRawType(tt));
    }
    ArrayEncoder<T> encoder = arrayEncoder(token, typeToken, writer, fury);
    return new ArrayEncoder<T>() {

      @Override
      public Field field() {
        return encoder.field();
      }

      @Override
      public T fromArray(BinaryArray array) {
        return encoder.fromArray(array);
      }

      @Override
      public BinaryArray toArray(T obj) {
        return encoder.toArray(obj);
      }

      @Override
      public T decode(byte[] bytes) {
        return encoder.decode(bytes);
      }

      @Override
      public byte[] encode(T obj) {
        return encoder.encode(obj);
      }
    };
  }

  /**
   * The underlying implementation uses array, only supported {@link Collection} format, because
   * generic type such as List is erased to simply List, so a bean class input param is required.
   *
   * @return
   */
  public static <T extends Collection, B> ArrayEncoder<T> arrayEncoder(
      Class<? extends Collection> arrayCls, Class<B> elementType) {
    Preconditions.checkNotNull(elementType);

    return (ArrayEncoder<T>) arrayEncoder(TypeUtils.listOf(elementType), null);
  }

  /**
   * Creates an encoder for Java Bean of type T.
   *
   * <p>T must be publicly accessible.
   *
   * <p>supported types for java bean field: - primitive types: boolean, int, double, etc. - boxed
   * types: Boolean, Integer, Double, etc. - String - java.math.BigDecimal, java.math.BigInteger -
   * time related: java.sql.Date, java.sql.Timestamp, java.time.LocalDate, java.time.Instant -
   * collection types: only array and java.util.List currently, map support is in progress - nested
   * java bean.
   */
  public static <T extends Collection, B> ArrayEncoder<T> arrayEncoder(
      TypeToken<? extends Collection> arrayToken,
      TypeToken<B> elementType,
      BinaryArrayWriter writer,
      Fury fury) {
    Field field = writer.getField();
    try {
      Class<?> rowCodecClass = loadOrGenArrayCodecClass(arrayToken, elementType);
      Object references = new Object[] {field, writer, fury};
      GeneratedArrayEncoder codec =
          rowCodecClass
              .asSubclass(GeneratedArrayEncoder.class)
              .getConstructor(Object[].class)
              .newInstance(references);

      return new ArrayEncoder<T>() {

        @Override
        public Field field() {
          return field;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T fromArray(BinaryArray array) {
          return (T) codec.fromArray(array);
        }

        @Override
        public BinaryArray toArray(T obj) {
          return codec.toArray(obj);
        }

        @Override
        public T decode(byte[] bytes) {
          MemoryBuffer buffer = MemoryUtils.wrap(bytes);
          BinaryArray array = new BinaryArray(field);
          array.pointTo(buffer, buffer.readerIndex(), buffer.size());
          return fromArray(array);
        }

        @Override
        public byte[] encode(T obj) {
          writer.reset(obj.size());
          BinaryArray array = toArray(obj);
          return writer.getBuffer().getBytes(0, 8 + array.getSizeInBytes());
        }
      };
    } catch (Exception e) {
      String msg = String.format("Create encoder failed, \nelementType: %s", elementType);
      throw new EncoderException(msg, e);
    }
  }

  /**
   * Supported nested map format. For instance, nest map can be expressed as Map in Map. Input param
   * must explicit specified type, like this: <code>
   * new TypeToken</code> instance with Collection in Collection type.
   *
   * @param token TypeToken instance which explicit specified the type.
   * @param <T> T is a array type, can be a nested list type.
   * @return
   */
  public static <T extends Map> MapEncoder<T> mapEncoder(TypeToken<T> token) {
    return mapEncoder(token, (Fury) null);
  }

  /**
   * The underlying implementation uses array, only supported {@link Map} format, because generic
   * type such as List is erased to simply List, so a bean class input param is required.
   *
   * @return
   */
  public static <T extends Map, K, V> MapEncoder<T> mapEncoder(
      Class<? extends Map> mapCls, Class<K> keyType, Class<V> valueType) {
    Preconditions.checkNotNull(keyType);
    Preconditions.checkNotNull(valueType);

    return (MapEncoder<T>) mapEncoder(TypeUtils.mapOf(keyType, valueType), null);
  }

  public static <T extends Map> MapEncoder<T> mapEncoder(TypeToken<T> token, Fury fury) {
    Preconditions.checkNotNull(token);

    Tuple2<TypeToken<?>, TypeToken<?>> tuple2 = TypeUtils.getMapKeyValueType(token);

    Set<TypeToken<?>> set1 = beanSet(tuple2.f0);
    Set<TypeToken<?>> set2 = beanSet(tuple2.f1);
    LOG.info("Find beans to load: {}, {}", set1, set2);

    if (set1.isEmpty() && set2.isEmpty()) {
      throw new IllegalArgumentException("can not find bean class.");
    }

    TypeToken<?> keyToken = token4BeanLoad(set1, tuple2.f0);
    TypeToken<?> valToken = token4BeanLoad(set2, tuple2.f1);

    MapEncoder<T> encoder = mapEncoder(token, keyToken, valToken, fury);
    return createMapEncoder(encoder);
  }

  /**
   * Creates an encoder for Java Bean of type T.
   *
   * <p>T must be publicly accessible.
   *
   * <p>supported types for java bean field: - primitive types: boolean, int, double, etc. - boxed
   * types: Boolean, Integer, Double, etc. - String - java.math.BigDecimal, java.math.BigInteger -
   * time related: java.sql.Date, java.sql.Timestamp, java.time.LocalDate, java.time.Instant -
   * collection types: only array and java.util.List currently, map support is in progress - nested
   * java bean.
   */
  public static <T extends Map, K, V> MapEncoder<T> mapEncoder(
      TypeToken<? extends Map> mapToken, TypeToken<K> keyToken, TypeToken<V> valToken, Fury fury) {
    Preconditions.checkNotNull(mapToken);
    Preconditions.checkNotNull(keyToken);
    Preconditions.checkNotNull(valToken);

    Schema schema = TypeInference.inferSchema(mapToken, false);
    Field field = DataTypes.fieldOfSchema(schema, 0);
    Field keyField = DataTypes.keyArrayFieldForMap(field);
    Field valField = DataTypes.itemArrayFieldForMap(field);
    BinaryArrayWriter keyWriter = new BinaryArrayWriter(keyField);
    BinaryArrayWriter valWriter = new BinaryArrayWriter(valField);
    try {
      Class<?> rowCodecClass = loadOrGenMapCodecClass(mapToken, keyToken, valToken);
      Object references = new Object[] {keyField, valField, keyWriter, valWriter, fury, field};
      GeneratedMapEncoder codec =
          rowCodecClass
              .asSubclass(GeneratedMapEncoder.class)
              .getConstructor(Object[].class)
              .newInstance(references);

      return new MapEncoder<T>() {
        @Override
        public Field keyField() {
          return keyField;
        }

        @Override
        public Field valueField() {
          return valField;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T fromMap(BinaryArray key, BinaryArray value) {
          return (T) codec.fromMap(key, value);
        }

        @Override
        public BinaryMap toMap(T obj) {
          return codec.toMap(obj);
        }

        @Override
        public T decode(byte[] bytes) {
          MemoryBuffer buffer = MemoryUtils.wrap(bytes);
          BinaryMap map = new BinaryMap(field);
          map.pointTo(buffer, 0, buffer.size());
          return fromMap(map);
        }

        @Override
        public byte[] encode(T obj) {
          BinaryMap map = toMap(obj);
          return map.getBuf().readBytes(map.getBuf().size());
        }
      };
    } catch (Exception e) {
      String msg =
          String.format("Create encoder failed, \nkeyType: %s, valueType: %s", keyToken, valToken);
      throw new EncoderException(msg, e);
    }
  }

  private static Set<TypeToken<?>> beanSet(TypeToken<?> token) {
    Set<TypeToken<?>> set = new HashSet<>();
    if (TypeUtils.isBean(token)) {
      set.add(token);
      return set;
    }
    findBeanToken(token, set);
    return set;
  }

  private static TypeToken<?> token4BeanLoad(Set<TypeToken<?>> set, TypeToken<?> init) {
    TypeToken<?> keyToken = init;
    for (TypeToken<?> tt : set) {
      keyToken = tt;
      Encoders.loadOrGenRowCodecClass(getRawType(tt));
      LOG.info("bean {} load finished", getRawType(tt));
    }
    return keyToken;
  }

  private static <T> MapEncoder<T> createMapEncoder(MapEncoder<T> encoder) {
    return new MapEncoder<T>() {

      @Override
      public Field keyField() {
        return encoder.keyField();
      }

      @Override
      public Field valueField() {
        return encoder.valueField();
      }

      @Override
      public T fromMap(BinaryArray key, BinaryArray value) {
        return encoder.fromMap(key, value);
      }

      @Override
      public BinaryMap toMap(T obj) {
        return encoder.toMap(obj);
      }

      @Override
      public T decode(byte[] bytes) {
        return encoder.decode(bytes);
      }

      @Override
      public byte[] encode(T obj) {
        return encoder.encode(obj);
      }
    };
  }

  private static void findBeanToken(TypeToken<?> typeToken, java.util.Set<TypeToken<?>> set) {
    while (TypeUtils.ITERABLE_TYPE.isSupertypeOf(typeToken)
        || TypeUtils.MAP_TYPE.isSupertypeOf(typeToken)) {
      if (TypeUtils.ITERABLE_TYPE.isSupertypeOf(typeToken)) {
        typeToken = TypeUtils.getElementType(typeToken);
        if (TypeUtils.isBean(typeToken)) {
          set.add(typeToken);
        }
      } else {
        Tuple2<TypeToken<?>, TypeToken<?>> tuple2 = TypeUtils.getMapKeyValueType(typeToken);
        if (TypeUtils.isBean(tuple2.f0)) {
          set.add(tuple2.f0);
        } else {
          typeToken = tuple2.f0;
          findBeanToken(tuple2.f0, set);
        }

        if (TypeUtils.isBean(tuple2.f1)) {
          set.add(tuple2.f1);
        } else {
          typeToken = tuple2.f1;
          findBeanToken(tuple2.f1, set);
        }
      }
    }
  }

  public static Class<?> loadOrGenRowCodecClass(Class<?> beanClass) {
    Set<Class<?>> classes = TypeUtils.listBeansRecursiveInclusive(beanClass);
    LOG.info("Create RowCodec for classes {}", classes);
    CompileUnit[] compileUnits =
        classes.stream()
            .map(
                cls -> {
                  RowEncoderBuilder codecBuilder = new RowEncoderBuilder(cls);
                  // use genCodeFunc to avoid gen code repeatedly
                  return new CompileUnit(
                      CodeGenerator.getPackage(cls),
                      codecBuilder.codecClassName(cls),
                      codecBuilder::genCode);
                })
            .toArray(CompileUnit[]::new);
    return loadCls(compileUnits);
  }

  private static <B> Class<?> loadOrGenArrayCodecClass(
      TypeToken<? extends Collection> arrayCls, TypeToken<B> elementType) {
    LOG.info("Create ArrayCodec for classes {}", elementType);
    Class<?> cls = getRawType(elementType);
    // class name prefix
    String prefix = TypeInference.inferTypeName(arrayCls);

    ArrayEncoderBuilder codecBuilder = new ArrayEncoderBuilder(arrayCls, elementType);
    CompileUnit compileUnit =
        new CompileUnit(
            CodeGenerator.getPackage(cls),
            codecBuilder.codecClassName(cls, prefix),
            codecBuilder::genCode);

    return loadCls(compileUnit);
  }

  private static <K, V> Class<?> loadOrGenMapCodecClass(
      TypeToken<? extends Map> mapCls, TypeToken<K> keyToken, TypeToken<V> valueToken) {
    LOG.info("Create MapCodec for classes {}, {}", keyToken, valueToken);
    boolean keyIsBean = TypeUtils.isBean(keyToken);
    boolean valIsBean = TypeUtils.isBean(valueToken);
    TypeToken<?> beanToken;
    Class<?> cls;
    if (keyIsBean) {
      cls = getRawType(keyToken);
      beanToken = keyToken;
    } else if (valIsBean) {
      cls = getRawType(valueToken);
      beanToken = valueToken;
    } else {
      throw new IllegalArgumentException("not find bean class.");
    }
    // class name prefix
    String prefix = TypeInference.inferTypeName(mapCls);

    MapEncoderBuilder codecBuilder = new MapEncoderBuilder(mapCls, beanToken);
    CompileUnit compileUnit =
        new CompileUnit(
            CodeGenerator.getPackage(cls),
            codecBuilder.codecClassName(cls, prefix),
            codecBuilder::genCode);

    return loadCls(compileUnit);
  }

  private static Class<?> loadCls(CompileUnit... compileUnit) {
    CodeGenerator codeGenerator =
        CodeGenerator.getSharedCodeGenerator(Thread.currentThread().getContextClassLoader());
    ClassLoader classLoader = codeGenerator.compile(compileUnit);
    String className = compileUnit[0].getQualifiedClassName();
    try {
      return classLoader.loadClass(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Impossible because we just compiled class", e);
    }
  }
}
