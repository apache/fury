/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.format.encoder;

import io.fury.Fury;
import io.fury.codegen.CodeGenerator;
import io.fury.codegen.CompileUnit;
import io.fury.exception.ClassNotCompatibleException;
import io.fury.format.row.binary.BinaryRow;
import io.fury.format.row.binary.writer.BinaryRowWriter;
import io.fury.format.type.DataTypes;
import io.fury.format.type.TypeInference;
import io.fury.memory.MemoryBuffer;
import io.fury.memory.MemoryUtils;
import io.fury.type.TypeUtils;
import io.fury.util.LoggerFactory;
import java.util.Set;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;

/**
 * Factory to create {@link Encoder}.
 *
 * @author chaokunyang
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
          long peerSchemaHash = buffer.readLong();
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
          buffer.writeLong(schemaHash);
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

  public static Class<?> loadOrGenRowCodecClass(Class<?> beanClass) {
    Set<Class<?>> classes = TypeUtils.listBeansRecursiveInclusive(beanClass);
    LOG.debug("Create RowCodec for classes {}", classes);
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
    CodeGenerator codeGenerator =
        CodeGenerator.getSharedCodeGenerator(Thread.currentThread().getContextClassLoader());
    ClassLoader classLoader = codeGenerator.compile(compileUnits);
    String className = compileUnits[0].getQualifiedClassName();
    try {
      return classLoader.loadClass(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Impossible because we just compiled class", e);
    }
  }
}
