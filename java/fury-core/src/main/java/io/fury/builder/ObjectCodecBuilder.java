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

package io.fury.builder;

import static io.fury.codegen.Expression.Invoke.inlineInvoke;
import static io.fury.type.TypeUtils.OBJECT_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_BOOLEAN_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_BYTE_ARRAY_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_BYTE_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_CHAR_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_DOUBLE_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_FLOAT_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_INT_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_LONG_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_SHORT_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_VOID_TYPE;
import static io.fury.type.TypeUtils.getRawType;
import static io.fury.type.TypeUtils.getSizeOfPrimitiveType;
import static io.fury.type.TypeUtils.isPrimitive;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import io.fury.Fury;
import io.fury.codegen.Expression;
import io.fury.codegen.Expression.Invoke;
import io.fury.codegen.Expression.ListExpression;
import io.fury.codegen.Expression.Literal;
import io.fury.codegen.Expression.Reference;
import io.fury.codegen.Expression.ReplaceStub;
import io.fury.codegen.Expression.StaticInvoke;
import io.fury.codegen.ExpressionUtils;
import io.fury.codegen.ExpressionVisitor;
import io.fury.memory.MemoryBuffer;
import io.fury.serializer.ObjectSerializer;
import io.fury.type.Descriptor;
import io.fury.type.DescriptorGrouper;
import io.fury.util.Functions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Generate sequential read/write code for java serialization to speed up performance. It also
 * reduces space overhead introduced by aligning. Codegen only for time-consuming field, others
 * delegate to fury.
 *
 * <p>In order to improve jit-compile and inline, serialization code should be spilt groups to avoid
 * huge/big methods.
 *
 * <p>With meta context share enabled and compatible mode, this serializer will take all non-inner
 * final types as non-final, so that fury can write class definition when write class info for those
 * types.
 *
 * @see ObjectCodecOptimizer for code stats and split heuristics.
 * @author chaokunyang
 */
@SuppressWarnings("UnstableApiUsage")
public class ObjectCodecBuilder extends BaseObjectCodecBuilder {
  public static final String BUFFER_NAME = "buffer";
  private final Literal classVersionHash;
  protected ObjectCodecOptimizer objectCodecOptimizer;

  public ObjectCodecBuilder(Class<?> beanClass, Fury fury) {
    super(TypeToken.of(beanClass), fury, Generated.GeneratedObjectSerializer.class);
    Collection<Descriptor> descriptors =
        fury.getClassResolver().getAllDescriptorsMap(beanClass, true).values();
    classVersionHash =
        new Literal(ObjectSerializer.computeVersionHash(descriptors), PRIMITIVE_INT_TYPE);
    DescriptorGrouper grouper =
        DescriptorGrouper.createDescriptorGrouper(descriptors, false, fury.compressNumber());
    objectCodecOptimizer =
        new ObjectCodecOptimizer(beanClass, grouper, !fury.isBasicTypesReferenceIgnored(), ctx);
  }

  protected ObjectCodecBuilder(TypeToken<?> beanType, Fury fury, Class<?> superSerializerClass) {
    super(beanType, fury, superSerializerClass);
    this.classVersionHash = null;
  }

  @Override
  protected String codecSuffix() {
    return "";
  }

  @Override
  protected void addCommonImports() {
    super.addCommonImports();
    ctx.addImport(Generated.GeneratedObjectSerializer.class);
  }

  /** Mark non-inner registered final types as non-final to write class def for those types. */
  @Override
  protected boolean isFinal(Class<?> clz) {
    return visitFury(f -> f.getClassResolver().isFinal(clz));
  }

  /**
   * Return an expression that serialize java bean of type {@link CodecBuilder#beanClass} to buffer.
   */
  @Override
  public Expression buildEncodeExpression() {
    Reference inputObject = new Reference(ROOT_OBJECT_NAME, OBJECT_TYPE, false);
    Reference buffer = new Reference(BUFFER_NAME, bufferTypeToken, false);

    ListExpression expressions = new ListExpression();
    Expression bean = tryCastIfPublic(inputObject, beanType, ctx.newName(beanClass));
    expressions.add(bean);
    if (fury.checkClassVersion()) {
      expressions.add(new Invoke(buffer, "writeInt", classVersionHash));
    }
    expressions.addAll(serializePrimitives(bean, buffer, objectCodecOptimizer.primitiveGroups));
    int numGroups = getNumGroups(objectCodecOptimizer);
    for (List<Descriptor> group :
        Iterables.concat(
            objectCodecOptimizer.boxedWriteGroups,
            objectCodecOptimizer.finalWriteGroups,
            objectCodecOptimizer.otherWriteGroups)) {
      if (group.isEmpty()) {
        continue;
      }
      boolean inline = group.size() == 1 && numGroups < 10;
      expressions.add(serializeGroup(group, bean, buffer, inline));
    }
    for (Descriptor descriptor :
        objectCodecOptimizer.descriptorGrouper.getCollectionDescriptors()) {
      expressions.add(serializeGroup(Collections.singletonList(descriptor), bean, buffer, false));
    }
    for (Descriptor d : objectCodecOptimizer.descriptorGrouper.getMapDescriptors()) {
      expressions.add(serializeGroup(Collections.singletonList(d), bean, buffer, false));
    }
    return expressions;
  }

  private int getNumGroups(ObjectCodecOptimizer objectCodecOptimizer) {
    return objectCodecOptimizer.boxedWriteGroups.size()
        + objectCodecOptimizer.finalWriteGroups.size()
        + objectCodecOptimizer.otherWriteGroups.size()
        + objectCodecOptimizer.descriptorGrouper.getCollectionDescriptors().size()
        + objectCodecOptimizer.descriptorGrouper.getMapDescriptors().size();
  }

  private Expression serializeGroup(
      List<Descriptor> group, Expression bean, Expression buffer, boolean inline) {
    Functions.SerializableSupplier<Expression> expressionSupplier =
        () -> {
          ListExpression groupExpressions = new ListExpression();
          for (Descriptor d : group) {
            // `bean` will be replaced by `Reference` to cut-off expr dependency.
            Expression fieldValue = getFieldValue(bean, d);
            Expression fieldExpr = serializeFor(fieldValue, buffer, d.getTypeToken());
            groupExpressions.add(fieldExpr);
          }
          return groupExpressions;
        };
    if (inline) {
      return expressionSupplier.get();
    }
    return objectCodecOptimizer.invokeGenerated(expressionSupplier, "writeFields");
  }

  /**
   * Return a list of expressions that serialize all primitive fields. This can reduce unnecessary
   * grow call and increment writerIndex in writeXXX.
   */
  private List<Expression> serializePrimitives(
      Expression bean, Expression buffer, List<List<Descriptor>> primitiveGroups) {
    int totalSize = getTotalSizeOfPrimitives(primitiveGroups);
    if (totalSize == 0) {
      return new ArrayList<>();
    }
    if (fury.compressNumber()) {
      return serializePrimitivesCompressed(bean, buffer, primitiveGroups, totalSize);
    } else {
      return serializePrimitivesUnCompressed(bean, buffer, primitiveGroups, totalSize);
    }
  }

  protected int getNumPrimitiveFields(List<List<Descriptor>> primitiveGroups) {
    return primitiveGroups.stream().mapToInt(List::size).sum();
  }

  private List<Expression> serializePrimitivesUnCompressed(
      Expression bean, Expression buffer, List<List<Descriptor>> primitiveGroups, int totalSize) {
    List<Expression> expressions = new ArrayList<>();
    int numPrimitiveFields = getNumPrimitiveFields(primitiveGroups);
    Literal totalSizeLiteral = new Literal(totalSize, PRIMITIVE_INT_TYPE);
    // After this grow, following writes can be unsafe without checks.
    expressions.add(new Invoke(buffer, "grow", totalSizeLiteral));
    // Must grow first, otherwise may get invalid address.
    Expression heapBuffer =
        new Invoke(buffer, "getHeapMemory", "heapBuffer", PRIMITIVE_BYTE_ARRAY_TYPE);
    Expression writerAddr =
        new Invoke(buffer, "getUnsafeWriterAddress", "writerAddr", PRIMITIVE_LONG_TYPE);
    expressions.add(heapBuffer);
    expressions.add(writerAddr);
    int acc = 0;
    for (List<Descriptor> group : primitiveGroups) {
      ListExpression groupExpressions = new ListExpression();
      // use Reference to cut-off expr dependency.
      for (Descriptor descriptor : group) {
        Class<?> clz = descriptor.getRawType();
        Preconditions.checkArgument(isPrimitive(clz));
        // `bean` will be replaced by `Reference` to cut-off expr dependency.
        Expression fieldValue = getFieldValue(bean, descriptor);
        if (clz == byte.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePut",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 1;
        } else if (clz == boolean.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutBoolean",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 1;
        } else if (clz == char.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutChar",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 2;
        } else if (clz == short.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutShort",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 2;
        } else if (clz == int.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutInt",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 4;
        } else if (clz == long.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutLong",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 8;
        } else if (clz == float.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutFloat",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 4;
        } else if (clz == double.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutDouble",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 8;
        } else {
          throw new IllegalStateException("impossible");
        }
      }
      if (numPrimitiveFields < 4) {
        expressions.add(groupExpressions);
      } else {
        expressions.add(
            objectCodecOptimizer.invokeGenerated(
                ImmutableSet.of(bean, heapBuffer, writerAddr), groupExpressions, "writeFields"));
      }
    }
    Expression increaseWriterIndex =
        new Invoke(
            buffer, "increaseWriterIndexUnsafe", new Literal(totalSizeLiteral, PRIMITIVE_INT_TYPE));
    expressions.add(increaseWriterIndex);
    return expressions;
  }

  private List<Expression> serializePrimitivesCompressed(
      Expression bean, Expression buffer, List<List<Descriptor>> primitiveGroups, int totalSize) {
    List<Expression> expressions = new ArrayList<>();
    int numPrimitiveFields = getNumPrimitiveFields(primitiveGroups);
    int growSize = (int) (totalSize + primitiveGroups.stream().mapToLong(Collection::size).sum());
    // After this grow, following writes can be unsafe without checks.
    expressions.add(new Invoke(buffer, "grow", Literal.ofInt(growSize)));
    // Must grow first, otherwise may get invalid address.
    Expression heapBuffer =
        new Invoke(buffer, "getHeapMemory", "heapBuffer", PRIMITIVE_BYTE_ARRAY_TYPE);
    expressions.add(heapBuffer);
    for (List<Descriptor> group : primitiveGroups) {
      ListExpression groupExpressions = new ListExpression();
      Expression writerAddr =
          new Invoke(buffer, "getUnsafeWriterAddress", "writerAddr", PRIMITIVE_LONG_TYPE);
      // use Reference to cut-off expr dependency.
      int acc = 0;
      boolean compressStarted = false;
      for (Descriptor descriptor : group) {
        Class<?> clz = descriptor.getRawType();
        Preconditions.checkArgument(isPrimitive(clz));
        // `bean` will be replaced by `Reference` to cut-off expr dependency.
        Expression fieldValue = getFieldValue(bean, descriptor);
        if (clz == byte.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePut",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 1;
        } else if (clz == boolean.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutBoolean",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 1;
        } else if (clz == char.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutChar",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 2;
        } else if (clz == short.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutShort",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 2;
        } else if (clz == float.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutFloat",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 4;
        } else if (clz == double.class) {
          groupExpressions.add(
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafePutDouble",
                  heapBuffer,
                  getWriterAddress(writerAddr, acc),
                  fieldValue));
          acc += 8;
        } else if (clz == int.class) {
          if (!compressStarted) {
            // int/long are sorted in the last.
            addIncWriterIndexExpr(groupExpressions, buffer, acc);
            compressStarted = true;
          }
          groupExpressions.add(new Invoke(buffer, "unsafeWriteVarInt", fieldValue));
          acc += 0;
        } else if (clz == long.class) {
          if (!compressStarted) {
            // int/long are sorted in the last.
            addIncWriterIndexExpr(groupExpressions, buffer, acc);
            compressStarted = true;
          }
          groupExpressions.add(new Invoke(buffer, "unsafeWriteVarLong", fieldValue));
        } else {
          throw new IllegalStateException("impossible");
        }
      }
      if (!compressStarted) {
        // int/long are sorted in the last.
        addIncWriterIndexExpr(groupExpressions, buffer, acc);
      }
      if (numPrimitiveFields < 4) {
        expressions.add(groupExpressions);
      } else {
        expressions.add(
            objectCodecOptimizer.invokeGenerated(
                ImmutableSet.of(bean, buffer, heapBuffer), groupExpressions, "writeFields"));
      }
    }
    return expressions;
  }

  private void addIncWriterIndexExpr(ListExpression expressions, Expression buffer, int diff) {
    if (diff != 0) {
      expressions.add(new Invoke(buffer, "increaseWriterIndexUnsafe", Literal.ofInt(diff)));
    }
  }

  private int getTotalSizeOfPrimitives(List<List<Descriptor>> primitiveGroups) {
    return primitiveGroups.stream()
        .flatMap(Collection::stream)
        .mapToInt(d -> getSizeOfPrimitiveType(d.getRawType()))
        .sum();
  }

  private Expression getWriterAddress(Expression writerPos, long acc) {
    if (acc == 0) {
      return writerPos;
    }
    return ExpressionUtils.add(writerPos, new Literal(acc, PRIMITIVE_LONG_TYPE));
  }

  public Expression buildDecodeExpression() {
    Reference buffer = new Reference(BUFFER_NAME, bufferTypeToken, false);
    ListExpression expressions = new ListExpression();
    if (fury.checkClassVersion()) {
      expressions.add(checkClassVersion(buffer));
    }
    Expression bean = newBean();
    Expression referenceObject = new Invoke(refResolverRef, "reference", PRIMITIVE_VOID_TYPE, bean);
    expressions.add(bean);
    expressions.add(referenceObject);
    expressions.addAll(deserializePrimitives(bean, buffer, objectCodecOptimizer.primitiveGroups));
    int numGroups = getNumGroups(objectCodecOptimizer);
    for (List<Descriptor> group :
        Iterables.concat(
            objectCodecOptimizer.boxedReadGroups,
            objectCodecOptimizer.finalReadGroups,
            objectCodecOptimizer.otherReadGroups)) {
      if (group.isEmpty()) {
        continue;
      }
      boolean inline = group.size() == 1 && numGroups < 10;
      expressions.add(deserializeGroup(group, bean, buffer, inline));
    }
    for (Descriptor d : objectCodecOptimizer.descriptorGrouper.getCollectionDescriptors()) {
      expressions.add(deserializeGroup(Collections.singletonList(d), bean, buffer, false));
    }
    for (Descriptor d : objectCodecOptimizer.descriptorGrouper.getMapDescriptors()) {
      expressions.add(deserializeGroup(Collections.singletonList(d), bean, buffer, false));
    }
    expressions.add(new Expression.Return(bean));
    return expressions;
  }

  protected Expression deserializeGroup(
      List<Descriptor> group, Expression bean, Expression buffer, boolean inline) {
    Functions.SerializableSupplier<Expression> exprSupplier =
        () -> {
          ListExpression groupExpressions = new ListExpression();
          // use Reference to cut-off expr dependency.
          for (Descriptor d : group) {
            ExpressionVisitor.ExprHolder exprHolder = ExpressionVisitor.ExprHolder.of("bean", bean);
            Expression action =
                deserializeFor(
                    buffer,
                    d.getTypeToken(),
                    // `bean` will be replaced by `Reference` to cut-off expr
                    // dependency.
                    expr ->
                        setFieldValue(
                            exprHolder.get("bean"), d, tryInlineCast(expr, d.getTypeToken())));
            groupExpressions.add(action);
          }
          return groupExpressions;
        };
    if (inline) {
      return exprSupplier.get();
    } else {
      return objectCodecOptimizer.invokeGenerated(exprSupplier, "readFields");
    }
  }

  private Expression checkClassVersion(Expression buffer) {
    return new StaticInvoke(
        ObjectSerializer.class,
        "checkClassVersion",
        PRIMITIVE_VOID_TYPE,
        false,
        furyRef,
        inlineInvoke(buffer, "readInt", PRIMITIVE_INT_TYPE),
        Objects.requireNonNull(classVersionHash));
  }

  /**
   * Return a list of expressions that deserialize all primitive fields. This can reduce unnecessary
   * check call and increment readerIndex in writeXXX.
   */
  private List<Expression> deserializePrimitives(
      Expression bean, Expression buffer, List<List<Descriptor>> primitiveGroups) {
    int totalSize = getTotalSizeOfPrimitives(primitiveGroups);
    if (totalSize == 0) {
      return new ArrayList<>();
    }
    if (fury.compressNumber()) {
      return deserializeCompressedPrimitives(bean, buffer, primitiveGroups);
    } else {
      return deserializeUnCompressedPrimitives(bean, buffer, primitiveGroups, totalSize);
    }
  }

  private List<Expression> deserializeUnCompressedPrimitives(
      Expression bean, Expression buffer, List<List<Descriptor>> primitiveGroups, int totalSize) {
    List<Expression> expressions = new ArrayList<>();
    int numPrimitiveFields = getNumPrimitiveFields(primitiveGroups);
    Literal totalSizeLiteral = Literal.ofInt(totalSize);
    Expression heapBuffer =
        new Invoke(buffer, "getHeapMemory", "heapBuffer", PRIMITIVE_BYTE_ARRAY_TYPE);
    Expression readerAddr =
        new Invoke(buffer, "getUnsafeReaderAddress", "readerAddr", PRIMITIVE_LONG_TYPE);
    expressions.add(heapBuffer);
    expressions.add(readerAddr);
    // After this check, following read can be totally unsafe without checks
    expressions.add(new Invoke(buffer, "checkReadableBytes", totalSizeLiteral));
    int acc = 0;
    for (List<Descriptor> group : primitiveGroups) {
      ListExpression groupExpressions = new ListExpression();
      for (Descriptor descriptor : group) {
        TypeToken<?> type = descriptor.getTypeToken();
        Class<?> clz = getRawType(type);
        Preconditions.checkArgument(isPrimitive(clz));
        Expression fieldValue;
        if (clz == byte.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGet",
                  PRIMITIVE_BYTE_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 1;
        } else if (clz == boolean.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetBoolean",
                  PRIMITIVE_BOOLEAN_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 1;
        } else if (clz == char.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetChar",
                  PRIMITIVE_CHAR_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 2;
        } else if (clz == short.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetShort",
                  PRIMITIVE_SHORT_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 2;
        } else if (clz == int.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetInt",
                  PRIMITIVE_INT_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 4;
        } else if (clz == long.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetLong",
                  PRIMITIVE_LONG_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 8;
        } else if (clz == float.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetFloat",
                  PRIMITIVE_FLOAT_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 4;
        } else if (clz == double.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetDouble",
                  PRIMITIVE_DOUBLE_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 8;
        } else {
          throw new IllegalStateException("impossible");
        }
        // `bean` will be replaced by `Reference` to cut-off expr dependency.
        groupExpressions.add(setFieldValue(bean, descriptor, fieldValue));
      }
      if (numPrimitiveFields < 4) {
        expressions.add(groupExpressions);
      } else {
        expressions.add(
            objectCodecOptimizer.invokeGenerated(
                ImmutableSet.of(bean, heapBuffer, readerAddr), groupExpressions, "readFields"));
      }
    }
    Expression increaseReaderIndex =
        new Invoke(
            buffer, "increaseReaderIndexUnsafe", new Literal(totalSizeLiteral, PRIMITIVE_INT_TYPE));
    expressions.add(increaseReaderIndex);
    return expressions;
  }

  private List<Expression> deserializeCompressedPrimitives(
      Expression bean, Expression buffer, List<List<Descriptor>> primitiveGroups) {
    List<Expression> expressions = new ArrayList<>();
    int numPrimitiveFields = getNumPrimitiveFields(primitiveGroups);
    Expression heapBuffer =
        new Invoke(buffer, "getHeapMemory", "heapBuffer", PRIMITIVE_BYTE_ARRAY_TYPE);
    expressions.add(heapBuffer);
    for (List<Descriptor> group : primitiveGroups) {
      ListExpression groupExpressions = new ListExpression();
      Expression readerAddr =
          new Invoke(buffer, "getUnsafeReaderAddress", "readerAddr", PRIMITIVE_LONG_TYPE);
      // After this check, following read can be totally unsafe without checks.
      ReplaceStub checkReadableBytesStub = new ReplaceStub();
      expressions.add(checkReadableBytesStub);
      int acc = 0;
      boolean compressStarted = false;
      for (Descriptor descriptor : group) {
        TypeToken<?> type = descriptor.getTypeToken();
        Class<?> clz = getRawType(type);
        Preconditions.checkArgument(isPrimitive(clz));
        Expression fieldValue;
        if (clz == byte.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGet",
                  PRIMITIVE_BYTE_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 1;
        } else if (clz == boolean.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetBoolean",
                  PRIMITIVE_BOOLEAN_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 1;
        } else if (clz == char.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetChar",
                  PRIMITIVE_CHAR_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 2;
        } else if (clz == short.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetShort",
                  PRIMITIVE_SHORT_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 2;
        } else if (clz == float.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetFloat",
                  PRIMITIVE_FLOAT_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 4;
        } else if (clz == double.class) {
          fieldValue =
              new StaticInvoke(
                  MemoryBuffer.class,
                  "unsafeGetDouble",
                  PRIMITIVE_DOUBLE_TYPE,
                  heapBuffer,
                  getReaderAddress(readerAddr, acc));
          acc += 8;
        } else if (clz == int.class) {
          if (!compressStarted) {
            compressStarted = true;
            addIncReaderIndexExpr(groupExpressions, buffer, acc);
          }
          fieldValue = new Invoke(buffer, "readVarInt", PRIMITIVE_INT_TYPE);
        } else if (clz == long.class) {
          if (!compressStarted) {
            compressStarted = true;
            addIncReaderIndexExpr(groupExpressions, buffer, acc);
          }
          fieldValue = new Invoke(buffer, "readVarLong", PRIMITIVE_LONG_TYPE);
        } else {
          throw new IllegalStateException("impossible");
        }
        // `bean` will be replaced by `Reference` to cut-off expr dependency.
        groupExpressions.add(setFieldValue(bean, descriptor, fieldValue));
      }
      if (acc != 0) {
        checkReadableBytesStub.setTargetObject(
            new Invoke(buffer, "checkReadableBytes", Literal.ofInt(acc)));
      }
      if (!compressStarted) {
        addIncReaderIndexExpr(groupExpressions, buffer, acc);
      }
      if (numPrimitiveFields < 4) {
        expressions.add(groupExpressions);
      } else {
        expressions.add(
            objectCodecOptimizer.invokeGenerated(
                ImmutableSet.of(bean, buffer, heapBuffer), groupExpressions, "readFields"));
      }
    }
    return expressions;
  }

  private void addIncReaderIndexExpr(ListExpression expressions, Expression buffer, int diff) {
    if (diff != 0) {
      expressions.add(new Invoke(buffer, "increaseReaderIndexUnsafe", Literal.ofInt(diff)));
    }
  }

  private Expression getReaderAddress(Expression readerPos, long acc) {
    if (acc == 0) {
      return readerPos;
    }
    return ExpressionUtils.add(readerPos, new Literal(acc, PRIMITIVE_LONG_TYPE));
  }
}
