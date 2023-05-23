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

import static io.fury.type.TypeUtils.getRawType;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.fury.builder.CodecBuilder;
import io.fury.codegen.CodeGenerator;
import io.fury.codegen.CodegenContext;
import io.fury.codegen.Expression;
import io.fury.codegen.Expression.NewInstance;
import io.fury.codegen.Expression.Reference;
import io.fury.codegen.ExpressionUtils;
import io.fury.format.row.ArrayData;
import io.fury.format.row.Getters;
import io.fury.format.row.MapData;
import io.fury.format.row.Row;
import io.fury.format.row.binary.BinaryArray;
import io.fury.format.row.binary.BinaryRow;
import io.fury.format.row.binary.BinaryUtils;
import io.fury.format.row.binary.writer.BinaryArrayWriter;
import io.fury.format.row.binary.writer.BinaryRowWriter;
import io.fury.format.row.binary.writer.BinaryWriter;
import io.fury.format.type.DataTypes;
import io.fury.memory.MemoryBuffer;
import io.fury.type.TypeUtils;
import io.fury.util.DateTimeUtils;
import io.fury.util.ReflectionUtils;
import io.fury.util.StringUtils;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Base encoder builder for {@link Row}, {@link ArrayData} and {@link MapData}.
 *
 * @author chaokunyang
 */
@SuppressWarnings("UnstableApiUsage")
public abstract class BaseBinaryEncoderBuilder extends CodecBuilder {
  protected static final String REFERENCES_NAME = "references";
  protected static final TypeToken<Schema> SCHEMA_TYPE = TypeToken.of(Schema.class);
  protected static final TypeToken<Field> ARROW_FIELD_TYPE = TypeToken.of(Field.class);
  protected static TypeToken<Schema> schemaTypeToken = TypeToken.of(Schema.class);
  protected static TypeToken<BinaryWriter> writerTypeToken = TypeToken.of(BinaryWriter.class);
  protected static TypeToken<BinaryRowWriter> rowWriterTypeToken =
      TypeToken.of(BinaryRowWriter.class);
  protected static TypeToken<BinaryArrayWriter> arrayWriterTypeToken =
      TypeToken.of(BinaryArrayWriter.class);
  protected static TypeToken<Row> rowTypeToken = TypeToken.of(Row.class);
  protected static TypeToken<BinaryRow> binaryRowTypeToken = TypeToken.of(BinaryRow.class);
  protected static TypeToken<BinaryArray> binaryArrayTypeToken = TypeToken.of(BinaryArray.class);

  protected final Map<TypeToken<?>, Reference> arrayWriterMap = new HashMap<>();
  protected final Map<TypeToken<?>, Reference> beanEncoderMap = new HashMap<>();
  // We need to call beanEncoder's rowWriter.reset() before write a corresponding nested bean every
  // time.
  // Outermost beanEncoder's rowWriter.reset() should be called outside generated code before
  // writer an outermost bean every time.
  protected final Map<TypeToken<?>, Reference> rowWriterMap = new HashMap<>();

  public BaseBinaryEncoderBuilder(CodegenContext context, Class<?> beanClass) {
    this(context, TypeToken.of(beanClass));
  }

  public BaseBinaryEncoderBuilder(CodegenContext context, TypeToken<?> beanType) {
    super(context, beanType);
    ctx.reserveName(REFERENCES_NAME);

    ctx.addImport(BinaryRow.class.getPackage().getName() + ".*");
    ctx.addImport(BinaryWriter.class.getPackage().getName() + ".*");
    ctx.addImport(Schema.class.getPackage().getName() + ".*");
  }

  public String codecClassName(Class<?> beanClass) {
    return codecClassName(beanClass, "");
  }

  public String codecClassName(Class<?> beanClass, String prefix) {
    String name =
        ReflectionUtils.getClassNameWithoutPackage(beanClass)
            + prefix
            + codecSuffix()
            + CodeGenerator.getClassUniqueId(beanClass);
    return name.replace("$", "_");
  }

  protected String codecSuffix() {
    return "RowCodec";
  }

  public String codecQualifiedClassName(Class<?> beanClass) {
    return CodeGenerator.getPackage(beanClass) + "." + codecClassName(beanClass);
  }

  public String codecQualifiedClassName(Class<?> beanClass, String prefix) {
    return CodeGenerator.getPackage(beanClass) + "." + codecClassName(beanClass, prefix);
  }

  /**
   * Return an expression for serializing an object of given type to row format representation. The
   * inputObject will be written to position <code>ordinal</code> of row/array using given <code>
   * writer</code>
   */
  protected Expression serializeFor(
      Expression ordinal,
      Expression inputObject,
      Expression writer,
      TypeToken<?> typeToken,
      Expression arrowField) {
    Class<?> rawType = getRawType(typeToken);
    if (TypeUtils.isPrimitive(rawType)) {
      return new Expression.ListExpression(
          // notNull is by default, no need to call setNotNullAt
          new Expression.Invoke(writer, "write", ordinal, inputObject));
    } else if (TypeUtils.isBoxed(rawType)) {
      // janino support autoboxing and unboxing, so we don't need to call intValue/longValue....
      return setValueOrNull(writer, ordinal, inputObject, inputObject);
    } else if (rawType == BigDecimal.class) {
      return setValueOrNull(writer, ordinal, inputObject, inputObject);
    } else if (rawType == java.math.BigInteger.class) {
      Expression.Invoke value =
          new Expression.Invoke(inputObject, "toByteArray", TypeToken.of(byte[].class));
      return setValueOrNull(writer, ordinal, inputObject, value);
    } else if (rawType == java.time.LocalDate.class) {
      Expression.StaticInvoke value =
          new Expression.StaticInvoke(
              DateTimeUtils.class,
              "localDateToDays",
              TypeUtils.PRIMITIVE_INT_TYPE,
              false,
              inputObject);
      return setValueOrNull(writer, ordinal, inputObject, value);
    } else if (rawType == java.sql.Date.class) {
      Expression.StaticInvoke value =
          new Expression.StaticInvoke(
              DateTimeUtils.class,
              "fromJavaDate",
              TypeUtils.PRIMITIVE_INT_TYPE,
              false,
              inputObject);
      return setValueOrNull(writer, ordinal, inputObject, value);
    } else if (rawType == java.sql.Timestamp.class) {
      Expression.StaticInvoke value =
          new Expression.StaticInvoke(
              DateTimeUtils.class,
              "fromJavaTimestamp",
              TypeUtils.PRIMITIVE_LONG_TYPE,
              false,
              inputObject);
      return setValueOrNull(writer, ordinal, inputObject, value);
    } else if (rawType == java.time.Instant.class) {
      Expression.StaticInvoke value =
          new Expression.StaticInvoke(
              DateTimeUtils.class,
              "instantToMicros",
              TypeUtils.PRIMITIVE_LONG_TYPE,
              false,
              inputObject);
      return setValueOrNull(writer, ordinal, inputObject, value);
    } else if (rawType == String.class) {
      return setValueOrNull(writer, ordinal, inputObject, inputObject);
    } else if (rawType.isEnum()) {
      Expression.Invoke value = new Expression.Invoke(inputObject, "name", TypeUtils.STRING_TYPE);
      return setValueOrNull(writer, ordinal, inputObject, value);
    } else if (rawType.isArray() || TypeUtils.ITERABLE_TYPE.isSupertypeOf(typeToken)) {
      // place outer writer operations here, because map key/value arrays need to call
      // serializeForArray,
      // but don't setOffsetAndSize for array.
      Expression.Invoke offset =
          new Expression.Invoke(writer, "writerIndex", "writerIndex", TypeUtils.PRIMITIVE_INT_TYPE);
      Expression serializeArray = serializeForArray(inputObject, writer, typeToken, arrowField);
      Expression.Arithmetic size =
          ExpressionUtils.subtract(
              new Expression.Invoke(
                  writer, "writerIndex", "writerIndex", TypeUtils.PRIMITIVE_INT_TYPE),
              offset);
      Expression.Invoke setOffsetAndSize =
          new Expression.Invoke(writer, "setOffsetAndSize", ordinal, offset, size);
      Expression.ListExpression expression =
          new Expression.ListExpression(offset, serializeArray, size, setOffsetAndSize);
      return new Expression.If(
          ExpressionUtils.eqNull(inputObject),
          new Expression.Invoke(writer, "setNullAt", ordinal),
          expression);
    } else if (TypeUtils.MAP_TYPE.isSupertypeOf(typeToken)) {
      return serializeForMap(ordinal, writer, inputObject, typeToken, arrowField);
    } else if (TypeUtils.isBean(rawType)) {
      return serializeForBean(ordinal, writer, inputObject, typeToken, arrowField);
    } else {
      return serializeForObject(ordinal, writer, inputObject);
    }
  }

  /**
   * Returns an expression to write iterable <code>inputObject</code> of type <code>typeToken</code>
   * as {@link BinaryArray} using given <code>writer</code>.
   */
  protected Expression serializeForArray(
      Expression inputObject, Expression writer, TypeToken<?> typeToken, Expression arrowField) {
    return serializeForArray(inputObject, writer, typeToken, arrowField, false);
  }

  protected Expression serializeForArray(
      Expression inputObject,
      Expression writer,
      TypeToken<?> typeToken,
      Expression arrowField,
      boolean reuse) {
    Reference arrayWriter = getOrCreateArrayWriter(typeToken, arrowField, writer, reuse);
    Expression.StaticInvoke arrayElementField =
        new Expression.StaticInvoke(
            DataTypes.class, "arrayElementField", "elemField", ARROW_FIELD_TYPE, false, arrowField);
    Class<?> rawType = getRawType(typeToken);
    if (rawType.isArray()) {
      Expression.FieldValue length =
          new Expression.FieldValue(inputObject, "length", TypeUtils.PRIMITIVE_INT_TYPE);
      Expression.Invoke reset = new Expression.Invoke(arrayWriter, "reset", length);
      if (rawType.getComponentType().isPrimitive()) {
        return new Expression.ListExpression(
            reset,
            new Expression.Invoke(arrayWriter, "fromPrimitiveArray", inputObject),
            arrayWriter);
      } else {
        Expression.ForEach forEach =
            new Expression.ForEach(
                inputObject,
                (i, value) ->
                    serializeFor(
                        i,
                        value,
                        arrayWriter,
                        Objects.requireNonNull(typeToken.getComponentType()),
                        arrayElementField));
        return new Expression.ListExpression(reset, forEach, arrayWriter);
      }
    } else if (getRawType(typeToken) == Iterable.class) {
      Expression.ListFromIterable listFromIterable = new Expression.ListFromIterable(inputObject);
      Expression.Invoke size =
          new Expression.Invoke(listFromIterable, "size", TypeUtils.PRIMITIVE_INT_TYPE);
      Expression.Invoke reset = new Expression.Invoke(arrayWriter, "reset", size);
      Expression.ForEach forEach =
          new Expression.ForEach(
              listFromIterable,
              (i, value) ->
                  serializeFor(
                      i,
                      value,
                      arrayWriter,
                      TypeUtils.getElementType(typeToken),
                      arrayElementField));
      return new Expression.ListExpression(reset, forEach, arrayWriter);
    } else { // collection
      Expression.Invoke size =
          new Expression.Invoke(inputObject, "size", TypeUtils.PRIMITIVE_INT_TYPE);
      Expression.Invoke reset = new Expression.Invoke(arrayWriter, "reset", size);
      Expression.ForEach forEach =
          new Expression.ForEach(
              inputObject,
              (i, value) ->
                  serializeFor(
                      i,
                      value,
                      arrayWriter,
                      TypeUtils.getElementType(typeToken),
                      arrayElementField));
      return new Expression.ListExpression(reset, forEach, arrayWriter);
    }
  }

  /**
   * Get or create an ArrayWriter for given <code>type</code> and use <code>writer</code> as parent
   * writer.
   */
  protected Reference getOrCreateArrayWriter(
      TypeToken<?> typeToken, Expression arrayDataType, Expression writer) {
    return getOrCreateArrayWriter(typeToken, arrayDataType, writer, false);
  }

  protected Reference getOrCreateArrayWriter(
      TypeToken<?> typeToken, Expression arrayDataType, Expression writer, boolean reuse) {
    if (reuse) {
      return (Reference) writer;
    }

    return arrayWriterMap.computeIfAbsent(
        typeToken,
        t -> {
          String name = ctx.newName("arrayWriter");
          ctx.addField(
              ctx.type(BinaryArrayWriter.class),
              name,
              new NewInstance(arrayWriterTypeToken, arrayDataType, writer));
          return new Reference(name, arrayWriterTypeToken, false);
        });
  }

  /**
   * Returns an expression to write map <code>inputObject</code> to position <code>ordinal</code> of
   * row/array using given <code>writer</code>.
   */
  protected Expression serializeForMap(
      Expression ordinal,
      Expression writer,
      Expression inputObject,
      TypeToken<?> typeToken,
      Expression arrowField) {
    Expression.StaticInvoke keyArrayField =
        new Expression.StaticInvoke(
            DataTypes.class,
            "keyArrayFieldForMap",
            "keyArrayField",
            ARROW_FIELD_TYPE,
            false,
            arrowField);
    Expression.StaticInvoke valueArrayField =
        new Expression.StaticInvoke(
            DataTypes.class,
            "itemArrayFieldForMap",
            "valueArrayField",
            ARROW_FIELD_TYPE,
            false,
            arrowField);

    @SuppressWarnings("unchecked")
    TypeToken<?> supertype = ((TypeToken<? extends Map<?, ?>>) typeToken).getSupertype(Map.class);
    TypeToken<?> keySetType = supertype.resolveType(TypeUtils.KEY_SET_RETURN_TYPE);
    TypeToken<?> valuesType = supertype.resolveType(TypeUtils.VALUES_RETURN_TYPE);

    Expression.Invoke keySet = new Expression.Invoke(inputObject, "keySet", keySetType);
    Expression keySerializationExpr = serializeForArray(keySet, writer, keySetType, keyArrayField);

    Expression.Invoke values = new Expression.Invoke(inputObject, "values", valuesType);
    Expression valueSerializationExpr =
        serializeForArray(values, writer, valuesType, valueArrayField);

    Expression.Invoke offset =
        new Expression.Invoke(writer, "writerIndex", "writerIndex", TypeUtils.PRIMITIVE_INT_TYPE);
    // preserve 8 bytes to write the key array numBytes later
    Expression.Invoke preserve =
        new Expression.Invoke(
            writer, "writeDirectly", new Expression.Literal(-1, TypeUtils.PRIMITIVE_INT_TYPE));
    Expression.Invoke writeKeyArrayNumBytes =
        new Expression.Invoke(
            writer,
            "writeDirectly",
            offset,
            new Expression.Invoke(keySerializationExpr, "size", TypeUtils.PRIMITIVE_INT_TYPE));
    Expression.Arithmetic size =
        ExpressionUtils.subtract(
            new Expression.Invoke(
                writer, "writerIndex", "writerIndex", TypeUtils.PRIMITIVE_INT_TYPE),
            offset);
    Expression.Invoke setOffsetAndSize =
        new Expression.Invoke(writer, "setOffsetAndSize", ordinal, offset, size);

    Expression.ListExpression expression =
        new Expression.ListExpression(
            offset,
            preserve,
            keySerializationExpr,
            writeKeyArrayNumBytes,
            valueSerializationExpr,
            setOffsetAndSize);

    return new Expression.If(
        ExpressionUtils.eqNull(inputObject),
        new Expression.Invoke(writer, "setNullAt", ordinal),
        expression);
  }

  /**
   * Returns an expression to write bean <code>inputObject</code> to position <code>ordinal</code>
   * of row/array using given <code>writer</code>.
   */
  protected Expression serializeForBean(
      Expression ordinal,
      Expression writer,
      Expression inputObject,
      TypeToken<?> typeToken,
      Expression structField) {
    Class<?> rawType = getRawType(typeToken);
    Reference rowWriter;
    Reference beanEncoder = beanEncoderMap.get(typeToken);
    if (beanEncoder == null) {
      // janino generics don't add cast, so this `<${type}>` is only for generated code readability
      Expression.StaticInvoke schema =
          new Expression.StaticInvoke(
              DataTypes.class, "schemaFromStructField", "schema", SCHEMA_TYPE, false, structField);
      String rowWriterName =
          ctx.newName(StringUtils.uncapitalize(rawType.getSimpleName() + "RowWriter"));
      NewInstance newRowWriter = new NewInstance(rowWriterTypeToken, schema, writer);
      ctx.addField(ctx.type(rowWriterTypeToken), rowWriterName, newRowWriter);

      Preconditions.checkArgument(!codecClassName(rawType).contains("."));
      String encoderName = ctx.newName(StringUtils.uncapitalize(codecClassName(rawType)));
      String encoderClass = codecQualifiedClassName(rawType);
      TypeToken<?> codecTypeToken = TypeToken.of(GeneratedRowEncoder.class);
      NewInstance newEncoder =
          new NewInstance(
              codecTypeToken,
              encoderClass,
              ExpressionUtils.newObjectArray(schema, newRowWriter, furyRef));
      ctx.addField(encoderClass, encoderName, newEncoder);

      rowWriter = new Reference(rowWriterName, rowWriterTypeToken);
      rowWriterMap.put(typeToken, rowWriter);
      beanEncoder = new Reference(encoderName, codecTypeToken);
      beanEncoderMap.put(typeToken, beanEncoder);
    }
    rowWriter = rowWriterMap.get(typeToken);

    Expression.Invoke reset = new Expression.Invoke(rowWriter, "reset");
    Expression.Invoke offset =
        new Expression.Invoke(writer, "writerIndex", "writerIndex", TypeUtils.PRIMITIVE_INT_TYPE);
    Expression.Invoke toRow = new Expression.Invoke(beanEncoder, "toRow", inputObject);
    Expression.Arithmetic size =
        ExpressionUtils.subtract(
            new Expression.Invoke(
                writer, "writerIndex", "writerIndex", TypeUtils.PRIMITIVE_INT_TYPE),
            offset);
    Expression.Invoke setOffsetAndSize =
        new Expression.Invoke(writer, "setOffsetAndSize", ordinal, offset, size);
    Expression.ListExpression expression =
        new Expression.ListExpression(
            offset,
            reset,
            toRow, // reset will change writerIndex. must call reset and toRow in pair.
            size,
            setOffsetAndSize);

    return new Expression.If(
        ExpressionUtils.eqNull(inputObject),
        new Expression.Invoke(writer, "setNullAt", ordinal),
        expression);
  }

  /**
   * Return an expression to serialize opaque <code>inputObject</code> as binary using <code>fury
   * </code>. When deserialization, using fury to deserialize sliced MemoryBuffer. See {@link
   * BinaryUtils#getElemAccessMethodName(TypeToken)}, {@link Getters#getBuffer(int)}
   */
  protected Expression serializeForObject(
      Expression ordinal, Expression writer, Expression inputObject) {
    Expression.Invoke offset =
        new Expression.Invoke(writer, "writerIndex", "writerIndex", TypeUtils.PRIMITIVE_INT_TYPE);
    Expression.Invoke buffer =
        new Expression.Invoke(writer, "getBuffer", "buffer", TypeToken.of(MemoryBuffer.class));
    Expression setWriterIndex = new Expression.Invoke(buffer, "writerIndex", offset);
    Expression.Invoke serialize = new Expression.Invoke(furyRef, "serialize", buffer, inputObject);
    Expression.Invoke newWriterIndex =
        new Expression.Invoke(buffer, "writerIndex", "writerIndex", TypeUtils.PRIMITIVE_INT_TYPE);
    Expression.Arithmetic size = ExpressionUtils.subtract(newWriterIndex, offset, "size");
    Expression increaseWriterIndexToAligned =
        new Expression.Invoke(writer, "increaseWriterIndexToAligned", size);
    Expression.Invoke setOffsetAndSize =
        new Expression.Invoke(writer, "setOffsetAndSize", ordinal, offset, size);
    Expression.ListExpression expression =
        new Expression.ListExpression(
            offset,
            buffer,
            setWriterIndex,
            serialize,
            increaseWriterIndexToAligned,
            setOffsetAndSize);
    return new Expression.If(
        ExpressionUtils.eqNull(inputObject),
        new Expression.Invoke(writer, "setNullAt", ordinal),
        expression);
  }

  protected Expression setValueOrNull(
      Expression writer, Expression ordinal, Expression inputObject, Expression value) {
    Expression action = new Expression.Invoke(writer, "write", ordinal, value);
    return new Expression.If(
        ExpressionUtils.eqNull(inputObject),
        new Expression.Invoke(writer, "setNullAt", ordinal),
        action);
  }

  /**
   * Returns an expression that deserialize <code>value</code> as a java object of type <code>
   * typeToken</code>.
   */
  protected Expression deserializeFor(Expression value, TypeToken<?> typeToken) {
    Class<?> rawType = getRawType(typeToken);
    if (TypeUtils.isPrimitive(rawType) || TypeUtils.isBoxed(rawType)) {
      return value;
    } else if (rawType == BigDecimal.class) {
      return value;
    } else if (rawType == java.math.BigInteger.class) {
      return new NewInstance(TypeUtils.BIG_INTEGER_TYPE, value);
    } else if (rawType == java.time.LocalDate.class) {
      return new Expression.StaticInvoke(
          DateTimeUtils.class, "daysToLocalDate", TypeUtils.LOCAL_DATE_TYPE, false, value);
    } else if (rawType == java.sql.Date.class) {
      return new Expression.StaticInvoke(
          DateTimeUtils.class, "toJavaDate", TypeUtils.DATE_TYPE, false, value);
    } else if (rawType == java.sql.Timestamp.class) {
      return new Expression.StaticInvoke(
          DateTimeUtils.class, "toJavaTimestamp", TypeUtils.TIMESTAMP_TYPE, false, value);
    } else if (rawType == java.time.Instant.class) {
      return new Expression.StaticInvoke(
          DateTimeUtils.class, "microsToInstant", TypeUtils.INSTANT_TYPE, false, value);
    } else if (rawType == String.class) {
      return value;
    } else if (rawType.isEnum()) {
      return ExpressionUtils.valueOf(typeToken, value);
    } else if (rawType.isArray()) {
      return deserializeForArray(value, typeToken);
    } else if (TypeUtils.ITERABLE_TYPE.isSupertypeOf(typeToken)) {
      return deserializeForCollection(value, typeToken);
    } else if (TypeUtils.MAP_TYPE.isSupertypeOf(typeToken)) {
      return deserializeForMap(value, typeToken);
    } else if (TypeUtils.isBean(rawType)) {
      return deserializeForBean(value, typeToken);
    } else {
      return deserializeForObject(value, typeToken);
    }
  }

  /**
   * Returns an expression that deserialize <code>row</code> as a java bean of type <code>typeToken
   * </code>.
   */
  protected Expression deserializeForBean(Expression row, TypeToken<?> typeToken) {
    Reference beanEncoder = beanEncoderMap.get(typeToken);
    if (beanEncoder == null) {
      throw new IllegalStateException("beanEncoder should have be added in serializeForBean()");
    }
    Expression.Invoke beanObj =
        new Expression.Invoke(beanEncoder, "fromRow", TypeUtils.OBJECT_TYPE, false, row);
    return new Expression.Cast(beanObj, typeToken, "bean");
  }

  /** Returns an expression that deserialize <code>mapData</code> as a java map. */
  protected Expression deserializeForMap(Expression mapData, TypeToken<?> typeToken) {
    Expression javaMap = newMap(typeToken);
    @SuppressWarnings("unchecked")
    TypeToken<?> supertype = ((TypeToken<? extends Map<?, ?>>) typeToken).getSupertype(Map.class);
    TypeToken<?> keySetType = supertype.resolveType(TypeUtils.KEY_SET_RETURN_TYPE);
    TypeToken<?> keysType = TypeUtils.getCollectionType(keySetType);
    TypeToken<?> valuesType = supertype.resolveType(TypeUtils.VALUES_RETURN_TYPE);
    Expression keyArray = new Expression.Invoke(mapData, "keyArray", binaryArrayTypeToken, false);
    Expression valueArray =
        new Expression.Invoke(mapData, "valueArray", binaryArrayTypeToken, false);
    Expression keyJavaArray;
    Expression valueJavaArray;
    if (TypeUtils.ITERABLE_TYPE.isSupertypeOf(keysType)) {
      keyJavaArray = deserializeForCollection(keyArray, keysType);
    } else {
      keyJavaArray = deserializeForArray(keyArray, keysType);
    }
    if (TypeUtils.ITERABLE_TYPE.isSupertypeOf(valuesType)) {
      valueJavaArray = deserializeForCollection(valueArray, valuesType);
    } else {
      valueJavaArray = deserializeForArray(valueArray, valuesType);
    }

    Expression.ZipForEach put =
        new Expression.ZipForEach(
            keyJavaArray,
            valueJavaArray,
            (i, key, value) ->
                new Expression.If(
                    ExpressionUtils.notNull(key),
                    new Expression.Invoke(javaMap, "put", key, value)));
    return new Expression.ListExpression(javaMap, put, javaMap);
  }

  /** Returns an expression that deserialize <code>arrayData</code> as a java collection. */
  protected Expression deserializeForCollection(Expression arrayData, TypeToken<?> typeToken) {
    Expression collection = newCollection(typeToken);
    try {
      TypeToken<?> elemType = TypeUtils.getElementType(typeToken);
      ArrayDataForEach addElemsOp =
          new ArrayDataForEach(
              arrayData,
              elemType,
              (i, value) ->
                  new Expression.Invoke(collection, "add", deserializeFor(value, elemType)),
              i -> new Expression.Invoke(collection, "add", ExpressionUtils.nullValue(elemType)));
      return new Expression.ListExpression(collection, addElemsOp, collection);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a java collection. Create a {@link ArrayList} if <code>typeToken</code> is super class
   * of {@link ArrayList}; Create a {@link HashSet} if <code>typeToken</code> is super class of
   * {@link HashSet}; Create an instance of <code>typeToken</code> otherwise.
   */
  protected Expression newCollection(TypeToken<?> typeToken) {
    Class<?> clazz = getRawType(typeToken);
    Expression collection;
    if (TypeToken.of(clazz).isSupertypeOf(TypeToken.of(ArrayList.class))) {
      collection = new NewInstance(TypeToken.of(ArrayList.class));
    } else if (TypeToken.of(clazz).isSupertypeOf(TypeToken.of(HashSet.class))) {
      collection = new NewInstance(TypeToken.of(HashSet.class));
    } else {
      if (ReflectionUtils.isAbstract(clazz) || clazz.isInterface()) {
        String msg = String.format("class %s can't be abstract or interface", clazz);
        throw new UnsupportedOperationException(msg);
      }
      collection = new NewInstance(typeToken);
    }
    return collection;
  }

  /**
   * Create a java map. Create a {@link HashMap} if <code>typeToken</code> is super class of
   * HashMap; Create an instance of <code>typeToken</code> otherwise.
   */
  protected Expression newMap(TypeToken<?> typeToken) {
    Class<?> clazz = getRawType(typeToken);
    Expression javaMap;
    // use TypeToken.of(clazz) rather typeToken to strip generics.
    if (TypeToken.of(clazz).isSupertypeOf(TypeToken.of(HashMap.class))) {
      javaMap = new NewInstance(TypeToken.of(HashMap.class));
    } else {
      if (ReflectionUtils.isAbstract(clazz) || clazz.isInterface()) {
        String msg = String.format("class %s can't be abstract or interface", clazz);
        throw new UnsupportedOperationException(msg);
      }
      javaMap = new NewInstance(typeToken);
    }
    return javaMap;
  }

  /**
   * Returns an expression to deserialize multi-array from <code>arrayData</code>, and set value to
   * <code>rootJavaArray</code>.
   */
  protected Expression deserializeForMultiDimensionArray(
      Expression arrayData,
      Expression rootJavaArray,
      int numDimensions,
      TypeToken<?> typeToken,
      Expression[] indexes) {
    Preconditions.checkArgument(numDimensions > 1);
    Preconditions.checkArgument(typeToken.isArray());
    TypeToken<?> elemType = typeToken.getComponentType();
    if (numDimensions == 2) {
      return new ArrayDataForEach(
          arrayData,
          elemType,
          (i, value) -> {
            Expression[] newIndexes = Arrays.copyOf(indexes, indexes.length + 1);
            newIndexes[indexes.length] = i;
            Expression elemArr =
                deserializeForArray(value, Objects.requireNonNull(typeToken.getComponentType()));
            Expression.AssignArrayElem assign =
                new Expression.AssignArrayElem(rootJavaArray, elemArr, newIndexes);
            return assign;
          });
    } else {
      return new ArrayDataForEach(
          arrayData,
          elemType,
          (i, value) -> {
            Expression[] newIndexes = Arrays.copyOf(indexes, indexes.length + 1);
            newIndexes[indexes.length] = i;
            return deserializeForMultiDimensionArray(
                value, rootJavaArray, numDimensions - 1, elemType, newIndexes);
          });
    }
  }

  /**
   * Return an expression that deserialize <code>arrayData</code>. If array is multi-array, forward
   * to {@link BaseBinaryEncoderBuilder#deserializeForMultiDimensionArray}
   */
  protected Expression deserializeForArray(Expression arrayData, TypeToken<?> typeToken) {
    int numDimensions = TypeUtils.getArrayDimensions(typeToken);
    if (numDimensions > 1) {
      // If some dimension's elements is all null, we take outer-most array as null,
      // and don't create multi-array. return an no-ops expression or return null.
      Expression.StaticInvoke dimensions =
          new Expression.StaticInvoke(
              BinaryArray.class,
              "getDimensions",
              "dims",
              TypeToken.of(int[].class),
              true,
              arrayData,
              new Expression.Literal(numDimensions, TypeUtils.INT_TYPE));
      TypeToken<?> innerElemType = TypeUtils.getMultiDimensionArrayElementType(typeToken);
      Class<?> innerElemClass = getRawType(innerElemType);
      Expression rootJavaMultiDimArray =
          new Expression.NewArray(innerElemClass, numDimensions, dimensions);
      Expression op =
          deserializeForMultiDimensionArray(
              arrayData, rootJavaMultiDimArray, numDimensions, typeToken, new Expression[0]);
      // although the value maybe null, we don't use this info, so we set nullability to false.
      return new Expression.If(
          ExpressionUtils.notNull(dimensions),
          new Expression.ListExpression(rootJavaMultiDimArray, op, rootJavaMultiDimArray),
          ExpressionUtils.nullValue(rootJavaMultiDimArray.type()),
          false);
    } else {
      TypeToken<?> elemType = typeToken.getComponentType();
      Class<?> innerElemClass = getRawType(Objects.requireNonNull(elemType));
      if (byte.class == innerElemClass) {
        return new Expression.Invoke(arrayData, "toByteArray", TypeUtils.PRIMITIVE_BYTE_ARRAY_TYPE);
      } else if (boolean.class == innerElemClass) {
        return new Expression.Invoke(
            arrayData, "toBooleanArray", TypeUtils.PRIMITIVE_BOOLEAN_ARRAY_TYPE);
      } else if (short.class == innerElemClass) {
        return new Expression.Invoke(
            arrayData, "toShortArray", TypeUtils.PRIMITIVE_SHORT_ARRAY_TYPE);
      } else if (int.class == innerElemClass) {
        return new Expression.Invoke(arrayData, "toIntArray", TypeUtils.PRIMITIVE_INT_ARRAY_TYPE);
      } else if (long.class == innerElemClass) {
        return new Expression.Invoke(arrayData, "toLongArray", TypeUtils.PRIMITIVE_LONG_ARRAY_TYPE);
      } else if (float.class == innerElemClass) {
        return new Expression.Invoke(
            arrayData, "toFloatArray", TypeUtils.PRIMITIVE_FLOAT_ARRAY_TYPE);
      } else if (double.class == innerElemClass) {
        return new Expression.Invoke(
            arrayData, "toDoubleArray", TypeUtils.PRIMITIVE_DOUBLE_ARRAY_TYPE);
      } else {
        Expression.Invoke dim =
            new Expression.Invoke(arrayData, "numElements", TypeUtils.PRIMITIVE_INT_TYPE);
        Expression.NewArray javaArray = new Expression.NewArray(innerElemClass, dim);
        ArrayDataForEach op =
            new ArrayDataForEach(
                arrayData,
                elemType,
                (i, value) -> {
                  Expression elemValue = deserializeFor(value, elemType);
                  return new Expression.AssignArrayElem(javaArray, elemValue, i);
                });
        // add javaArray at last as expression value
        return new Expression.ListExpression(javaArray, op, javaArray);
      }
    }
  }

  /**
   * Using fury to deserialize sliced MemoryBuffer. see {@link
   * BinaryUtils#getElemAccessMethodName(TypeToken)}, {@link Getters#getBuffer(int)}
   */
  protected Expression deserializeForObject(Expression value, TypeToken<?> typeToken) {
    return new Expression.Invoke(furyRef, "deserialize", typeToken, value);
  }
}
