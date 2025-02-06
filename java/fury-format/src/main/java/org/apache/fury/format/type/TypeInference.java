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

package org.apache.fury.format.type;

import static org.apache.fury.format.type.DataTypes.field;
import static org.apache.fury.type.TypeUtils.getRawType;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.reflect.TypeRef;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.util.DecimalUtils;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.StringUtils;

/** Arrow related type inference. */
public class TypeInference {

  public static Schema inferSchema(java.lang.reflect.Type type) {
    return inferSchema(TypeRef.of(type));
  }

  public static Schema inferSchema(Class<?> clz) {
    return inferSchema(TypeRef.of(clz));
  }

  /**
   * Infer the schema for class.
   *
   * @param typeRef bean class type
   * @return schema of a class
   */
  public static Schema inferSchema(TypeRef<?> typeRef) {
    return inferSchema(typeRef, true);
  }

  public static Schema inferSchema(TypeRef<?> typeRef, boolean forStruct) {
    Field field = inferField(typeRef);
    if (forStruct) {
      Preconditions.checkArgument(field.getType().getTypeID() == ArrowType.ArrowTypeID.Struct);
      return new Schema(field.getChildren());
    } else {
      return new Schema(Arrays.asList(field));
    }
  }

  public static Optional<ArrowType> getDataType(Class<?> cls) {
    return getDataType(TypeRef.of(cls));
  }

  public static Optional<ArrowType> getDataType(TypeRef<?> typeRef) {
    try {
      return Optional.of(inferDataType(typeRef));
    } catch (UnsupportedOperationException e) {
      return Optional.empty();
    }
  }

  public static ArrowType inferDataType(TypeRef<?> typeRef) {
    return inferField(typeRef).getType();
  }

  public static Field arrayInferField(
      java.lang.reflect.Type arrayType, java.lang.reflect.Type type) {
    return arrayInferField(TypeRef.of(arrayType), TypeRef.of(type));
  }

  public static Field arrayInferField(Class<?> arrayClz, Class<?> clz) {
    return arrayInferField(TypeRef.of(arrayClz), TypeRef.of(clz));
  }

  /**
   * Infer the field of the list.
   *
   * @param typeRef bean class type
   * @return field of the list
   */
  public static Field arrayInferField(TypeRef<?> arrayTypeRef, TypeRef<?> typeRef) {
    Field field = inferField(arrayTypeRef, typeRef);
    Preconditions.checkArgument(field.getType().getTypeID() == ArrowType.ArrowTypeID.List);
    return field;
  }

  private static Field inferField(TypeRef<?> typeRef) {
    return inferField(null, typeRef);
  }

  private static Field inferField(TypeRef<?> arrayTypeRef, TypeRef<?> typeRef) {
    LinkedHashSet<Class<?>> seenTypeSet = new LinkedHashSet<>();
    String name = "";
    if (arrayTypeRef != null) {
      Field f = inferField(DataTypes.ARRAY_ITEM_NAME, typeRef, seenTypeSet);
      return DataTypes.arrayField(name, f);
    } else {
      return inferField("", typeRef, seenTypeSet);
    }
  }

  /**
   * When type is both iterable and bean, we take it as iterable in row-format. Note circular
   * references in bean class is not allowed.
   *
   * @return DataType of a typeToken
   */
  private static Field inferField(
      String name, TypeRef<?> typeRef, LinkedHashSet<Class<?>> seenTypeSet) {
    Class<?> rawType = getRawType(typeRef);
    if (rawType == boolean.class) {
      return field(name, DataTypes.notNullFieldType(ArrowType.Bool.INSTANCE));
    } else if (rawType == byte.class) {
      return field(name, DataTypes.notNullFieldType(new ArrowType.Int(8, true)));
    } else if (rawType == short.class) {
      return field(name, DataTypes.notNullFieldType(new ArrowType.Int(16, true)));
    } else if (rawType == int.class) {
      return field(name, DataTypes.notNullFieldType(new ArrowType.Int(32, true)));
    } else if (rawType == long.class) {
      return field(name, DataTypes.notNullFieldType(new ArrowType.Int(64, true)));
    } else if (rawType == float.class) {
      return field(
          name,
          DataTypes.notNullFieldType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
    } else if (rawType == double.class) {
      return field(
          name,
          DataTypes.notNullFieldType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    } else if (rawType == Boolean.class) {
      return field(name, FieldType.nullable((ArrowType.Bool.INSTANCE)));
    } else if (rawType == Byte.class) {
      return field(name, FieldType.nullable((new ArrowType.Int(8, true))));
    } else if (rawType == Short.class) {
      return field(name, FieldType.nullable((new ArrowType.Int(16, true))));
    } else if (rawType == Integer.class) {
      return field(name, FieldType.nullable((new ArrowType.Int(32, true))));
    } else if (rawType == Long.class) {
      return field(name, FieldType.nullable((new ArrowType.Int(64, true))));
    } else if (rawType == Float.class) {
      return field(
          name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
    } else if (rawType == Double.class) {
      return field(
          name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    } else if (rawType == java.math.BigDecimal.class) {
      return field(
          name,
          FieldType.nullable(
              new ArrowType.Decimal(DecimalUtils.MAX_PRECISION, DecimalUtils.MAX_SCALE)));
    } else if (rawType == java.math.BigInteger.class) {
      return field(name, FieldType.nullable(new ArrowType.Decimal(DecimalUtils.MAX_PRECISION, 0)));
    } else if (rawType == java.time.LocalDate.class) {
      return field(name, FieldType.nullable(new ArrowType.Date(DateUnit.DAY)));
    } else if (rawType == java.sql.Date.class) {
      return field(name, FieldType.nullable(new ArrowType.Date(DateUnit.DAY)));
    } else if (rawType == java.sql.Timestamp.class) {
      return field(name, FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    } else if (rawType == java.time.Instant.class) {
      return field(name, FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    } else if (rawType == String.class) {
      return field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE));
    } else if (rawType.isEnum()) {
      return field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE));
    } else if (rawType.isArray()) { // array
      Field f =
          inferField(
              DataTypes.ARRAY_ITEM_NAME,
              Objects.requireNonNull(typeRef.getComponentType()),
              seenTypeSet);
      return DataTypes.arrayField(name, f);
    } else if (TypeUtils.ITERABLE_TYPE.isSupertypeOf(typeRef)) { // iterable
      // when type is both iterable and bean, we take it as iterable in row-format
      Field f =
          inferField(DataTypes.ARRAY_ITEM_NAME, TypeUtils.getElementType(typeRef), seenTypeSet);
      return DataTypes.arrayField(name, f);
    } else if (TypeUtils.MAP_TYPE.isSupertypeOf(typeRef)) {
      Tuple2<TypeRef<?>, TypeRef<?>> kvType = TypeUtils.getMapKeyValueType(typeRef);
      Field keyField = inferField(MapVector.KEY_NAME, kvType.f0, seenTypeSet);
      // Map's keys must be non-nullable
      FieldType keyFieldType =
          new FieldType(
              false, keyField.getType(), keyField.getDictionary(), keyField.getMetadata());
      keyField = DataTypes.field(keyField.getName(), keyFieldType, keyField.getChildren());
      Field valueField = inferField(MapVector.VALUE_NAME, kvType.f1, seenTypeSet);
      return DataTypes.mapField(name, keyField, valueField);
    } else if (TypeUtils.isBean(rawType)) { // bean field
      if (seenTypeSet.contains(rawType)) {
        String msg =
            String.format(
                "circular references in bean class is not allowed, but got " + "%s in %s",
                rawType, seenTypeSet);
        throw new UnsupportedOperationException(msg);
      }
      List<Field> fields =
          Descriptor.getDescriptors(rawType).stream()
              .map(
                  descriptor -> {
                    LinkedHashSet<Class<?>> newSeenTypeSet = new LinkedHashSet<>(seenTypeSet);
                    newSeenTypeSet.add(rawType);
                    String n = StringUtils.lowerCamelToLowerUnderscore(descriptor.getName());
                    return inferField(n, descriptor.getTypeRef(), newSeenTypeSet);
                  })
              .collect(Collectors.toList());
      return DataTypes.structField(name, true, fields);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported type %s for field %s, seen type set is %s", typeRef, name, seenTypeSet));
    }
  }

  public static String inferTypeName(TypeRef<?> token) {
    StringBuilder sb = new StringBuilder();
    if (TypeUtils.ITERABLE_TYPE.isSupertypeOf(token)) {
      sb.append("Array_");
      sb.append(inferTypeName(TypeUtils.getElementType(token)));
    } else if (TypeUtils.MAP_TYPE.isSupertypeOf(token)) {
      sb.append("Map_");
      Tuple2<TypeRef<?>, TypeRef<?>> mapKeyValueType = TypeUtils.getMapKeyValueType(token);
      sb.append(inferTypeName(mapKeyValueType.f0));
      sb.append("_").append(inferTypeName(mapKeyValueType.f1));
    } else {
      sb.append(token.getRawType().getSimpleName());
    }
    return sb.toString();
  }
}
