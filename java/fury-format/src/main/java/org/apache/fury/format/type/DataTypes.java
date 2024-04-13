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

import static org.apache.fury.util.Preconditions.checkArgument;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fury.exception.FuryException;
import org.apache.fury.io.MemoryBufferOutputStream;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.type.Type;
import org.apache.fury.util.DecimalUtils;
import org.apache.fury.util.Preconditions;

/** Arrow data type utils. */
public class DataTypes {
  public static Field PRIMITIVE_BOOLEAN_ARRAY_FIELD = primitiveArrayField(ArrowType.Bool.INSTANCE);
  public static Field PRIMITIVE_BYTE_ARRAY_FIELD = primitiveArrayField(intType(8));
  public static Field PRIMITIVE_SHORT_ARRAY_FIELD = primitiveArrayField(intType(16));
  public static Field PRIMITIVE_INT_ARRAY_FIELD = primitiveArrayField(intType(32));

  public static Field PRIMITIVE_LONG_ARRAY_FIELD = primitiveArrayField(intType(64));
  public static Field PRIMITIVE_FLOAT_ARRAY_FIELD =
      primitiveArrayField(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
  public static Field PRIMITIVE_DOUBLE_ARRAY_FIELD =
      primitiveArrayField(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
  // Array item field default name
  public static final String ARRAY_ITEM_NAME = "item";

  private static final ArrowType.ArrowTypeVisitor<Integer> typeWidthVisitor =
      new DefaultTypeVisitor<Integer>() {

        @Override
        public Integer visit(ArrowType.Struct type) {
          return -1;
        }

        @Override
        public Integer visit(ArrowType.List type) {
          return -1;
        }

        @Override
        public Integer visit(ArrowType.Map type) {
          return -1;
        }

        @Override
        public Integer visit(ArrowType.Bool type) {
          return 1;
        }

        @Override
        public Integer visit(ArrowType.Int type) {
          return type.getBitWidth() / 8;
        }

        @Override
        public Integer visit(ArrowType.FloatingPoint type) {
          switch (type.getPrecision()) {
            case SINGLE:
              return 4;
            case DOUBLE:
              return 8;
            default:
              return unsupported(type);
          }
        }

        @Override
        public Integer visit(ArrowType.Date type) {
          return 4;
        }

        @Override
        public Integer visit(ArrowType.Timestamp type) {
          return 8;
        }

        @Override
        public Integer visit(ArrowType.Binary type) {
          return -1;
        }

        @Override
        public Integer visit(ArrowType.Decimal type) {
          return -1;
        }

        @Override
        public Integer visit(ArrowType.Utf8 type) {
          return -1;
        }
      };

  private static final ArrowType.ArrowTypeVisitor<Type> typeIdVisitor =
      new DefaultTypeVisitor<Type>() {

        @Override
        public Type visit(ArrowType.Bool type) {
          return Type.BOOL;
        }

        @Override
        public Type visit(ArrowType.Int type) {
          if (type.getIsSigned()) {
            int byteWidth = type.getBitWidth() / 8;
            switch (byteWidth) {
              case 1:
                return Type.INT8;
              case 2:
                return Type.INT16;
              case 4:
                return Type.INT32;
              case 8:
                return Type.INT64;
              default:
                return unsupported(type);
            }
          }
          return unsupported(type);
        }

        @Override
        public Type visit(ArrowType.FloatingPoint type) {
          switch (type.getPrecision()) {
            case SINGLE:
              return Type.FLOAT;
            case DOUBLE:
              return Type.DOUBLE;
            default:
              return unsupported(type);
          }
        }

        @Override
        public Type visit(ArrowType.Date type) {
          switch (type.getUnit()) {
            case DAY:
              return Type.DATE32;
            case MILLISECOND:
              return Type.DATE64;
            default:
              return unsupported(type);
          }
        }

        @Override
        public Type visit(ArrowType.Timestamp type) {
          return Type.TIMESTAMP;
        }

        @Override
        public Type visit(ArrowType.Binary type) {
          return Type.BINARY;
        }

        @Override
        public Type visit(ArrowType.Decimal type) {
          return Type.DECIMAL;
        }

        @Override
        public Type visit(ArrowType.Utf8 type) {
          return Type.STRING;
        }

        @Override
        public Type visit(ArrowType.Struct type) {
          return Type.STRUCT;
        }

        @Override
        public Type visit(ArrowType.List type) {
          return Type.LIST;
        }

        @Override
        public Type visit(ArrowType.Map type) {
          return Type.MAP;
        }
      };

  public static int getTypeWidth(ArrowType type) {
    return type.accept(typeWidthVisitor);
  }

  public static Type getTypeId(ArrowType type) {
    return type.accept(typeIdVisitor);
  }

  public static short getTypeIdValue(ArrowType type) {
    return type.accept(typeIdVisitor).getId();
  }

  public static ArrowType.Bool bool() {
    return ArrowType.Bool.INSTANCE;
  }

  public static ArrowType.Int intType(int bitWidth) {
    return new ArrowType.Int(bitWidth, true);
  }

  public static ArrowType.Int int8() {
    return intType(8);
  }

  public static ArrowType.Int int16() {
    return intType(16);
  }

  public static ArrowType.Int int32() {
    return intType(32);
  }

  public static ArrowType.Int int64() {
    return intType(64);
  }

  public static ArrowType.FloatingPoint float32() {
    return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
  }

  public static ArrowType.FloatingPoint float64() {
    return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
  }

  public static ArrowType.Date date32() {
    return new ArrowType.Date(DateUnit.DAY);
  }

  public static ArrowType.Date date64() {
    return new ArrowType.Date(DateUnit.MILLISECOND);
  }

  public static ArrowType.Timestamp timestamp() {
    return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
  }

  public static ArrowType.Binary binary() {
    return ArrowType.Binary.INSTANCE;
  }

  public static ArrowType.Utf8 utf8() {
    return ArrowType.Utf8.INSTANCE;
  }

  public static ArrowType.Decimal decimal() {
    return decimal(DecimalUtils.MAX_PRECISION, DecimalUtils.MAX_SCALE);
  }

  public static ArrowType.Decimal decimal(int precision, int scale) {
    return new ArrowType.Decimal(precision, scale);
  }

  public static ArrowType.Decimal bigintDecimal() {
    return decimal(DecimalUtils.MAX_PRECISION, 0);
  }

  /* ========================= field utils ========================= */
  public static Field field(String name, FieldType fieldType) {
    return field(name, fieldType, Collections.emptyList());
  }

  public static Field field(String name, boolean nullable, ArrowType type, Field... children) {
    return field(name, new FieldType(nullable, type, null), children);
  }

  public static Field field(String name, FieldType fieldType, Field... children) {
    return field(name, fieldType, Arrays.asList(children));
  }

  public static Field field(String name, boolean nullable, ArrowType type, List<Field> children) {
    return field(name, new FieldType(nullable, type, null), children);
  }

  public static Field field(String name, ArrowType type, Field... children) {
    return field(name, true, type, children);
  }

  public static Field field(String name, FieldType fieldType, List<Field> children) {
    return new ExtField(name, fieldType, children);
  }

  public static Field notNullField(String name, ArrowType type, Field... children) {
    return field(name, false, type, children);
  }

  public static FieldType notNullFieldType(ArrowType type) {
    return new FieldType(false, type, null);
  }

  /* ========================= array field utils ========================= */
  public static Field primitiveArrayField(ArrowType type) {
    return primitiveArrayField("", type);
  }

  public static Field primitiveArrayField(String name, ArrowType type) {
    return field(
        name,
        FieldType.nullable(ArrowType.List.INSTANCE),
        Collections.singletonList(field(ARRAY_ITEM_NAME, false, type)));
  }

  public static Field arrayField(ArrowType type) {
    return arrayField("", type);
  }

  public static Field arrayField(String name, ArrowType type) {
    return field(
        name,
        FieldType.nullable(ArrowType.List.INSTANCE),
        Collections.singletonList(field(ARRAY_ITEM_NAME, true, type)));
  }

  public static Field arrayField(FieldType valueType) {
    return arrayField("", valueType);
  }

  public static Field arrayField(String name, FieldType valueType) {
    return field(
        name,
        FieldType.nullable(ArrowType.List.INSTANCE),
        Collections.singletonList(field(ARRAY_ITEM_NAME, valueType)));
  }

  public static Field arrayField(Field valueField) {
    return arrayField("", valueField);
  }

  public static Field arrayField(String name, Field valueField) {
    return field(
        name, FieldType.nullable(ArrowType.List.INSTANCE), Collections.singletonList(valueField));
  }

  public static Field arrayElementField(Field field) {
    return field.getChildren().get(0);
  }

  /* ========================= map field utils start ========================= */
  public static Field mapField(ArrowType keyType, ArrowType itemType) {
    return mapField("", keyType, itemType);
  }

  public static Field mapField(String name, ArrowType keyType, ArrowType itemType) {
    return mapField(
        name,
        field(MapVector.KEY_NAME, false, keyType),
        field(MapVector.VALUE_NAME, true, itemType));
  }

  /**
   * Map data is nested data where each value is a variable number of key-item pairs. Maps can be
   * recursively nested, for example map(utf8, map(utf8, int32)). see more about MapType in type.h
   */
  public static Field mapField(String name, Field keyField, Field itemField) {
    Preconditions.checkArgument(!keyField.isNullable(), "Map's keys must be non-nullable");
    // Map's key-item pairs must be non-nullable structs
    Field valueField = structField(false, keyField, itemField);
    return field(name, true, new ArrowType.Map(false), valueField);
  }

  public static Field keyFieldForMap(Field mapField) {
    Field field = mapField.getChildren().get(0).getChildren().get(0);
    if (field.getClass() != ExtField.class) {
      return new ExtField(field.getName(), field.getFieldType(), field.getChildren());
    }
    return field;
  }

  public static Field itemFieldForMap(Field mapField) {
    Field field = mapField.getChildren().get(0).getChildren().get(1);
    if (field.getClass() != ExtField.class) {
      return new ExtField(field.getName(), field.getFieldType(), field.getChildren());
    }
    return field;
  }

  public static Field keyArrayFieldForMap(Field mapField) {
    return arrayField("keys", keyFieldForMap(mapField));
  }

  public static Field itemArrayFieldForMap(Field mapField) {
    return arrayField("items", itemFieldForMap(mapField));
  }

  /* ========================= struct field utils start ========================= */
  public static Schema schemaFromStructField(Field structField) {
    return new Schema(structField.getChildren(), structField.getMetadata());
  }

  public static Schema createSchema(Field field) {
    if (field.getClass() != ExtField.class) {
      throw new IllegalArgumentException(
          String.format("Field %s got wrong type %s", field, field.getClass()));
    }
    ExtField extField = (ExtField) field;
    Object extData = extField.extData;
    if (extData == null) {
      extField.extData = extData = new Schema(field.getChildren(), field.getMetadata());
    }
    return (Schema) extData;
  }

  static class ExtField extends Field {
    Object extData;

    public ExtField(String name, FieldType fieldType, List<Field> children) {
      super(name, fieldType, children);
    }
  }

  public static Field structField(boolean nullable, Field... fields) {
    return structField("", nullable, fields);
  }

  public static Field structField(String name, boolean nullable, Field... fields) {
    return field(name, nullable, ArrowType.Struct.INSTANCE, fields);
  }

  public static Field structField(String name, boolean nullable, List<Field> fields) {
    return field(name, nullable, ArrowType.Struct.INSTANCE, fields);
  }

  /* ========================= struct field utils end ========================= */

  public static Field fieldOfSchema(Schema schema, int index) {
    return schema.getFields().get(index);
  }

  public static void serializeSchema(Schema schema, MemoryBuffer buffer) {
    try (MemoryBufferOutputStream outputStream = new MemoryBufferOutputStream(buffer);
        WriteChannel writeChannel = new WriteChannel(Channels.newChannel(outputStream))) {
      MessageSerializer.serialize(writeChannel, schema);
    } catch (IOException e) {
      throw new FuryException(String.format("Write schema %s failed", schema), e);
    }
  }

  public static byte[] serializeSchema(Schema schema) {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        WriteChannel writeChannel = new WriteChannel(Channels.newChannel(outputStream))) {
      MessageSerializer.serialize(writeChannel, schema);
      return outputStream.toByteArray();
    } catch (IOException e) {
      throw new FuryException(String.format("Write schema %s failed", schema), e);
    }
  }

  public static Schema deserializeSchema(byte[] bytes) {
    try (ReadChannel readChannel =
        new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes)))) {
      return MessageSerializer.deserializeSchema(readChannel);
    } catch (IOException e) {
      throw new FuryException("Deserialize schema failed", e);
    }
  }

  public static long computeSchemaHash(Schema schema) {
    long hash = 17;
    for (Field field : schema.getFields()) {
      hash = computeHash(hash, field);
    }
    return hash;
  }

  private static long computeHash(long hash, Field field) {
    Type typeID = getTypeId(field.getType());
    while (true) {
      try {
        hash = Math.addExact(Math.multiplyExact(hash, 31), (long) typeID.getId());
        break;
      } catch (ArithmeticException e) {
        hash = hash >> 2;
      }
    }
    List<Field> fields = new ArrayList<>();
    switch (typeID) {
      case LIST:
        fields.add(arrayElementField(field));
        break;
      case MAP:
        {
          fields.add(keyFieldForMap(field));
          fields.add(itemFieldForMap(field));
          break;
        }
      case STRUCT:
        {
          fields.addAll(field.getChildren());
          break;
        }
      default:
        checkArgument(
            field.getChildren().size() == 0,
            "field type should not be nested, but got type id %s field %s.",
            typeID,
            field);
    }
    for (Field child : fields) {
      hash = computeHash(hash, child);
    }
    return hash;
  }
}
