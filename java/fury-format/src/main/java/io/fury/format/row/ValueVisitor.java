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

package io.fury.format.row;

import io.fury.format.type.DefaultTypeVisitor;
import java.util.function.Function;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * An arrow visitor to visit values in {@link Row} and {@link ArrayData}.
 *
 * @author chaokunyang
 */
class ValueVisitor extends DefaultTypeVisitor<Function<Integer, Object>> {
  private final Getters getters;

  ValueVisitor(Getters getters) {
    this.getters = getters;
  }

  @Override
  public Function<Integer, Object> visit(ArrowType.Bool type) {
    return getters::getBoolean;
  }

  @Override
  public Function<Integer, Object> visit(ArrowType.Int type) {
    if (type.getIsSigned()) {
      int byteWidth = type.getBitWidth() / 8;
      switch (byteWidth) {
        case 1:
          return getters::getByte;
        case 2:
          return getters::getShort;
        case 4:
          return getters::getInt;
        case 8:
          return getters::getLong;
        default:
          return unsupported(type);
      }
    }
    return unsupported(type);
  }

  @Override
  public Function<Integer, Object> visit(ArrowType.FloatingPoint type) {
    switch (type.getPrecision()) {
      case SINGLE:
        return getters::getFloat;
      case DOUBLE:
        return getters::getDouble;
      default:
        return unsupported(type);
    }
  }

  @Override
  public Function<Integer, Object> visit(ArrowType.Date type) {
    switch (type.getUnit()) {
      case DAY:
        return getters::getDate;
      default:
        return unsupported(type);
    }
  }

  @Override
  public Function<Integer, Object> visit(ArrowType.Timestamp type) {
    return getters::getTimestamp;
  }

  @Override
  public Function<Integer, Object> visit(ArrowType.Binary type) {
    return getters::getBinary;
  }

  @Override
  public Function<Integer, Object> visit(ArrowType.Decimal type) {
    return getters::getDecimal;
  }

  @Override
  public Function<Integer, Object> visit(ArrowType.Utf8 type) {
    return getters::getString;
  }

  @Override
  public Function<Integer, Object> visit(ArrowType.Struct type) {
    return getters::getStruct;
  }

  @Override
  public Function<Integer, Object> visit(ArrowType.List type) {
    return getters::getArray;
  }

  @Override
  public Function<Integer, Object> visit(ArrowType.Map type) {
    return getters::getMap;
  }
}
