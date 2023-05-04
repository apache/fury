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

package io.fury.util;

import com.google.common.base.Preconditions;
import io.fury.type.TypeUtils;
import java.lang.reflect.Field;

/**
 * Field accessor for primitive types and object types.
 *
 * <p>Note for primitive types, there will be box/unbox overhead. Use {@link UnsafeFieldAccessor} if
 * possible to avoid this overhead.
 */
public abstract class FieldAccessor {
  protected final Field field;
  protected final UnsafeFieldAccessor unsafeFieldAccessor;

  public FieldAccessor(Field field) {
    this.field = field;
    this.unsafeFieldAccessor = new UnsafeFieldAccessor(field);
  }

  public abstract Object get(Object obj);

  public abstract void set(Object obj, Object value);

  public Field getField() {
    return field;
  }

  void checkObj(Object obj) {
    if (!this.field.getDeclaringClass().isAssignableFrom(obj.getClass())) {
      throw new IllegalArgumentException("Illegal class " + obj.getClass());
    }
  }

  public static FieldAccessor createAccessor(Field field) {
    if (field.getType() == boolean.class) {
      return new BooleanAccessor(field);
    } else if (field.getType() == byte.class) {
      return new ByteAccessor(field);
    } else if (field.getType() == char.class) {
      return new CharAccessor(field);
    } else if (field.getType() == short.class) {
      return new ShortAccessor(field);
    } else if (field.getType() == int.class) {
      return new IntAccessor(field);
    } else if (field.getType() == long.class) {
      return new LongAccessor(field);
    } else if (field.getType() == float.class) {
      return new FloatAccessor(field);
    } else if (field.getType() == double.class) {
      return new DoubleAccessor(field);
    } else {
      return new ObjectAccessor(field);
    }
  }

  /** Primitive boolean accessor. */
  public static class BooleanAccessor extends FieldAccessor {
    public BooleanAccessor(Field field) {
      super(field);
      Preconditions.checkArgument(field.getType() == boolean.class);
    }

    @Override
    public Object get(Object obj) {
      checkObj(obj);
      return unsafeFieldAccessor.getBoolean(obj);
    }

    @Override
    public void set(Object obj, Object value) {
      checkObj(obj);
      unsafeFieldAccessor.putBoolean(obj, (Boolean) value);
    }
  }

  /** Primitive byte accessor. */
  public static class ByteAccessor extends FieldAccessor {
    public ByteAccessor(Field field) {
      super(field);
      Preconditions.checkArgument(field.getType() == byte.class);
    }

    @Override
    public Object get(Object obj) {
      checkObj(obj);
      return unsafeFieldAccessor.getByte(obj);
    }

    @Override
    public void set(Object obj, Object value) {
      checkObj(obj);
      unsafeFieldAccessor.putByte(obj, (Byte) value);
    }
  }

  /** Primitive char accessor. */
  public static class CharAccessor extends FieldAccessor {
    public CharAccessor(Field field) {
      super(field);
      Preconditions.checkArgument(field.getType() == char.class);
    }

    @Override
    public Object get(Object obj) {
      checkObj(obj);
      return unsafeFieldAccessor.getChar(obj);
    }

    @Override
    public void set(Object obj, Object value) {
      checkObj(obj);
      unsafeFieldAccessor.putChar(obj, (Character) value);
    }
  }

  /** Primitive short accessor. */
  public static class ShortAccessor extends FieldAccessor {
    public ShortAccessor(Field field) {
      super(field);
      Preconditions.checkArgument(field.getType() == short.class);
    }

    @Override
    public Object get(Object obj) {
      checkObj(obj);
      return unsafeFieldAccessor.getShort(obj);
    }

    @Override
    public void set(Object obj, Object value) {
      checkObj(obj);
      unsafeFieldAccessor.putShort(obj, (Short) value);
    }
  }

  /** Primitive int accessor. */
  public static class IntAccessor extends FieldAccessor {
    public IntAccessor(Field field) {
      super(field);
      Preconditions.checkArgument(field.getType() == int.class);
    }

    @Override
    public Object get(Object obj) {
      checkObj(obj);
      return unsafeFieldAccessor.getInt(obj);
    }

    @Override
    public void set(Object obj, Object value) {
      checkObj(obj);
      unsafeFieldAccessor.putInt(obj, (Integer) value);
    }
  }

  /** Primitive long accessor. */
  public static class LongAccessor extends FieldAccessor {
    public LongAccessor(Field field) {
      super(field);
      Preconditions.checkArgument(field.getType() == long.class);
    }

    @Override
    public Object get(Object obj) {
      checkObj(obj);
      return unsafeFieldAccessor.getLong(obj);
    }

    @Override
    public void set(Object obj, Object value) {
      checkObj(obj);
      unsafeFieldAccessor.putLong(obj, (Long) value);
    }
  }

  /** Primitive float accessor. */
  public static class FloatAccessor extends FieldAccessor {
    public FloatAccessor(Field field) {
      super(field);
      Preconditions.checkArgument(field.getType() == float.class);
    }

    @Override
    public Object get(Object obj) {
      checkObj(obj);
      return unsafeFieldAccessor.getFloat(obj);
    }

    @Override
    public void set(Object obj, Object value) {
      checkObj(obj);
      unsafeFieldAccessor.putFloat(obj, (Float) value);
    }
  }

  /** Primitive double accessor. */
  public static class DoubleAccessor extends FieldAccessor {
    public DoubleAccessor(Field field) {
      super(field);
      Preconditions.checkArgument(field.getType() == double.class);
    }

    @Override
    public Object get(Object obj) {
      checkObj(obj);
      return unsafeFieldAccessor.getDouble(obj);
    }

    @Override
    public void set(Object obj, Object value) {
      checkObj(obj);
      unsafeFieldAccessor.putDouble(obj, (Double) value);
    }
  }

  /** Object accessor. */
  public static class ObjectAccessor extends FieldAccessor {
    public ObjectAccessor(Field field) {
      super(field);
      Preconditions.checkArgument(!TypeUtils.isPrimitive(field.getType()));
    }

    @Override
    public Object get(Object obj) {
      checkObj(obj);
      return unsafeFieldAccessor.getObject(obj);
    }

    @Override
    public void set(Object obj, Object value) {
      checkObj(obj);
      unsafeFieldAccessor.putObject(obj, value);
    }
  }
}
