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

import static io.fury.type.TypeUtils.OBJECT_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_BOOLEAN_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_BYTE_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_CHAR_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_DOUBLE_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_FLOAT_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_INT_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_LONG_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_SHORT_TYPE;
import static io.fury.type.TypeUtils.PRIMITIVE_VOID_TYPE;
import static io.fury.type.TypeUtils.getRawType;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.fury.Fury;
import io.fury.codegen.CodegenContext;
import io.fury.codegen.Expression;
import io.fury.codegen.Expression.Inlineable;
import io.fury.codegen.Expression.Literal;
import io.fury.codegen.Expression.Reference;
import io.fury.codegen.Expression.StaticInvoke;
import io.fury.codegen.ExpressionUtils;
import io.fury.memory.MemoryBuffer;
import io.fury.resolver.ClassInfo;
import io.fury.resolver.ClassInfoCache;
import io.fury.type.Descriptor;
import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;
import io.fury.util.StringUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import sun.misc.Unsafe;

/**
 * Base builder for generating code to serialize java bean in row-format or object stream format.
 *
 * <ul>
 *   This builder has following requirements for the class of java bean:
 *   <li>public
 *   <li>For instance inner class, ignore outer class field.
 *   <li>For instance inner class, deserialized outer class field is null
 * </ul>
 *
 * @author chaokunyang
 */
@SuppressWarnings("UnstableApiUsage")
public abstract class CodecBuilder {
  protected static final String ROOT_OBJECT_NAME = "obj";
  protected static final String FURY_NAME = "fury";

  static TypeToken<Object[]> objectArrayTypeToken = TypeToken.of(Object[].class);
  static TypeToken<MemoryBuffer> bufferTypeToken = TypeToken.of(MemoryBuffer.class);
  static TypeToken<ClassInfo> classInfoTypeToken = TypeToken.of(ClassInfo.class);
  static TypeToken<ClassInfoCache> classInfoCacheTypeToken = TypeToken.of(ClassInfoCache.class);

  protected final CodegenContext ctx;
  protected final TypeToken<?> beanType;
  protected final Class<?> beanClass;
  private final Set<String> duplicatedFields;
  protected Reference furyRef = new Reference(FURY_NAME, TypeToken.of(Fury.class));
  private final Map<String, Reference> fieldMap = new HashMap<>();

  public CodecBuilder(CodegenContext ctx, TypeToken<?> beanType) {
    this.ctx = ctx;
    this.beanType = beanType;
    this.beanClass = getRawType(beanType);
    duplicatedFields =
        Descriptor.getSortedDuplicatedFields(Descriptor.getAllDescriptorsMap(beanClass)).keySet();
    // don't ctx.addImport beanClass, because it maybe causes name collide.
    ctx.reserveName(FURY_NAME);
    ctx.reserveName(ROOT_OBJECT_NAME);
    // Don't import other packages to avoid class conflicts.
    // For example user class named as `Date`/`List`/`MemoryBuffer`
  }

  /** Generate codec class code. */
  public abstract String genCode();

  /** Returns an expression that serialize java bean of type {@link CodecBuilder#beanClass}. */
  public abstract Expression buildEncodeExpression();

  // left null check in sub class encode method to reduce data dependence.
  private final boolean fieldNullable = false;

  /** Returns an expression that get field value from <code>bean</code>. */
  protected Expression getFieldValue(Expression inputBeanExpr, Descriptor descriptor) {
    TypeToken<?> fieldType = descriptor.getTypeToken();
    // No public field type is cast to public parent classes in subclasses of this class.
    Preconditions.checkArgument(
        Modifier.isPublic(getRawType(fieldType).getModifiers()),
        "Field type should be public for codegen-based access");
    String fieldName = descriptor.getName();
    if (duplicatedFields.contains(fieldName) || !Modifier.isPublic(beanClass.getModifiers())) {
      return unsafeAccessField(inputBeanExpr, beanClass, descriptor);
    }
    // public field or non-private non-java field access field directly.
    if (Modifier.isPublic(descriptor.getModifiers())) {
      return new Expression.FieldValue(inputBeanExpr, fieldName, fieldType, fieldNullable, false);
    } else if (descriptor.getReadMethod() != null
        && Modifier.isPublic(descriptor.getReadMethod().getModifiers())) {
      return new Expression.Invoke(
          inputBeanExpr, descriptor.getReadMethod().getName(), fieldName, fieldType, fieldNullable);
    } else {
      if (!Modifier.isPrivate(descriptor.getModifiers())) {
        if (AccessorHelper.defineAccessor(descriptor.getField())) {
          return new StaticInvoke(
              AccessorHelper.getAccessorClass(descriptor.getField()),
              fieldName,
              fieldType,
              fieldNullable,
              inputBeanExpr);
        }
      }
      if (descriptor.getReadMethod() != null
          && !Modifier.isPrivate(descriptor.getReadMethod().getModifiers())) {
        if (AccessorHelper.defineAccessor(descriptor.getReadMethod())) {
          return new StaticInvoke(
              AccessorHelper.getAccessorClass(descriptor.getReadMethod()),
              descriptor.getReadMethod().getName(),
              fieldType,
              fieldNullable,
              inputBeanExpr);
        }
      }
      return unsafeAccessField(inputBeanExpr, beanClass, descriptor);
    }
  }

  /** Returns an expression that get field value> from <code>bean</code> using reflection. */
  private Expression reflectAccessField(
      Expression inputObject, Class<?> cls, Descriptor descriptor) {
    Reference fieldRef = getOrCreateField(cls, descriptor.getName());
    // boolean fieldNullable = !descriptor.getTypeToken().isPrimitive();
    Expression.Invoke getObj =
        new Expression.Invoke(fieldRef, "get", OBJECT_TYPE, fieldNullable, inputObject);
    return new Expression.Cast(getObj, descriptor.getTypeToken(), descriptor.getName());
  }

  /** Returns an expression that get field value> from <code>bean</code> using {@link Unsafe}. */
  private Expression unsafeAccessField(
      Expression inputObject, Class<?> cls, Descriptor descriptor) {
    String fieldName = descriptor.getName();
    // Use Field in case the class has duplicate field name as `fieldName`.
    long fieldOffset = ReflectionUtils.getFieldOffset(descriptor.getField());
    Preconditions.checkArgument(fieldOffset != -1);
    Literal fieldOffsetExpr = Literal.ofLong(fieldOffset);
    if (descriptor.getTypeToken().isPrimitive()) {
      // ex: Platform.UNSAFE.getFloat(obj, fieldOffset)
      Preconditions.checkArgument(!fieldNullable);
      TypeToken<?> returnType = descriptor.getTypeToken();
      String funcName = "get" + StringUtils.capitalize(descriptor.getRawType().toString());
      return new StaticInvoke(
          Platform.class, funcName, returnType, false, inputObject, fieldOffsetExpr);
    } else {
      // ex: Platform.UNSAFE.getObject(obj, fieldOffset)
      StaticInvoke getObj =
          new StaticInvoke(
              Platform.class,
              "getObject",
              OBJECT_TYPE,
              fieldNullable,
              inputObject,
              fieldOffsetExpr);
      return new Expression.Cast(getObj, descriptor.getTypeToken(), fieldName);
    }
  }

  /**
   * Returns an expression that deserialize data as a java bean of type {@link
   * CodecBuilder#beanClass}.
   */
  public abstract Expression buildDecodeExpression();

  /** Returns an expression that set field <code>value</code> to <code>bean</code>. */
  protected Expression setFieldValue(Expression bean, Descriptor d, Expression value) {
    String fieldName = d.getName();
    if (value instanceof Inlineable) {
      ((Inlineable) value).inline();
    }
    if (duplicatedFields.contains(fieldName) || !Modifier.isPublic(beanClass.getModifiers())) {
      return unsafeSetField(bean, d, value);
    }
    if (!Modifier.isFinal(d.getModifiers()) && Modifier.isPublic(d.getModifiers())) {
      return new Expression.SetField(bean, fieldName, value);
    } else if (d.getWriteMethod() != null && Modifier.isPublic(d.getWriteMethod().getModifiers())) {
      return new Expression.Invoke(bean, d.getWriteMethod().getName(), value);
    } else {
      if (!Modifier.isFinal(d.getModifiers()) && !Modifier.isPrivate(d.getModifiers())) {
        if (AccessorHelper.defineAccessor(d.getField())) {
          Class<?> accessorClass = AccessorHelper.getAccessorClass(d.getField());
          return new StaticInvoke(
              accessorClass, d.getName(), PRIMITIVE_VOID_TYPE, false, bean, value);
        }
      }
      if (d.getWriteMethod() != null && !Modifier.isPrivate(d.getWriteMethod().getModifiers())) {
        if (AccessorHelper.defineAccessor(d.getWriteMethod())) {
          Class<?> accessorClass = AccessorHelper.getAccessorClass(d.getWriteMethod());
          return new StaticInvoke(
              accessorClass, d.getWriteMethod().getName(), PRIMITIVE_VOID_TYPE, false, bean, value);
        }
      }
      return unsafeSetField(bean, d, value);
    }
  }

  /**
   * Returns an expression that set field <code>value</code> to <code>bean</code> using reflection.
   */
  private Expression reflectSetField(Expression bean, String fieldName, Expression value) {
    // Class maybe have getter, but don't have setter, so we can't rely on reflectAccessField to
    // populate fieldMap
    Reference fieldRef = getOrCreateField(getRawType(bean.type()), fieldName);
    Preconditions.checkNotNull(fieldRef);
    return new Expression.Invoke(fieldRef, "set", bean, value);
  }

  /**
   * Returns an expression that set field <code>value</code> to <code>bean</code> using {@link
   * Unsafe}.
   */
  private Expression unsafeSetField(Expression bean, Descriptor descriptor, Expression value) {
    TypeToken<?> fieldType = descriptor.getTypeToken();
    // Use Field in case the class has duplicate field name as `fieldName`.
    long fieldOffset = ReflectionUtils.getFieldOffset(descriptor.getField());
    Preconditions.checkArgument(fieldOffset != -1);
    Literal fieldOffsetExpr = Literal.ofLong(fieldOffset);
    if (descriptor.getTypeToken().isPrimitive()) {
      Preconditions.checkArgument(value.type().equals(fieldType));
      String funcName = "put" + StringUtils.capitalize(getRawType(fieldType).toString());
      return new StaticInvoke(Platform.class, funcName, bean, fieldOffsetExpr, value);
    } else {
      return new StaticInvoke(Platform.class, "putObject", bean, fieldOffsetExpr, value);
    }
  }

  private Reference getOrCreateField(Class<?> cls, String fieldName) {
    Reference fieldRef = fieldMap.get(fieldName);
    if (fieldRef == null) {
      TypeToken<Field> fieldTypeToken = TypeToken.of(Field.class);
      String fieldRefName = ctx.newName(fieldName + "Field");
      Preconditions.checkArgument(Modifier.isPublic(cls.getModifiers()));
      Literal clzLiteral = new Literal(ctx.type(cls) + ".class");
      StaticInvoke fieldExpr =
          new StaticInvoke(
              ReflectionUtils.class,
              "getField",
              fieldTypeToken,
              false,
              clzLiteral,
              ExpressionUtils.literalStr(fieldName));
      Expression.Invoke setAccessible =
          new Expression.Invoke(fieldExpr, "setAccessible", new Literal("true"));
      Expression.ListExpression createField =
          new Expression.ListExpression(setAccessible, fieldExpr);
      ctx.addField(ctx.type(Field.class), fieldRefName, createField);
      fieldRef = new Reference(fieldRefName, fieldTypeToken);
      fieldMap.put(fieldName, fieldRef);
    }
    return fieldRef;
  }

  /** Returns an Expression that create a new java object of type {@link CodecBuilder#beanClass}. */
  protected Expression newBean() {
    // TODO allow default access-level class.
    if (Modifier.isPublic(beanClass.getModifiers())) {
      return new Expression.NewInstance(beanType);
    } else {
      return new StaticInvoke(Platform.class, "newInstance", OBJECT_TYPE, beanClassExpr());
    }
  }

  protected Expression beanClassExpr() {
    throw new UnsupportedOperationException();
  }

  /**
   * Build unsafePut operation.
   *
   * @see MemoryBuffer#unsafePut(Object, long, byte)
   */
  protected Expression unsafePut(Expression base, Expression pos, Expression value) {
    return new StaticInvoke(MemoryBuffer.class, "unsafePut", base, pos, value);
  }

  protected Expression unsafePutBoolean(Expression base, Expression pos, Expression value) {
    return new StaticInvoke(MemoryBuffer.class, "unsafePutBoolean", base, pos, value);
  }

  protected Expression unsafePutChar(Expression base, Expression pos, Expression value) {
    return new StaticInvoke(MemoryBuffer.class, "unsafePutChar", base, pos, value);
  }

  protected Expression unsafePutShort(Expression base, Expression pos, Expression value) {
    return new StaticInvoke(MemoryBuffer.class, "unsafePutShort", base, pos, value);
  }

  protected Expression unsafePutInt(Expression base, Expression pos, Expression value) {
    return new StaticInvoke(MemoryBuffer.class, "unsafePutInt", base, pos, value);
  }

  protected Expression unsafePutLong(Expression base, Expression pos, Expression value) {
    return new StaticInvoke(MemoryBuffer.class, "unsafePutLong", base, pos, value);
  }

  protected Expression unsafePutFloat(Expression base, Expression pos, Expression value) {
    return new StaticInvoke(MemoryBuffer.class, "unsafePutFloat", base, pos, value);
  }

  /**
   * Build unsafePutDouble operation.
   *
   * @see MemoryBuffer#unsafePutDouble(Object, long, double)
   */
  protected Expression unsafePutDouble(Expression base, Expression pos, Expression value) {
    return new StaticInvoke(MemoryBuffer.class, "unsafePutDouble", base, pos, value);
  }

  /**
   * Build unsafeGet operation.
   *
   * @see MemoryBuffer#unsafeGet(Object, long)
   */
  protected Expression unsafeGet(Expression base, Expression pos) {
    return new StaticInvoke(MemoryBuffer.class, "unsafeGet", PRIMITIVE_BYTE_TYPE, base, pos);
  }

  protected Expression unsafeGetBoolean(Expression base, Expression pos) {
    return new StaticInvoke(
        MemoryBuffer.class, "unsafeGetBoolean", PRIMITIVE_BOOLEAN_TYPE, base, pos);
  }

  protected Expression unsafeGetChar(Expression base, Expression pos) {
    return new StaticInvoke(MemoryBuffer.class, "unsafeGetChar", PRIMITIVE_CHAR_TYPE, base, pos);
  }

  protected Expression unsafeGetShort(Expression base, Expression pos) {
    return new StaticInvoke(MemoryBuffer.class, "unsafeGetShort", PRIMITIVE_SHORT_TYPE, base, pos);
  }

  protected Expression unsafeGetInt(Expression base, Expression pos) {
    return new StaticInvoke(MemoryBuffer.class, "unsafeGetInt", PRIMITIVE_INT_TYPE, base, pos);
  }

  protected Expression unsafeGetLong(Expression base, Expression pos) {
    return new StaticInvoke(MemoryBuffer.class, "unsafeGetLong", PRIMITIVE_LONG_TYPE, base, pos);
  }

  protected Expression unsafeGetFloat(Expression base, Expression pos) {
    return new StaticInvoke(MemoryBuffer.class, "unsafeGetFloat", PRIMITIVE_FLOAT_TYPE, base, pos);
  }

  protected Expression unsafeGetDouble(Expression base, Expression pos) {
    return new StaticInvoke(
        MemoryBuffer.class, "unsafeGetDouble", PRIMITIVE_DOUBLE_TYPE, base, pos);
  }
}
