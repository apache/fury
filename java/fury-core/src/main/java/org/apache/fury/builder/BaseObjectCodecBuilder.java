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

package org.apache.fury.builder;

import static org.apache.fury.codegen.CodeGenerator.getPackage;
import static org.apache.fury.codegen.Expression.Invoke.inlineInvoke;
import static org.apache.fury.codegen.Expression.Reference.fieldRef;
import static org.apache.fury.codegen.ExpressionOptimizer.invokeGenerated;
import static org.apache.fury.codegen.ExpressionUtils.eq;
import static org.apache.fury.codegen.ExpressionUtils.gt;
import static org.apache.fury.codegen.ExpressionUtils.inline;
import static org.apache.fury.codegen.ExpressionUtils.neq;
import static org.apache.fury.codegen.ExpressionUtils.not;
import static org.apache.fury.codegen.ExpressionUtils.nullValue;
import static org.apache.fury.codegen.ExpressionUtils.uninline;
import static org.apache.fury.collection.Collections.ofHashSet;
import static org.apache.fury.serializer.CodegenSerializer.LazyInitBeanSerializer;
import static org.apache.fury.type.TypeUtils.CLASS_TYPE;
import static org.apache.fury.type.TypeUtils.COLLECTION_TYPE;
import static org.apache.fury.type.TypeUtils.LIST_TYPE;
import static org.apache.fury.type.TypeUtils.MAP_TYPE;
import static org.apache.fury.type.TypeUtils.OBJECT_TYPE;
import static org.apache.fury.type.TypeUtils.PRIMITIVE_BOOLEAN_TYPE;
import static org.apache.fury.type.TypeUtils.PRIMITIVE_BYTE_TYPE;
import static org.apache.fury.type.TypeUtils.PRIMITIVE_INT_TYPE;
import static org.apache.fury.type.TypeUtils.PRIMITIVE_VOID_TYPE;
import static org.apache.fury.type.TypeUtils.SET_TYPE;
import static org.apache.fury.type.TypeUtils.getElementType;
import static org.apache.fury.type.TypeUtils.getRawType;
import static org.apache.fury.type.TypeUtils.isBoxed;
import static org.apache.fury.type.TypeUtils.isPrimitive;
import static org.apache.fury.util.Preconditions.checkArgument;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.fury.Fury;
import org.apache.fury.codegen.CodeGenerator;
import org.apache.fury.codegen.CodegenContext;
import org.apache.fury.codegen.Expression;
import org.apache.fury.codegen.Expression.Assign;
import org.apache.fury.codegen.Expression.BitAnd;
import org.apache.fury.codegen.Expression.Cast;
import org.apache.fury.codegen.Expression.ForEach;
import org.apache.fury.codegen.Expression.ForLoop;
import org.apache.fury.codegen.Expression.If;
import org.apache.fury.codegen.Expression.Invoke;
import org.apache.fury.codegen.Expression.ListExpression;
import org.apache.fury.codegen.Expression.Literal;
import org.apache.fury.codegen.Expression.Reference;
import org.apache.fury.codegen.Expression.Return;
import org.apache.fury.codegen.Expression.StaticInvoke;
import org.apache.fury.codegen.ExpressionUtils;
import org.apache.fury.codegen.ExpressionVisitor.ExprHolder;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.serializer.CompatibleSerializer;
import org.apache.fury.serializer.ObjectSerializer;
import org.apache.fury.serializer.PrimitiveSerializers.LongSerializer;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.Serializers;
import org.apache.fury.serializer.StringSerializer;
import org.apache.fury.serializer.collection.AbstractCollectionSerializer;
import org.apache.fury.serializer.collection.AbstractMapSerializer;
import org.apache.fury.serializer.collection.CollectionFlags;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.util.GraalvmSupport;
import org.apache.fury.util.Platform;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;
import org.apache.fury.util.StringUtils;

/**
 * Generate sequential read/write code for java serialization to speed up performance. It also
 * reduces space overhead introduced by aligning. Codegen only for time-consuming field, others
 * delegate to fury.
 */
@SuppressWarnings("unchecked")
public abstract class BaseObjectCodecBuilder extends CodecBuilder {
  public static final String BUFFER_NAME = "buffer";
  public static final String REF_RESOLVER_NAME = "refResolver";
  public static final String CLASS_RESOLVER_NAME = "classResolver";
  public static final String POJO_CLASS_TYPE_NAME = "classType";
  public static final String STRING_SERIALIZER_NAME = "strSerializer";
  private static final TypeToken<?> CLASS_RESOLVER_TYPE_TOKEN = TypeToken.of(ClassResolver.class);
  private static final TypeToken<?> STRING_SERIALIZER_TYPE_TOKEN =
      TypeToken.of(StringSerializer.class);
  private static final TypeToken<?> SERIALIZER_TYPE = TypeToken.of(Serializer.class);
  private static final TypeToken<?> COLLECTION_SERIALIZER_TYPE =
      TypeToken.of(AbstractCollectionSerializer.class);
  private static final TypeToken<?> MAP_SERIALIZER_TYPE = TypeToken.of(AbstractMapSerializer.class);

  protected final Reference refResolverRef;
  protected final Reference classResolverRef =
      fieldRef(CLASS_RESOLVER_NAME, CLASS_RESOLVER_TYPE_TOKEN);
  protected final Fury fury;
  protected final ClassResolver classResolver;
  protected final Reference stringSerializerRef;
  private final Map<Class<?>, Reference> serializerMap = new HashMap<>();
  private final Map<String, Object> sharedFieldMap = new HashMap<>();
  protected final Class<?> parentSerializerClass;
  private final Map<String, String> jitCallbackUpdateFields;
  protected LinkedList<String> walkPath = new LinkedList<>();

  public BaseObjectCodecBuilder(TypeToken<?> beanType, Fury fury, Class<?> parentSerializerClass) {
    super(new CodegenContext(), beanType);
    this.fury = fury;
    this.classResolver = fury.getClassResolver();
    this.parentSerializerClass = parentSerializerClass;
    addCommonImports();
    ctx.reserveName(REF_RESOLVER_NAME);
    ctx.reserveName(CLASS_RESOLVER_NAME);
    TypeToken<?> refResolverTypeToken = TypeToken.of(fury.getRefResolver().getClass());
    refResolverRef = fieldRef(REF_RESOLVER_NAME, refResolverTypeToken);
    Expression refResolverExpr =
        new Invoke(furyRef, "getRefResolver", TypeToken.of(RefResolver.class));
    ctx.addField(
        ctx.type(refResolverTypeToken),
        REF_RESOLVER_NAME,
        new Cast(refResolverExpr, refResolverTypeToken));
    Expression classResolverExpr =
        inlineInvoke(furyRef, "getClassResolver", CLASS_RESOLVER_TYPE_TOKEN);
    ctx.addField(ctx.type(CLASS_RESOLVER_TYPE_TOKEN), CLASS_RESOLVER_NAME, classResolverExpr);
    ctx.reserveName(STRING_SERIALIZER_NAME);
    stringSerializerRef = fieldRef(STRING_SERIALIZER_NAME, STRING_SERIALIZER_TYPE_TOKEN);
    ctx.addField(
        ctx.type(TypeToken.of(StringSerializer.class)),
        STRING_SERIALIZER_NAME,
        inlineInvoke(furyRef, "getStringSerializer", CLASS_RESOLVER_TYPE_TOKEN));
    jitCallbackUpdateFields = new HashMap<>();
  }

  public String codecClassName(Class<?> beanClass) {
    String name = ReflectionUtils.getClassNameWithoutPackage(beanClass).replace("$", "_");
    StringBuilder nameBuilder = new StringBuilder(name);
    if (fury.trackingRef()) {
      // Generated classes are different when referenceTracking is switched.
      // So we need to use a different name.
      nameBuilder.append("FuryRef");
    } else {
      nameBuilder.append("Fury");
    }
    nameBuilder.append(codecSuffix()).append("Codec");
    nameBuilder.append('_').append(fury.getConfig().getConfigHash());
    String classUniqueId = CodeGenerator.getClassUniqueId(beanClass);
    if (StringUtils.isNotBlank(classUniqueId)) {
      nameBuilder.append('_').append(classUniqueId);
    }
    return nameBuilder.toString();
  }

  public String codecQualifiedClassName(Class<?> beanClass) {
    String pkg = getPackage(beanClass);
    if (StringUtils.isNotBlank(pkg)) {
      return pkg + "." + codecClassName(beanClass);
    } else {
      return codecClassName(beanClass);
    }
  }

  protected abstract String codecSuffix();

  <T> T visitFury(Function<Fury, T> function) {
    return fury.getJITContext().asyncVisitFury(function);
  }

  @Override
  public String genCode() {
    ctx.setPackage(getPackage(beanClass));
    String className = codecClassName(beanClass);
    ctx.setClassName(className);
    // don't addImport(beanClass), because user class may name collide.
    ctx.extendsClasses(ctx.type(parentSerializerClass));
    ctx.reserveName(POJO_CLASS_TYPE_NAME);
    ctx.addField(ctx.type(Fury.class), FURY_NAME);
    Expression encodeExpr = buildEncodeExpression();
    Expression decodeExpr = buildDecodeExpression();
    String constructorCode =
        StringUtils.format(
            ""
                + "super(${fury}, ${cls});\n"
                + "this.${fury} = ${fury};\n"
                + "${fury}.getClassResolver().setSerializerIfAbsent(${cls}, this);\n",
            "fury",
            FURY_NAME,
            "cls",
            POJO_CLASS_TYPE_NAME);

    ctx.clearExprState();
    String encodeCode = encodeExpr.genCode(ctx).code();
    encodeCode = ctx.optimizeMethodCode(encodeCode);
    ctx.clearExprState();
    String decodeCode = decodeExpr.genCode(ctx).code();
    decodeCode = ctx.optimizeMethodCode(decodeCode);
    ctx.overrideMethod(
        "write",
        encodeCode,
        void.class,
        MemoryBuffer.class,
        BUFFER_NAME,
        Object.class,
        ROOT_OBJECT_NAME);
    ctx.overrideMethod("read", decodeCode, Object.class, MemoryBuffer.class, BUFFER_NAME);
    registerJITNotifyCallback();
    ctx.addConstructor(constructorCode, Fury.class, "fury", Class.class, POJO_CLASS_TYPE_NAME);
    return ctx.genCode();
  }

  protected static class CutPoint {
    public boolean genNewMethod;
    public Set<Expression> cutPoints = new HashSet<>();

    public CutPoint(boolean genNewMethod, Expression... cutPoints) {
      this.genNewMethod = genNewMethod;
      Collections.addAll(this.cutPoints, cutPoints);
    }

    public CutPoint add(Expression cutPoint) {
      cutPoints.add(cutPoint);
      return this;
    }

    @Override
    public String toString() {
      return "CutPoint{" + "genNewMethod=" + genNewMethod + ", cutPoints=" + cutPoints + '}';
    }
  }

  protected void registerJITNotifyCallback() {
    // build encode/decode expr before add constructor to fill up jitCallbackUpdateFields.
    if (!jitCallbackUpdateFields.isEmpty()) {
      StringJoiner stringJoiner = new StringJoiner(", ", "registerJITNotifyCallback(this,", ");\n");
      for (Map.Entry<String, String> entry : jitCallbackUpdateFields.entrySet()) {
        stringJoiner.add("\"" + entry.getKey() + "\"");
        stringJoiner.add(entry.getValue());
      }
      // add this code after field serialization initialization to avoid
      // it overrides field updates by this callback.
      ctx.addInitCode(stringJoiner.toString());
    }
  }

  /**
   * Add common imports to reduce generated code size to speed up jit. Since user class are
   * qualified, there won't be any conflict even if user class has the same name as fury classes.
   *
   * @see CodeGenerator#getClassUniqueId
   */
  protected void addCommonImports() {
    ctx.addImports(
        Fury.class, MemoryBuffer.class, fury.getRefResolver().getClass(), Platform.class);
    ctx.addImports(ClassInfo.class, ClassInfoHolder.class, ClassResolver.class);
    ctx.addImport(Generated.class);
    ctx.addImports(LazyInitBeanSerializer.class, Serializers.EnumSerializer.class);
    ctx.addImports(Serializer.class, StringSerializer.class);
    ctx.addImports(ObjectSerializer.class, CompatibleSerializer.class);
    ctx.addImports(AbstractCollectionSerializer.class, AbstractMapSerializer.class);
  }

  protected Expression serializeFor(
      Expression inputObject, Expression buffer, TypeToken<?> typeToken) {
    return serializeFor(inputObject, buffer, typeToken, false);
  }

  /**
   * Returns an expression that serialize an nullable <code>inputObject</code> to <code>buffer
   * </code>.
   */
  protected Expression serializeFor(
      Expression inputObject,
      Expression buffer,
      TypeToken<?> typeToken,
      boolean generateNewMethod) {
    return serializeFor(inputObject, buffer, typeToken, null, generateNewMethod);
  }

  protected Expression serializeFor(
      Expression inputObject,
      Expression buffer,
      TypeToken<?> typeToken,
      Expression serializer,
      boolean generateNewMethod) {
    // access rawType without jit lock to reduce lock competition.
    Class<?> rawType = getRawType(typeToken);
    if (visitFury(fury -> fury.getClassResolver().needToWriteRef(rawType))) {
      return new If(
          not(writeRefOrNull(buffer, inputObject)),
          serializeForNotNull(inputObject, buffer, typeToken, serializer, generateNewMethod));
    } else {
      // if typeToken is not final, ref tracking of subclass will be ignored too.
      if (typeToken.isPrimitive()) {
        return serializeForNotNull(inputObject, buffer, typeToken, serializer, generateNewMethod);
      }
      Expression action =
          new ListExpression(
              new Invoke(
                  buffer, "writeByte", new Literal(Fury.REF_VALUE_FLAG, PRIMITIVE_BYTE_TYPE)),
              serializeForNotNull(inputObject, buffer, typeToken, serializer, generateNewMethod));
      return new If(
          ExpressionUtils.eqNull(inputObject),
          new Invoke(buffer, "writeByte", new Literal(Fury.NULL_FLAG, PRIMITIVE_BYTE_TYPE)),
          action);
    }
  }

  protected Expression writeRefOrNull(Expression buffer, Expression object) {
    return inlineInvoke(refResolverRef, "writeRefOrNull", PRIMITIVE_BOOLEAN_TYPE, buffer, object);
  }

  protected Expression serializeForNotNull(
      Expression inputObject, Expression buffer, TypeToken<?> typeToken) {
    return serializeForNotNull(inputObject, buffer, typeToken, null, false);
  }

  /**
   * Returns an expression that serialize an not null <code>inputObject</code> to <code>buffer
   * </code>.
   */
  private Expression serializeForNotNull(
      Expression inputObject,
      Expression buffer,
      TypeToken<?> typeToken,
      boolean generateNewMethod) {
    return serializeForNotNull(inputObject, buffer, typeToken, null, generateNewMethod);
  }

  private Expression serializeForNotNull(
      Expression inputObject,
      Expression buffer,
      TypeToken<?> typeToken,
      Expression serializer,
      boolean generateNewMethod) {
    Class<?> clz = getRawType(typeToken);
    if (isPrimitive(clz) || isBoxed(clz)) {
      // for primitive, inline call here to avoid java boxing, rather call corresponding serializer.
      if (clz == byte.class || clz == Byte.class) {
        return new Invoke(buffer, "writeByte", inputObject);
      } else if (clz == boolean.class || clz == Boolean.class) {
        return new Invoke(buffer, "writeBoolean", inputObject);
      } else if (clz == char.class || clz == Character.class) {
        return new Invoke(buffer, "writeChar", inputObject);
      } else if (clz == short.class || clz == Short.class) {
        return new Invoke(buffer, "writeInt16", inputObject);
      } else if (clz == int.class || clz == Integer.class) {
        String func = fury.compressInt() ? "writeVarInt32" : "writeInt32";
        return new Invoke(buffer, func, inputObject);
      } else if (clz == long.class || clz == Long.class) {
        return LongSerializer.writeInt64(buffer, inputObject, fury.longEncoding(), true);
      } else if (clz == float.class || clz == Float.class) {
        return new Invoke(buffer, "writeFloat32", inputObject);
      } else if (clz == double.class || clz == Double.class) {
        return new Invoke(buffer, "writeFloat64", inputObject);
      } else {
        throw new IllegalStateException("impossible");
      }
    } else {
      if (clz == String.class) {
        return fury.getStringSerializer().writeStringExpr(stringSerializerRef, buffer, inputObject);
      }
      Expression action;
      // this is different from ITERABLE_TYPE in RowCodecBuilder. In row-format we don't need to
      // ensure
      // class consistence, we only need to ensure interface consistence. But in java serialization,
      // we need to ensure class consistence.
      if (useCollectionSerialization(typeToken)) {
        action =
            serializeForCollection(buffer, inputObject, typeToken, serializer, generateNewMethod);
      } else if (useMapSerialization(typeToken)) {
        action = serializeForMap(buffer, inputObject, typeToken, serializer, generateNewMethod);
      } else {
        action = serializeForNotNullObject(inputObject, buffer, typeToken, serializer);
      }
      return action;
    }
  }

  protected boolean useCollectionSerialization(TypeToken<?> typeToken) {
    return visitFury(f -> f.getClassResolver().isCollection(TypeUtils.getRawType(typeToken)));
  }

  protected boolean useMapSerialization(TypeToken<?> typeToken) {
    return visitFury(f -> f.getClassResolver().isMap(TypeUtils.getRawType(typeToken)));
  }

  /**
   * Whether the provided type should be taken as final. Although the <code>clz</code> can be final,
   * the method can still return false. For example, we return false in meta share mode to write
   * class defs for the non-inner final types.
   */
  protected abstract boolean isMonomorphic(Class<?> clz);

  protected Expression serializeForNotNullObject(
      Expression inputObject, Expression buffer, TypeToken<?> typeToken, Expression serializer) {
    Class<?> clz = getRawType(typeToken);
    if (serializer != null) {
      return new Invoke(serializer, "write", buffer, inputObject);
    }
    if (isMonomorphic(clz)) {
      serializer = getOrCreateSerializer(clz);
      return new Invoke(serializer, "write", buffer, inputObject);
    } else {
      return writeForNotNullNonFinalObject(inputObject, buffer, typeToken);
    }
  }

  // Note that `CompatibleCodecBuilder` may mark some final objects as non-final.
  protected Expression writeForNotNullNonFinalObject(
      Expression inputObject, Expression buffer, TypeToken<?> typeToken) {
    Class<?> clz = getRawType(typeToken);
    Expression clsExpr = new Invoke(inputObject, "getClass", "cls", CLASS_TYPE);
    ListExpression writeClassAndObject = new ListExpression();
    Tuple2<Reference, Boolean> classInfoRef = addClassInfoField(clz);
    Expression classInfo = classInfoRef.f0;
    if (classInfoRef.f1) {
      writeClassAndObject.add(
          new If(
              neq(new Invoke(classInfo, "getCls", CLASS_TYPE), clsExpr),
              new Assign(
                  classInfo,
                  inlineInvoke(classResolverRef, "getClassInfo", classInfoTypeToken, clsExpr))));
    }
    writeClassAndObject.add(
        fury.getClassResolver().writeClassExpr(classResolverRef, buffer, classInfo));
    writeClassAndObject.add(
        new Invoke(
            inlineInvoke(classInfo, "getSerializer", SERIALIZER_TYPE),
            "write",
            PRIMITIVE_VOID_TYPE,
            buffer,
            inputObject));
    return invokeGenerated(
        ctx,
        ImmutableSet.of(buffer, inputObject),
        writeClassAndObject,
        "writeClassAndObject",
        false);
  }

  /**
   * Returns a serializer expression which will be used to call write/read method to avoid virtual
   * methods calls in most situations.
   */
  protected Expression getOrCreateSerializer(Class<?> cls) {
    // Not need to check cls final, take collection writeSameTypeElements as an example.
    // Preconditions.checkArgument(isMonomorphic(cls), cls);
    Reference serializerRef = serializerMap.get(cls);
    if (serializerRef == null) {
      // potential recursive call for seq codec generation is handled in `getSerializerClass`.
      Class<? extends Serializer> serializerClass =
          visitFury(f -> f.getClassResolver().getSerializerClass(cls));
      Preconditions.checkNotNull(serializerClass, "Unsupported for class " + cls);
      if (!ReflectionUtils.isPublic(serializerClass)) {
        // TODO(chaokunyang) add jdk17+ unexported class check.
        // non-public class can't be accessed in generated class.
        serializerClass = Serializer.class;
      } else {
        ClassLoader beanClassClassLoader = beanClass.getClassLoader();
        if (beanClassClassLoader == null) {
          beanClassClassLoader = Thread.currentThread().getContextClassLoader();
          if (beanClassClassLoader == null) {
            beanClassClassLoader = Fury.class.getClassLoader();
          }
        }
        try {
          beanClassClassLoader.loadClass(serializerClass.getName());
        } catch (ClassNotFoundException e) {
          // If `cls` is loaded in another class different from `beanClassClassLoader`,
          // then serializerClass is loaded in another class different from `beanClassClassLoader`.
          serializerClass = LazyInitBeanSerializer.class;
        }
        if (serializerClass == LazyInitBeanSerializer.class
            || serializerClass == ObjectSerializer.class
            || serializerClass == CompatibleSerializer.class) {
          // field init may get jit serializer, which will cause cast exception if not use base
          // type.
          serializerClass = Serializer.class;
        }
      }
      TypeToken<? extends Serializer> serializerTypeToken = TypeToken.of(serializerClass);
      Expression fieldTypeExpr = getClassExpr(cls);
      // Don't invoke `Serializer.newSerializer` here, since it(ex. ObjectSerializer) may set itself
      // as global serializer, which overwrite serializer updates in jit callback.
      Expression newSerializerExpr =
          inlineInvoke(classResolverRef, "getRawSerializer", SERIALIZER_TYPE, fieldTypeExpr);
      String name = ctx.newName(StringUtils.uncapitalize(serializerClass.getSimpleName()));
      // It's ok it jit already finished and this method return false, in such cases
      // `serializerClass` is already jit generated class.
      boolean hasJITResult = fury.getJITContext().hasJITResult(cls);
      if (hasJITResult) {
        jitCallbackUpdateFields.put(name, ctx.type(cls) + ".class");
        ctx.addField(
            false, ctx.type(Serializer.class), name, new Cast(newSerializerExpr, SERIALIZER_TYPE));
        serializerRef = new Reference(name, SERIALIZER_TYPE, false);
      } else {
        ctx.addField(
            true,
            ctx.type(serializerClass),
            name,
            new Cast(newSerializerExpr, serializerTypeToken));
        serializerRef = fieldRef(name, serializerTypeToken);
      }
      serializerMap.put(cls, serializerRef);
    }
    return serializerRef;
  }

  protected Expression getClassExpr(Class<?> cls) {
    if (sourcePublicAccessible(cls)) {
      return Literal.ofClass(cls);
    } else {
      return new StaticInvoke(
          ReflectionUtils.class,
          "loadClass",
          CLASS_TYPE,
          beanClassExpr(),
          Literal.ofString(cls.getName()));
    }
  }

  /**
   * The boolean value in tuple indicates whether the classinfo needs update.
   *
   * @return false for tuple field1 if the classinfo doesn't need update.
   */
  protected Tuple2<Reference, Boolean> addClassInfoField(Class<?> cls) {
    Expression classInfoExpr;
    boolean needUpdate = !ReflectionUtils.isMonomorphic(cls);
    String key;
    if (!needUpdate) {
      key = "classInfo:" + cls;
    } else {
      key = "classInfo:" + cls + walkPath;
    }
    Tuple2<Reference, Boolean> classInfoRef = (Tuple2<Reference, Boolean>) sharedFieldMap.get(key);
    if (classInfoRef != null) {
      return classInfoRef;
    }
    if (!needUpdate) {
      Expression clsExpr = getClassExpr(cls);
      classInfoExpr = inlineInvoke(classResolverRef, "getClassInfo", classInfoTypeToken, clsExpr);
      // Use `ctx.freshName(cls)` to avoid wrong name for arr type.
      String name = ctx.newName(ctx.newName(cls) + "ClassInfo");
      ctx.addField(true, ctx.type(ClassInfo.class), name, classInfoExpr);
      classInfoRef = Tuple2.of(fieldRef(name, classInfoTypeToken), false);
    } else {
      classInfoExpr = inlineInvoke(classResolverRef, "nilClassInfo", classInfoTypeToken);
      String name = ctx.newName(cls, "ClassInfo");
      ctx.addField(false, ctx.type(ClassInfo.class), name, classInfoExpr);
      // Can't use fieldRef, since the field is not final.
      classInfoRef = Tuple2.of(new Reference(name, classInfoTypeToken), true);
    }
    sharedFieldMap.put(key, classInfoRef);
    return classInfoRef;
  }

  protected Reference addClassInfoHolderField(Class<?> cls) {
    // Final type need to write classinfo when meta share enabled.
    String key;
    if (ReflectionUtils.isMonomorphic(cls)) {
      key = "classInfoHolder:" + cls;
    } else {
      key = "classInfoHolder:" + cls + walkPath;
    }
    Reference reference = (Reference) sharedFieldMap.get(key);
    if (reference != null) {
      return reference;
    }
    Expression classInfoHolderExpr =
        inlineInvoke(classResolverRef, "nilClassInfoHolder", classInfoHolderTypeToken);
    String name = ctx.newName(cls, "ClassInfoHolder");
    ctx.addField(true, ctx.type(ClassInfoHolder.class), name, classInfoHolderExpr);
    // The class info field read only once, no need to shallow.
    reference = new Reference(name, classInfoHolderTypeToken);
    sharedFieldMap.put(key, reference);
    return reference;
  }

  protected Expression readClassInfo(Class<?> cls, Expression buffer) {
    return readClassInfo(cls, buffer, true);
  }

  protected Expression readClassInfo(Class<?> cls, Expression buffer, boolean inlineReadClassInfo) {
    if (ReflectionUtils.isMonomorphic(cls)) {
      Reference classInfoRef = addClassInfoField(cls).f0;
      if (inlineReadClassInfo) {
        return inlineInvoke(
            classResolverRef, "readClassInfo", classInfoTypeToken, buffer, classInfoRef);
      } else {
        return new Invoke(
            classResolverRef, "readClassInfo", classInfoTypeToken, buffer, classInfoRef);
      }
    }
    Reference classInfoHolderRef = addClassInfoHolderField(cls);
    if (inlineReadClassInfo) {
      return inlineInvoke(
          classResolverRef, "readClassInfo", classInfoTypeToken, buffer, classInfoHolderRef);
    } else {
      return new Invoke(
          classResolverRef, "readClassInfo", classInfoTypeToken, buffer, classInfoHolderRef);
    }
  }

  protected TypeToken<?> getSerializerType(TypeToken<?> objType) {
    Class<?> rawType = getRawType(objType);
    if (classResolver.isCollection(rawType)) {
      return COLLECTION_SERIALIZER_TYPE;
    } else if (classResolver.isMap(rawType)) {
      return MAP_SERIALIZER_TYPE;
    }
    return SERIALIZER_TYPE;
  }

  /**
   * Return an expression to write a collection to <code>buffer</code>. This expression can have
   * better efficiency for final element type. For final element type, it doesn't have to write
   * class info, no need to forward to <code>fury</code>.
   *
   * @param generateNewMethod Generated code for nested container will be greater than 325 bytes,
   *     which is not possible for inlining, and if code is bigger, jit compile may also be skipped.
   */
  protected Expression serializeForCollection(
      Expression buffer,
      Expression collection,
      TypeToken<?> typeToken,
      Expression serializer,
      boolean generateNewMethod) {
    // get serializer, write class info if necessary.
    if (serializer == null) {
      Class<?> clz = getRawType(typeToken);
      if (isMonomorphic(clz)) {
        serializer = getOrCreateSerializer(clz);
      } else {
        ListExpression writeClassAction = new ListExpression();
        Tuple2<Reference, Boolean> classInfoRef = addClassInfoField(clz);
        Expression classInfo = classInfoRef.f0;
        Expression clsExpr = new Invoke(collection, "getClass", "cls", CLASS_TYPE);
        writeClassAction.add(
            new If(
                neq(new Invoke(classInfo, "getCls", CLASS_TYPE), clsExpr),
                new Assign(
                    classInfo,
                    inlineInvoke(classResolverRef, "getClassInfo", classInfoTypeToken, clsExpr))));
        writeClassAction.add(
            fury.getClassResolver().writeClassExpr(classResolverRef, buffer, classInfo));
        serializer = new Invoke(classInfo, "getSerializer", "serializer", SERIALIZER_TYPE, false);
        serializer = new Cast(serializer, TypeToken.of(AbstractCollectionSerializer.class));
        writeClassAction.add(serializer, new Return(serializer));
        // Spit this into a separate method to avoid method too big to inline.
        serializer =
            invokeGenerated(
                ctx,
                ImmutableSet.of(buffer, collection),
                writeClassAction,
                "writeCollectionClassInfo",
                false);
      }
    } else if (!TypeToken.of(AbstractCollectionSerializer.class).isSupertypeOf(serializer.type())) {
      serializer =
          new Cast(serializer, TypeToken.of(AbstractCollectionSerializer.class), "colSerializer");
    }
    // write collection data.
    ListExpression actions = new ListExpression();
    Expression write =
        new If(
            inlineInvoke(serializer, "supportCodegenHook", PRIMITIVE_BOOLEAN_TYPE),
            writeCollectionData(buffer, collection, serializer, getElementType(typeToken)),
            new Invoke(serializer, "write", buffer, collection));
    actions.add(write);
    if (generateNewMethod) {
      return invokeGenerated(
          ctx, ImmutableSet.of(buffer, collection, serializer), actions, "writeCollection", false);
    }
    return actions;
  }

  protected Expression writeCollectionData(
      Expression buffer, Expression collection, Expression serializer, TypeToken<?> elementType) {
    Invoke onCollectionWrite =
        new Invoke(
            serializer,
            "onCollectionWrite",
            TypeUtils.collectionOf(elementType),
            buffer,
            collection);
    boolean isList = List.class.isAssignableFrom(getRawType(collection.type()));
    collection =
        isList ? new Cast(onCollectionWrite.inline(), LIST_TYPE, "list") : onCollectionWrite;
    Expression size = new Invoke(collection, "size", PRIMITIVE_INT_TYPE);
    walkPath.add(elementType.toString());
    ListExpression builder = new ListExpression();
    Class<?> elemClass = TypeUtils.getRawType(elementType);
    boolean trackingRef = visitFury(fury -> fury.getClassResolver().needToWriteRef(elemClass));
    Tuple2<Expression, Invoke> writeElementsHeader =
        writeElementsHeader(elemClass, trackingRef, serializer, buffer, collection);
    Expression flags = writeElementsHeader.f0;
    builder.add(flags);
    boolean finalType = isMonomorphic(elemClass);
    if (finalType) {
      if (trackingRef) {
        builder.add(
            writeContainerElements(elementType, true, null, null, buffer, collection, size));
      } else {
        Literal hasNullFlag = Literal.ofInt(CollectionFlags.HAS_NULL);
        Expression hasNull = eq(new BitAnd(flags, hasNullFlag), hasNullFlag, "hasNull");
        builder.add(
            hasNull,
            writeContainerElements(elementType, false, null, hasNull, buffer, collection, size));
      }
    } else {
      Literal flag = Literal.ofInt(CollectionFlags.NOT_SAME_TYPE);
      Expression sameElementClass = neq(new BitAnd(flags, flag), flag, "sameElementClass");
      builder.add(sameElementClass);
      //  if ((flags & Flags.NOT_DECL_ELEMENT_TYPE) == Flags.NOT_DECL_ELEMENT_TYPE)
      Literal notDeclTypeFlag = Literal.ofInt(CollectionFlags.NOT_DECL_ELEMENT_TYPE);
      Expression isDeclType = neq(new BitAnd(flags, notDeclTypeFlag), notDeclTypeFlag);
      Expression elemSerializer; // make it in scope of `if(sameElementClass)`
      boolean maybeDecl = visitFury(f -> f.getClassResolver().isSerializable(elemClass));
      TypeToken<?> serializerType = getSerializerType(elementType);
      if (maybeDecl) {
        elemSerializer =
            new If(
                isDeclType,
                new Cast(getOrCreateSerializer(elemClass), serializerType),
                new Cast(writeElementsHeader.f1.inline(), serializerType),
                false,
                serializerType);
      } else {
        elemSerializer = new Cast(writeElementsHeader.f1.inline(), serializerType);
      }
      elemSerializer = uninline(elemSerializer);
      Expression action;
      if (trackingRef) {
        ListExpression writeBuilder = new ListExpression(elemSerializer);
        writeBuilder.add(
            writeContainerElements(
                elementType, true, elemSerializer, null, buffer, collection, size));
        Set<Expression> cutPoint = ofHashSet(buffer, collection, size);
        if (maybeDecl) {
          cutPoint.add(flags);
        }
        action =
            new If(
                sameElementClass,
                invokeGenerated(ctx, cutPoint, writeBuilder, "sameElementClassWrite", false),
                writeContainerElements(elementType, true, null, null, buffer, collection, size));
      } else {
        Literal hasNullFlag = Literal.ofInt(CollectionFlags.HAS_NULL);
        Expression hasNull = eq(new BitAnd(flags, hasNullFlag), hasNullFlag, "hasNull");
        builder.add(hasNull);
        ListExpression writeBuilder = new ListExpression(elemSerializer);
        writeBuilder.add(
            writeContainerElements(
                elementType, false, elemSerializer, hasNull, buffer, collection, size));
        Set<Expression> cutPoint = ofHashSet(buffer, collection, size, hasNull);
        if (maybeDecl) {
          cutPoint.add(flags);
        }
        action =
            new If(
                sameElementClass,
                invokeGenerated(ctx, cutPoint, writeBuilder, "sameElementClassWrite", false),
                writeContainerElements(
                    elementType, false, null, hasNull, buffer, collection, size));
      }
      builder.add(action);
    }
    walkPath.removeLast();
    return new ListExpression(onCollectionWrite, new If(gt(size, Literal.ofInt(0)), builder));
  }

  /**
   * Write collection elements header: flags and maybe elements classinfo. Keep this consistent with
   * `AbstractCollectionSerializer#writeElementsHeader`.
   *
   * @return Tuple(flags, Nullable ( element serializer))
   */
  private Tuple2<Expression, Invoke> writeElementsHeader(
      Class<?> elementType,
      boolean trackingRef,
      Expression collectionSerializer,
      Expression buffer,
      Expression value) {
    if (isMonomorphic(elementType)) {
      Expression bitmap;
      if (trackingRef) {
        bitmap =
            new ListExpression(
                new Invoke(buffer, "writeByte", Literal.ofInt(CollectionFlags.TRACKING_REF)),
                Literal.ofInt(CollectionFlags.TRACKING_REF));
      } else {
        bitmap =
            new Invoke(
                collectionSerializer, "writeNullabilityHeader", PRIMITIVE_INT_TYPE, buffer, value);
      }
      return Tuple2.of(bitmap, null);
    } else {
      Expression elementTypeExpr = getClassExpr(elementType);
      Expression classInfoHolder = addClassInfoHolderField(elementType);
      Expression bitmap;
      if (trackingRef) {
        if (elementType == Object.class) {
          bitmap =
              new Invoke(
                  collectionSerializer,
                  "writeTypeHeader",
                  PRIMITIVE_INT_TYPE,
                  buffer,
                  value,
                  classInfoHolder);
        } else {
          bitmap =
              new Invoke(
                  collectionSerializer,
                  "writeTypeHeader",
                  PRIMITIVE_INT_TYPE,
                  buffer,
                  value,
                  elementTypeExpr,
                  classInfoHolder);
        }
      } else {
        bitmap =
            new Invoke(
                collectionSerializer,
                "writeTypeNullabilityHeader",
                PRIMITIVE_INT_TYPE,
                buffer,
                value,
                elementTypeExpr,
                classInfoHolder);
      }
      Invoke serializer = new Invoke(classInfoHolder, "getSerializer", SERIALIZER_TYPE);
      return Tuple2.of(bitmap, serializer);
    }
  }

  private Expression writeContainerElements(
      TypeToken<?> elementType,
      boolean trackingRef,
      Expression serializer,
      Expression hasNull,
      Expression buffer,
      Expression collection,
      Expression size) {
    ExprHolder exprHolder =
        ExprHolder.of("buffer", buffer, "hasNull", hasNull, "serializer", serializer);
    // If `List#get` raise UnsupportedException, we should make this collection class un-jit able.
    boolean isList = List.class.isAssignableFrom(getRawType(collection.type()));
    if (isList) {
      exprHolder.add("list", collection);
      return new ForLoop(
          new Literal(0, PRIMITIVE_INT_TYPE),
          size,
          new Literal(1, PRIMITIVE_INT_TYPE),
          i -> {
            Invoke elem = new Invoke(exprHolder.get("list"), "get", OBJECT_TYPE, false, i);
            return writeContainerElement(
                exprHolder.get("buffer"),
                elem,
                elementType,
                trackingRef,
                exprHolder.get("hasNull"),
                exprHolder.get("serializer"));
          });
    } else {
      return new ForEach(
          collection,
          (i, elem) ->
              writeContainerElement(
                  exprHolder.get("buffer"),
                  elem,
                  elementType,
                  trackingRef,
                  exprHolder.get("hasNull"),
                  exprHolder.get("serializer")));
    }
  }

  private Expression writeContainerElement(
      Expression buffer,
      Expression elem,
      TypeToken<?> elementType,
      boolean trackingRef,
      Expression hasNull,
      Expression elemSerializer) {
    boolean generateNewMethod =
        useCollectionSerialization(elementType) || useMapSerialization(elementType);
    Class<?> rawType = getRawType(elementType);
    boolean finalType = isMonomorphic(rawType);
    elem = tryCastIfPublic(elem, elementType);
    Expression write;
    if (finalType) {
      if (trackingRef) {
        write =
            new If(
                not(writeRefOrNull(buffer, elem)),
                serializeForNotNull(elem, buffer, elementType, generateNewMethod));
      } else {
        write =
            new If(
                hasNull,
                serializeFor(elem, buffer, elementType, generateNewMethod),
                serializeForNotNull(elem, buffer, elementType));
      }
    } else {
      if (trackingRef) {
        write =
            new If(
                not(writeRefOrNull(buffer, elem)),
                serializeForNotNull(elem, buffer, elementType, elemSerializer, generateNewMethod));
      } else {
        write =
            new If(
                hasNull,
                serializeFor(elem, buffer, elementType, elemSerializer, generateNewMethod),
                serializeForNotNull(elem, buffer, elementType, elemSerializer, generateNewMethod));
      }
    }
    return new ListExpression(elem, write);
  }

  /**
   * Return an expression to write a map to <code>buffer</code>. This expression can have better
   * efficiency for final key/value type. For final key/value type, it doesn't have to write class
   * info, no need to forward to <code>fury</code>.
   */
  protected Expression serializeForMap(
      Expression buffer,
      Expression map,
      TypeToken<?> typeToken,
      Expression serializer,
      boolean generateNewMethod) {
    if (serializer == null) {
      Class<?> clz = getRawType(typeToken);
      if (isMonomorphic(clz)) {
        serializer = getOrCreateSerializer(clz);
      } else {
        ListExpression writeClassAction = new ListExpression();
        Tuple2<Reference, Boolean> classInfoRef = addClassInfoField(clz);
        Expression classInfo = classInfoRef.f0;
        Expression clsExpr = new Invoke(map, "getClass", "cls", CLASS_TYPE);
        writeClassAction.add(
            new If(
                neq(new Invoke(classInfo, "getCls", CLASS_TYPE), clsExpr),
                new Assign(
                    classInfo,
                    inlineInvoke(classResolverRef, "getClassInfo", classInfoTypeToken, clsExpr))));
        // Note: writeClassExpr is thread safe.
        writeClassAction.add(
            fury.getClassResolver().writeClassExpr(classResolverRef, buffer, classInfo));
        serializer = new Invoke(classInfo, "getSerializer", "serializer", SERIALIZER_TYPE, false);
        serializer = new Cast(serializer, TypeToken.of(AbstractMapSerializer.class));
        writeClassAction.add(serializer, new Return(serializer));
        // Spit this into a separate method to avoid method too big to inline.
        serializer =
            invokeGenerated(
                ctx, ImmutableSet.of(buffer, map), writeClassAction, "writeMapClassInfo", false);
      }
    } else if (!AbstractMapSerializer.class.isAssignableFrom(serializer.type().getRawType())) {
      serializer = new Cast(serializer, TypeToken.of(AbstractMapSerializer.class), "mapSerializer");
    }
    Expression write =
        new If(
            inlineInvoke(serializer, "supportCodegenHook", PRIMITIVE_BOOLEAN_TYPE),
            jitWriteMap(buffer, map, serializer, typeToken),
            new Invoke(serializer, "write", buffer, map));
    if (generateNewMethod) {
      return invokeGenerated(ctx, ImmutableSet.of(buffer, map), write, "writeMap", false);
    }
    return write;
  }

  private Expression jitWriteMap(
      Expression buffer, Expression map, Expression serializer, TypeToken<?> typeToken) {
    Tuple2<TypeToken<?>, TypeToken<?>> keyValueType = TypeUtils.getMapKeyValueType(typeToken);
    TypeToken<?> keyType = keyValueType.f0;
    TypeToken<?> valueType = keyValueType.f1;
    Invoke onMapWrite =
        new Invoke(serializer, "onMapWrite", TypeUtils.mapOf(keyType, valueType), buffer, map);
    map = onMapWrite;
    Invoke size = new Invoke(map, "size", PRIMITIVE_INT_TYPE);
    Invoke entrySet = new Invoke(map, "entrySet", "entrySet", SET_TYPE);
    ExprHolder exprHolder = ExprHolder.of("buffer", buffer);
    ForEach writeKeyValues =
        new ForEach(
            entrySet,
            (i, entryObj) -> {
              Expression entry = new Cast(entryObj, TypeToken.of(Map.Entry.class), "entry");
              Expression key = new Invoke(entry, "getKey", "keyObj", OBJECT_TYPE);
              key = tryCastIfPublic(key, keyType, "key");
              Expression value = new Invoke(entry, "getValue", "valueObj", OBJECT_TYPE);
              value = tryCastIfPublic(value, valueType, "value");
              walkPath.add("key:" + keyType);
              boolean genMethodForKey =
                  useCollectionSerialization(keyType) || useMapSerialization(keyType);
              Expression keyAction =
                  serializeFor(key, exprHolder.get("buffer"), keyType, genMethodForKey);
              walkPath.removeLast();
              walkPath.add("value:" + valueType);
              boolean genMethodForValue =
                  useCollectionSerialization(valueType) || useMapSerialization(valueType);
              Expression valueAction =
                  serializeFor(value, exprHolder.get("buffer"), valueType, genMethodForValue);
              walkPath.removeLast();
              return new ListExpression(keyAction, valueAction);
            });
    return new ListExpression(onMapWrite, writeKeyValues);
  }

  protected Expression readRefOrNull(Expression buffer) {
    return new Invoke(refResolverRef, "readRefOrNull", "tag", PRIMITIVE_BYTE_TYPE, false, buffer);
  }

  protected Expression tryPreserveRefId(Expression buffer) {
    return new Invoke(
        refResolverRef, "tryPreserveRefId", "refId", PRIMITIVE_INT_TYPE, false, buffer);
  }

  protected Expression deserializeFor(
      Expression buffer, TypeToken<?> typeToken, Function<Expression, Expression> callback) {
    return deserializeFor(buffer, typeToken, callback, null);
  }

  /**
   * Returns an expression that deserialize a nullable <code>inputObject</code> from <code>buffer
   * </code>.
   *
   * @param callback is used to consume the deserialized value to avoid an extra condition branch.
   */
  protected Expression deserializeFor(
      Expression buffer,
      TypeToken<?> typeToken,
      Function<Expression, Expression> callback,
      CutPoint cutPoint) {
    Class<?> rawType = getRawType(typeToken);
    if (visitFury(f -> f.getClassResolver().needToWriteRef(rawType))) {
      return readRef(buffer, callback, () -> deserializeForNotNull(buffer, typeToken, cutPoint));
    } else {
      if (typeToken.isPrimitive()) {
        Expression value = deserializeForNotNull(buffer, typeToken, cutPoint);
        // Should put value expr ahead to avoid generated code in wrong scope.
        return new ListExpression(value, callback.apply(value));
      }
      return readNullable(
          buffer, typeToken, callback, () -> deserializeForNotNull(buffer, typeToken, cutPoint));
    }
  }

  private Expression readRef(
      Expression buffer,
      Function<Expression, Expression> callback,
      Supplier<Expression> deserializeForNotNull) {
    Expression refId = tryPreserveRefId(buffer);
    // indicates that the object is first read.
    Expression needDeserialize =
        ExpressionUtils.egt(refId, new Literal(Fury.NOT_NULL_VALUE_FLAG, PRIMITIVE_BYTE_TYPE));
    Expression deserializedValue = deserializeForNotNull.get();
    Expression setReadObject =
        new Invoke(refResolverRef, "setReadObject", refId, deserializedValue);
    Expression readValue = inlineInvoke(refResolverRef, "getReadObject", OBJECT_TYPE, false);
    // use false to ignore null
    return new If(
        needDeserialize,
        callback.apply(
            new ListExpression(refId, deserializedValue, setReadObject, deserializedValue)),
        callback.apply(readValue),
        false);
  }

  private Expression readNullable(
      Expression buffer,
      TypeToken<?> typeToken,
      Function<Expression, Expression> callback,
      Supplier<Expression> deserializeForNotNull) {
    Expression notNull =
        neq(
            inlineInvoke(buffer, "readByte", PRIMITIVE_BYTE_TYPE),
            new Literal(Fury.NULL_FLAG, PRIMITIVE_BYTE_TYPE));
    Expression value = deserializeForNotNull.get();
    // use false to ignore null.
    return new If(notNull, callback.apply(value), callback.apply(nullValue(typeToken)), false);
  }

  protected Expression deserializeForNotNull(
      Expression buffer, TypeToken<?> typeToken, CutPoint cutPoint) {
    return deserializeForNotNull(buffer, typeToken, null, cutPoint);
  }

  /**
   * Return an expression that deserialize an not null <code>inputObject</code> from <code>buffer
   * </code>.
   *
   * @param cutPoint for generate new method to cut off dependencies.
   */
  protected Expression deserializeForNotNull(
      Expression buffer, TypeToken<?> typeToken, Expression serializer, CutPoint cutPoint) {
    Class<?> cls = getRawType(typeToken);
    if (isPrimitive(cls) || isBoxed(cls)) {
      // for primitive, inline call here to avoid java boxing, rather call corresponding serializer.
      if (cls == byte.class || cls == Byte.class) {
        return new Invoke(buffer, "readByte", PRIMITIVE_BYTE_TYPE);
      } else if (cls == boolean.class || cls == Boolean.class) {
        return new Invoke(buffer, "readBoolean", PRIMITIVE_BOOLEAN_TYPE);
      } else if (cls == char.class || cls == Character.class) {
        return readChar(buffer);
      } else if (cls == short.class || cls == Short.class) {
        return readInt16(buffer);
      } else if (cls == int.class || cls == Integer.class) {
        return fury.compressInt() ? readVarInt32(buffer) : readInt32(buffer);
      } else if (cls == long.class || cls == Long.class) {
        return LongSerializer.readInt64(buffer, fury.longEncoding());
      } else if (cls == float.class || cls == Float.class) {
        return readFloat32(buffer);
      } else if (cls == double.class || cls == Double.class) {
        return readFloat64(buffer);
      } else {
        throw new IllegalStateException("impossible");
      }
    } else {
      if (cls == String.class) {
        return fury.getStringSerializer().readStringExpr(stringSerializerRef, buffer);
      }
      Expression obj;
      if (useCollectionSerialization(typeToken)) {
        obj = deserializeForCollection(buffer, typeToken, serializer, cutPoint);
      } else if (useMapSerialization(typeToken)) {
        obj = deserializeForMap(buffer, typeToken, serializer, cutPoint);
      } else {
        if (isMonomorphic(cls)) {
          Preconditions.checkState(serializer == null);
          serializer = getOrCreateSerializer(cls);
          Class<?> returnType =
              ReflectionUtils.getReturnType(getRawType(serializer.type()), "read");
          obj = new Invoke(serializer, "read", TypeToken.of(returnType), buffer);
        } else {
          obj = readForNotNullNonFinal(buffer, typeToken, serializer);
        }
      }
      return obj;
    }
  }

  protected Expression readForNotNullNonFinal(
      Expression buffer, TypeToken<?> typeToken, Expression serializer) {
    if (serializer == null) {
      Expression classInfo = readClassInfo(getRawType(typeToken), buffer);
      serializer = inlineInvoke(classInfo, "getSerializer", SERIALIZER_TYPE);
    }
    return new Invoke(serializer, "read", OBJECT_TYPE, buffer);
  }

  /**
   * Return an expression to deserialize a collection from <code>buffer</code>. Must keep consistent
   * with {@link BaseObjectCodecBuilder#serializeForCollection}
   */
  protected Expression deserializeForCollection(
      Expression buffer, TypeToken<?> typeToken, Expression serializer, CutPoint cutPoint) {
    TypeToken<?> elementType = getElementType(typeToken);
    if (serializer == null) {
      Class<?> cls = getRawType(typeToken);
      if (isMonomorphic(cls)) {
        serializer = getOrCreateSerializer(cls);
      } else {
        Expression classInfo = readClassInfo(cls, buffer);
        serializer = new Invoke(classInfo, "getSerializer", "serializer", SERIALIZER_TYPE, false);
        serializer =
            new Cast(
                serializer,
                TypeToken.of(AbstractCollectionSerializer.class),
                "collectionSerializer");
      }
    } else {
      checkArgument(
          AbstractCollectionSerializer.class.isAssignableFrom(serializer.type().getRawType()),
          "Expected AbstractCollectionSerializer but got %s",
          serializer.type());
    }
    Invoke supportHook = inlineInvoke(serializer, "supportCodegenHook", PRIMITIVE_BOOLEAN_TYPE);
    Expression collection = new Invoke(serializer, "newCollection", COLLECTION_TYPE, buffer);
    Expression size = new Invoke(serializer, "getAndClearNumElements", "size", PRIMITIVE_INT_TYPE);
    // if add branch by `ArrayList`, generated code will be > 325 bytes.
    // and List#add is more likely be inlined if there is only one subclass.
    Expression hookRead = readCollectionCodegen(buffer, collection, size, elementType);
    hookRead = new Invoke(serializer, "onCollectionRead", OBJECT_TYPE, hookRead);
    Expression action =
        new If(
            supportHook,
            new ListExpression(collection, hookRead),
            new Invoke(serializer, "read", OBJECT_TYPE, buffer),
            false);
    if (cutPoint != null && cutPoint.genNewMethod) {
      cutPoint.add(buffer);
      return invokeGenerated(
          ctx,
          cutPoint.cutPoints,
          new ListExpression(action, new Return(action)),
          "readCollection",
          false);
    }
    return action;
  }

  protected Expression readCollectionCodegen(
      Expression buffer, Expression collection, Expression size, TypeToken<?> elementType) {
    ListExpression builder = new ListExpression();
    Invoke flags = new Invoke(buffer, "readByte", "flags", PRIMITIVE_INT_TYPE, false);
    builder.add(flags);
    Class<?> elemClass = TypeUtils.getRawType(elementType);
    walkPath.add(elementType.toString());
    boolean finalType = isMonomorphic(elemClass);
    boolean trackingRef = visitFury(fury -> fury.getClassResolver().needToWriteRef(elemClass));
    if (finalType) {
      if (trackingRef) {
        builder.add(readContainerElements(elementType, true, null, null, buffer, collection, size));
      } else {
        Literal hasNullFlag = Literal.ofInt(CollectionFlags.HAS_NULL);
        Expression hasNull = eq(new BitAnd(flags.inline(), hasNullFlag), hasNullFlag, "hasNull");
        builder.add(
            hasNull,
            readContainerElements(elementType, false, null, hasNull, buffer, collection, size));
      }
    } else {
      Literal notSameTypeFlag = Literal.ofInt(CollectionFlags.NOT_SAME_TYPE);
      Expression sameElementClass =
          neq(new BitAnd(flags, notSameTypeFlag), notSameTypeFlag, "sameElementClass");
      //  if ((flags & Flags.NOT_DECL_ELEMENT_TYPE) == Flags.NOT_DECL_ELEMENT_TYPE)
      Literal notDeclTypeFlag = Literal.ofInt(CollectionFlags.NOT_DECL_ELEMENT_TYPE);
      Expression isDeclType = neq(new BitAnd(flags, notDeclTypeFlag), notDeclTypeFlag);
      Invoke serializer =
          inlineInvoke(readClassInfo(elemClass, buffer), "getSerializer", SERIALIZER_TYPE);
      TypeToken<?> serializerType = getSerializerType(elementType);
      Expression elemSerializer; // make it in scope of `if(sameElementClass)`
      boolean maybeDecl = visitFury(f -> f.getClassResolver().isSerializable(elemClass));
      if (maybeDecl) {
        elemSerializer =
            new If(
                isDeclType,
                new Cast(getOrCreateSerializer(elemClass), serializerType),
                new Cast(serializer.inline(), serializerType),
                false,
                serializerType);
      } else {
        elemSerializer = new Cast(serializer.inline(), serializerType);
      }
      elemSerializer = uninline(elemSerializer);
      builder.add(sameElementClass);
      Expression action;
      if (trackingRef) {
        // Same element class read start
        ListExpression readBuilder = new ListExpression(elemSerializer);
        readBuilder.add(
            readContainerElements(
                elementType, true, elemSerializer, null, buffer, collection, size));
        Set<Expression> cutPoint = ofHashSet(buffer, collection, size);
        if (maybeDecl) { // For `isDeclType`
          cutPoint.add(flags);
        }
        Expression sameElementClassRead =
            invokeGenerated(ctx, cutPoint, readBuilder, "sameElementClassRead", false);
        // Same element class read end
        action =
            new If(
                sameElementClass,
                sameElementClassRead,
                readContainerElements(elementType, true, null, null, buffer, collection, size));
      } else {
        Literal hasNullFlag = Literal.ofInt(CollectionFlags.HAS_NULL);
        Expression hasNull = eq(new BitAnd(flags, hasNullFlag), hasNullFlag, "hasNull");
        builder.add(hasNull);
        // Same element class read start
        ListExpression readBuilder = new ListExpression(elemSerializer);
        readBuilder.add(
            readContainerElements(
                elementType, false, elemSerializer, hasNull, buffer, collection, size));
        Set<Expression> cutPoint = ofHashSet(buffer, collection, size, hasNull);
        if (maybeDecl) { // For `isDeclType`
          cutPoint.add(flags);
        }
        // Same element class read end
        Expression sameElementClassRead =
            invokeGenerated(ctx, cutPoint, readBuilder, "sameElementClassRead", false);
        action =
            new If(
                sameElementClass,
                sameElementClassRead,
                readContainerElements(elementType, false, null, hasNull, buffer, collection, size));
      }
      builder.add(action);
    }
    walkPath.removeLast();
    // place newCollection as last as expr value
    return new ListExpression(
        size, collection, new If(gt(size, Literal.ofInt(0)), builder), collection);
  }

  private Expression readContainerElements(
      TypeToken<?> elementType,
      boolean trackingRef,
      Expression serializer,
      Expression hasNull,
      Expression buffer,
      Expression collection,
      Expression size) {
    ExprHolder exprHolder =
        ExprHolder.of(
            "collection",
            collection,
            "buffer",
            buffer,
            "hasNull",
            hasNull,
            "serializer",
            serializer);
    Expression start = new Literal(0, PRIMITIVE_INT_TYPE);
    Expression step = new Literal(1, PRIMITIVE_INT_TYPE);
    return new ForLoop(
        start,
        size,
        step,
        i ->
            readContainerElement(
                exprHolder.get("buffer"),
                elementType,
                trackingRef,
                exprHolder.get("hasNull"),
                exprHolder.get("serializer"),
                v -> new Invoke(exprHolder.get("collection"), "add", inline(v))));
  }

  private Expression readContainerElement(
      Expression buffer,
      TypeToken<?> elementType,
      boolean trackingRef,
      Expression hasNull,
      Expression elemSerializer,
      Function<Expression, Expression> callback) {
    boolean genNewMethod =
        useCollectionSerialization(elementType) || useMapSerialization(elementType);
    CutPoint cutPoint = new CutPoint(genNewMethod, buffer);
    Class<?> rawType = getRawType(elementType);
    boolean finalType = isMonomorphic(rawType);
    Expression read;
    if (finalType) {
      if (trackingRef) {
        read = deserializeFor(buffer, elementType, callback, cutPoint);
      } else {
        cutPoint.add(hasNull);
        read =
            new If(
                hasNull,
                deserializeFor(buffer, elementType, callback, cutPoint),
                callback.apply(deserializeForNotNull(buffer, elementType, cutPoint)));
      }
    } else {
      cutPoint.add(elemSerializer);
      if (trackingRef) {
        // eager callback, no need to use ExprHolder.
        read =
            readRef(
                buffer,
                callback,
                () -> deserializeForNotNull(buffer, elementType, elemSerializer, cutPoint));
      } else {
        cutPoint.add(hasNull);
        read =
            new If(
                hasNull,
                readNullable(
                    buffer,
                    elementType,
                    callback,
                    () -> deserializeForNotNull(buffer, elementType, elemSerializer, cutPoint)),
                callback.apply(
                    deserializeForNotNull(buffer, elementType, elemSerializer, cutPoint)));
      }
    }
    return read;
  }

  /**
   * Return an expression to deserialize a map from <code>buffer</code>. Must keep consistent with
   * {@link BaseObjectCodecBuilder#serializeForMap}
   */
  protected Expression deserializeForMap(
      Expression buffer, TypeToken<?> typeToken, Expression serializer, CutPoint cutPoint) {
    Tuple2<TypeToken<?>, TypeToken<?>> keyValueType = TypeUtils.getMapKeyValueType(typeToken);
    TypeToken<?> keyType = keyValueType.f0;
    TypeToken<?> valueType = keyValueType.f1;
    if (serializer == null) {
      Class<?> cls = getRawType(typeToken);
      if (isMonomorphic(cls)) {
        serializer = getOrCreateSerializer(cls);
      } else {
        Expression classInfo = readClassInfo(cls, buffer);
        serializer = new Invoke(classInfo, "getSerializer", SERIALIZER_TYPE);
        serializer =
            new Cast(serializer, TypeToken.of(AbstractMapSerializer.class), "mapSerializer");
      }
    } else {
      checkArgument(
          AbstractMapSerializer.class.isAssignableFrom(serializer.type().getRawType()),
          "Expected AbstractMapSerializer but got %s",
          serializer.type());
    }
    Invoke supportHook = inlineInvoke(serializer, "supportCodegenHook", PRIMITIVE_BOOLEAN_TYPE);
    Expression newMap = new Invoke(serializer, "newMap", MAP_TYPE, buffer);
    Expression size = new Invoke(serializer, "getAndClearNumElements", "size", PRIMITIVE_INT_TYPE);
    Expression start = new Literal(0, PRIMITIVE_INT_TYPE);
    Expression step = new Literal(1, PRIMITIVE_INT_TYPE);
    ExprHolder exprHolder = ExprHolder.of("map", newMap, "buffer", buffer);
    ForLoop readKeyValues =
        new ForLoop(
            start,
            size,
            step,
            i -> {
              boolean genKeyMethod =
                  useCollectionSerialization(keyType) || useMapSerialization(keyType);
              boolean genValueMethod =
                  useCollectionSerialization(valueType) || useMapSerialization(valueType);
              walkPath.add("key:" + keyType);
              Expression keyAction =
                  deserializeFor(
                      exprHolder.get("buffer"), keyType, e -> e, new CutPoint(genKeyMethod));
              walkPath.removeLast();
              walkPath.add("value:" + valueType);
              Expression valueAction =
                  deserializeFor(
                      exprHolder.get("buffer"), valueType, e -> e, new CutPoint(genValueMethod));
              walkPath.removeLast();
              return new Invoke(exprHolder.get("map"), "put", keyAction, valueAction);
            });
    // first newMap to create map, last newMap as expr value
    Expression hookRead = new ListExpression(newMap, size, readKeyValues, newMap);
    hookRead = new Invoke(serializer, "onMapRead", OBJECT_TYPE, hookRead);
    Expression action =
        new If(supportHook, hookRead, new Invoke(serializer, "read", OBJECT_TYPE, buffer), false);
    if (cutPoint != null && cutPoint.genNewMethod) {
      cutPoint.add(buffer);
      return invokeGenerated(
          ctx,
          cutPoint.cutPoints,
          new ListExpression(action, new Return(action)),
          "readMap",
          false);
    }
    return action;
  }

  @Override
  protected Expression beanClassExpr() {
    if (GraalvmSupport.isGraalBuildtime()) {
      return staticBeanClassExpr();
    }
    // Serializer has a `type` field.
    return new Reference("super.type", CLASS_TYPE);
  }
}
