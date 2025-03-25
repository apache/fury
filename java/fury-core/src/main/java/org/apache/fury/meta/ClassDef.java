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

package org.apache.fury.meta;

import static org.apache.fury.meta.ClassDefEncoder.buildFields;
import static org.apache.fury.type.TypeUtils.COLLECTION_TYPE;
import static org.apache.fury.type.TypeUtils.MAP_TYPE;
import static org.apache.fury.type.TypeUtils.collectionOf;
import static org.apache.fury.type.TypeUtils.getArrayComponent;
import static org.apache.fury.type.TypeUtils.mapOf;

import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.stream.Collectors;
import org.apache.fury.Fury;
import org.apache.fury.builder.MetaSharedCodecBuilder;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.reflect.TypeRef;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.CompatibleSerializer;
import org.apache.fury.serializer.NonexistentClass;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.FinalObjectTypeStub;
import org.apache.fury.type.GenericType;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.util.Preconditions;

/**
 * Serializable class definition to be sent to other process. So if sender peer and receiver peer
 * has different class definition for same class, such as add/remove fields, we can use this
 * definition to create different serializer to support back/forward compatibility.
 *
 * <p>Note that:
 * <li>If a class is already registered, this definition will contain class id only.
 * <li>Sending class definition is not cheap, should be sent with some kind of meta share mechanism.
 * <li>{@link ObjectStreamClass} doesn't contain any non-primitive field type info, which is not
 *     enough to create serializer in receiver.
 *
 * @see MetaSharedCodecBuilder
 * @see CompatibleMode#COMPATIBLE
 * @see CompatibleSerializer
 * @see FuryBuilder#withMetaShare
 * @see ReflectionUtils#getFieldOffset
 */
public class ClassDef implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ClassDef.class);

  static final int SCHEMA_COMPATIBLE_FLAG = 0b10000;
  public static final int SIZE_TWO_BYTES_FLAG = 0b100000;
  static final int OBJECT_TYPE_FLAG = 0b1000000;
  static final int COMPRESSION_FLAG = 0b10000000;
  // TODO use field offset to sort field, which will hit l1-cache more. Since
  // `objectFieldOffset` is not part of jvm-specification, it may change between different jdk
  // vendor. But the deserialization peer use the class definition to create deserializer, it's OK
  // even field offset or fields order change between jvm process.
  public static final Comparator<Field> FIELD_COMPARATOR =
      (f1, f2) -> {
        long offset1 = Platform.objectFieldOffset(f1);
        long offset2 = Platform.objectFieldOffset(f2);
        long diff = offset1 - offset2;
        if (diff != 0) {
          return (int) diff;
        } else {
          if (!f1.equals(f2)) {
            LOG.warn(
                "Field {} has same offset with {}, please an issue with jdk info to fury", f1, f2);
          }
          int compare = f1.getDeclaringClass().getName().compareTo(f2.getName());
          if (compare != 0) {
            return compare;
          }
          return f1.getName().compareTo(f2.getName());
        }
      };

  private final ClassSpec classSpec;
  private final List<FieldInfo> fieldsInfo;
  private final boolean isObjectType;
  // Unique id for class def. If class def are same between processes, then the id will
  // be same too.
  private final long id;
  private final byte[] encoded;
  private transient List<Descriptor> descriptors;

  ClassDef(
      ClassSpec classSpec,
      List<FieldInfo> fieldsInfo,
      boolean isObjectType,
      long id,
      byte[] encoded) {
    this.classSpec = classSpec;
    this.fieldsInfo = fieldsInfo;
    this.isObjectType = isObjectType;
    this.id = id;
    this.encoded = encoded;
  }

  /**
   * Returns class name.
   *
   * @see Class#getName()
   */
  public String getClassName() {
    return classSpec.entireClassName;
  }

  public ClassSpec getClassSpec() {
    return classSpec;
  }

  /** Contain all fields info including all parent classes. */
  public List<FieldInfo> getFieldsInfo() {
    return fieldsInfo;
  }

  /** Returns ext meta for the class. */
  public boolean isObjectType() {
    return isObjectType;
  }

  /**
   * Returns an unique id for class def. If class def are same between processes, then the id will
   * be same too.
   */
  public long getId() {
    return id;
  }

  public byte[] getEncoded() {
    return encoded;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClassDef classDef = (ClassDef) o;
    return Objects.equals(classSpec.entireClassName, classDef.classSpec.entireClassName)
        && Objects.equals(fieldsInfo, classDef.fieldsInfo)
        && Objects.equals(id, classDef.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classSpec.entireClassName, fieldsInfo, id);
  }

  @Override
  public String toString() {
    return "ClassDef{"
        + "className='"
        + classSpec.entireClassName
        + '\''
        + ", fieldsInfo="
        + fieldsInfo
        + ", isObjectType="
        + isObjectType
        + ", id="
        + id
        + '}';
  }

  /** Write class definition to buffer. */
  public void writeClassDef(MemoryBuffer buffer) {
    buffer.writeBytes(encoded, 0, encoded.length);
  }

  /** Read class definition from buffer. */
  public static ClassDef readClassDef(ClassResolver classResolver, MemoryBuffer buffer) {
    return ClassDefDecoder.decodeClassDef(classResolver, buffer, buffer.readInt64());
  }

  /** Read class definition from buffer. */
  public static ClassDef readClassDef(
      ClassResolver classResolver, MemoryBuffer buffer, long header) {
    return ClassDefDecoder.decodeClassDef(classResolver, buffer, header);
  }

  /**
   * Consolidate fields of <code>classDef</code> with <code>cls</code>. If some field exists in
   * <code>cls</code> but not in <code>classDef</code>, it won't be returned in final collection. If
   * some field exists in <code>classDef</code> but not in <code> cls</code>, it will be added to
   * final collection.
   *
   * @param cls class load in current process.
   */
  public List<Descriptor> getDescriptors(ClassResolver resolver, Class<?> cls) {
    if (descriptors == null) {
      SortedMap<Field, Descriptor> allDescriptorsMap = resolver.getAllDescriptorsMap(cls, true);
      Map<String, Descriptor> descriptorsMap = new HashMap<>();
      for (Map.Entry<Field, Descriptor> e : allDescriptorsMap.entrySet()) {
        if (descriptorsMap.put(
                e.getKey().getDeclaringClass().getName() + "." + e.getKey().getName(), e.getValue())
            != null) {
          throw new IllegalStateException("Duplicate key");
        }
      }
      descriptors = new ArrayList<>(fieldsInfo.size());
      for (ClassDef.FieldInfo fieldInfo : fieldsInfo) {
        Descriptor descriptor =
            descriptorsMap.get(fieldInfo.getDefinedClass() + "." + fieldInfo.getFieldName());
        Descriptor newDesc = fieldInfo.toDescriptor(resolver);
        Class<?> rawType = newDesc.getRawType();
        FieldType fieldType = fieldInfo.getFieldType();
        if (fieldType instanceof RegisteredFieldType) {
          String typeAlias = String.valueOf(((RegisteredFieldType) fieldType).getClassId());
          if (!typeAlias.equals(newDesc.getTypeName())) {
            newDesc = newDesc.copyWithTypeName(typeAlias);
          }
        }
        if (descriptor != null) {
          // Make DescriptorGrouper have consistent order whether field exist or not
          // fury builtin types skip
          if (rawType.isEnum()
              || rawType.isAssignableFrom(descriptor.getRawType())
              || NonexistentClass.isNonexistent(rawType)
              || rawType == FinalObjectTypeStub.class
              || (rawType.isArray() && getArrayComponent(rawType) == FinalObjectTypeStub.class)) {
            descriptor = descriptor.copyWithTypeName(newDesc.getTypeName());
            descriptors.add(descriptor);
          } else {
            descriptors.add(newDesc);
          }
        } else {
          descriptors.add(newDesc);
        }
      }
    }
    return descriptors;
  }

  /**
   * FieldInfo contains all necessary info of a field to execute serialization/deserialization
   * logic.
   */
  public static class FieldInfo implements Serializable {
    /** where are current field defined. */
    private final String definedClass;

    /** Name of a field. */
    private final String fieldName;

    private final FieldType fieldType;

    FieldInfo(String definedClass, String fieldName, FieldType fieldType) {
      this.definedClass = definedClass;
      this.fieldName = fieldName;
      this.fieldType = fieldType;
    }

    /** Returns classname of current field defined. */
    public String getDefinedClass() {
      return definedClass;
    }

    /** Returns name of current field. */
    public String getFieldName() {
      return fieldName;
    }

    public boolean hasTypeTag() {
      return false;
    }

    public short getTypeTag() {
      return -1;
    }

    /** Returns type of current field. */
    public FieldType getFieldType() {
      return fieldType;
    }

    /**
     * Convert this field into a {@link Descriptor}, the corresponding {@link Field} field will be
     * null. Don't invoke this method if class does have <code>fieldName</code> field. In such case,
     * reflection should be used to get the descriptor.
     */
    Descriptor toDescriptor(ClassResolver classResolver) {
      TypeRef<?> typeRef = fieldType.toTypeToken(classResolver);
      // This field doesn't exist in peer class, so any legal modifier will be OK.
      int stubModifiers = ReflectionUtils.getField(getClass(), "fieldName").getModifiers();
      return new Descriptor(typeRef, fieldName, stubModifiers, definedClass);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FieldInfo fieldInfo = (FieldInfo) o;
      return Objects.equals(definedClass, fieldInfo.definedClass)
          && Objects.equals(fieldName, fieldInfo.fieldName)
          && Objects.equals(fieldType, fieldInfo.fieldType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(definedClass, fieldName, fieldType);
    }

    @Override
    public String toString() {
      return "FieldInfo{"
          + "definedClass='"
          + definedClass
          + '\''
          + ", fieldName='"
          + fieldName
          + '\''
          + ", fieldType="
          + fieldType
          + '}';
    }
  }

  public abstract static class FieldType implements Serializable {
    public FieldType(boolean isMonomorphic, boolean trackingRef) {
      this.isMonomorphic = isMonomorphic;
      this.trackingRef = trackingRef;
    }

    protected final boolean isMonomorphic;
    protected final boolean trackingRef;

    public boolean isMonomorphic() {
      return isMonomorphic;
    }

    public boolean trackingRef() {
      return trackingRef;
    }

    /**
     * Convert a serializable field type to type token. If field type is a generic type with
     * generics, the generics will be built up recursively. The final leaf object type will be built
     * from class id or class stub.
     *
     * @see FinalObjectTypeStub
     */
    public abstract TypeRef<?> toTypeToken(ClassResolver classResolver);

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FieldType fieldType = (FieldType) o;
      return isMonomorphic == fieldType.isMonomorphic && trackingRef == fieldType.trackingRef;
    }

    @Override
    public int hashCode() {
      return Objects.hash(isMonomorphic, trackingRef);
    }

    /** Write field type info. */
    public void write(MemoryBuffer buffer, boolean writeHeader) {
      byte header = (byte) ((isMonomorphic ? 1 : 0) << 1);
      // header of nested generic fields in collection/map will be written independently
      header |= (byte) (trackingRef ? 1 : 0);
      if (this instanceof ClassDef.RegisteredFieldType) {
        short classId = ((ClassDef.RegisteredFieldType) this).getClassId();
        buffer.writeVarUint32Small7(writeHeader ? ((5 + classId) << 2) | header : 5 + classId);
      } else if (this instanceof ClassDef.EnumFieldType) {
        buffer.writeVarUint32Small7(writeHeader ? ((4) << 2) | header : 4);
      } else if (this instanceof ClassDef.ArrayFieldType) {
        ClassDef.ArrayFieldType arrayFieldType = (ClassDef.ArrayFieldType) this;
        buffer.writeVarUint32Small7(writeHeader ? ((3) << 2) | header : 3);
        buffer.writeVarUint32Small7(arrayFieldType.getDimensions());
        (arrayFieldType).getComponentType().write(buffer);
      } else if (this instanceof ClassDef.CollectionFieldType) {
        buffer.writeVarUint32Small7(writeHeader ? ((2) << 2) | header : 2);
        // TODO remove it when new collection deserialization jit finished.
        ((ClassDef.CollectionFieldType) this).getElementType().write(buffer);
      } else if (this instanceof ClassDef.MapFieldType) {
        buffer.writeVarUint32Small7(writeHeader ? ((1) << 2) | header : 1);
        // TODO remove it when new map deserialization jit finished.
        ClassDef.MapFieldType mapFieldType = (ClassDef.MapFieldType) this;
        mapFieldType.getKeyType().write(buffer);
        mapFieldType.getValueType().write(buffer);
      } else {
        Preconditions.checkArgument(this instanceof ClassDef.ObjectFieldType);
        buffer.writeVarUint32Small7(writeHeader ? header : 0);
      }
    }

    public void write(MemoryBuffer buffer) {
      write(buffer, true);
    }

    public static FieldType read(MemoryBuffer buffer) {
      int header = buffer.readVarUint32Small7();
      boolean isMonomorphic = (header & 0b10) != 0;
      boolean trackingRef = (header & 0b1) != 0;
      return read(buffer, isMonomorphic, trackingRef, header >>> 2);
    }

    /** Read field type info. */
    public static FieldType read(
        MemoryBuffer buffer, boolean isFinal, boolean trackingRef, int typeId) {
      if (typeId == 0) {
        return new ObjectFieldType(isFinal, trackingRef);
      } else if (typeId == 1) {
        return new MapFieldType(isFinal, trackingRef, read(buffer), read(buffer));
      } else if (typeId == 2) {
        return new CollectionFieldType(isFinal, trackingRef, read(buffer));
      } else if (typeId == 3) {
        int dims = buffer.readVarUint32Small7();
        return new ArrayFieldType(isFinal, trackingRef, read(buffer), dims);
      } else if (typeId == 4) {
        return EnumFieldType.getInstance();
      } else {
        return new RegisteredFieldType(isFinal, trackingRef, (short) (typeId - 5));
      }
    }
  }

  /** Class for field type which is registered. */
  public static class RegisteredFieldType extends FieldType {
    private final short classId;

    public RegisteredFieldType(boolean isFinal, boolean trackingRef, short classId) {
      super(isFinal, trackingRef);
      this.classId = classId;
    }

    public short getClassId() {
      return classId;
    }

    @Override
    public TypeRef<?> toTypeToken(ClassResolver classResolver) {
      Class<?> cls = classResolver.getRegisteredClass(classId);
      if (cls == null) {
        LOG.warn("Class {} not registered, take it as Struct type for deserialization.", classId);
        cls = NonexistentClass.NonexistentMetaShared.class;
      }
      return TypeRef.of(cls, new TypeExtMeta(trackingRef));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      RegisteredFieldType that = (RegisteredFieldType) o;
      return classId == that.classId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), classId);
    }

    @Override
    public String toString() {
      return "RegisteredFieldType{"
          + "isMonomorphic="
          + isMonomorphic()
          + ", trackingRef="
          + trackingRef()
          + ", classId="
          + classId
          + '}';
    }
  }

  /**
   * Class for collection field type, which store collection element type information. Nested
   * collection/map generics example:
   *
   * <pre>{@code
   * new TypeToken<Collection<Map<String, String>>>() {}
   * }</pre>
   */
  public static class CollectionFieldType extends FieldType {
    private final FieldType elementType;

    public CollectionFieldType(boolean isFinal, boolean trackingRef, FieldType elementType) {
      super(isFinal, trackingRef);
      this.elementType = elementType;
    }

    public FieldType getElementType() {
      return elementType;
    }

    @Override
    public TypeRef<?> toTypeToken(ClassResolver classResolver) {
      // TODO support preserve element TypeExtMeta
      return collectionOf(elementType.toTypeToken(classResolver), new TypeExtMeta(trackingRef));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      CollectionFieldType that = (CollectionFieldType) o;
      return Objects.equals(elementType, that.elementType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), elementType);
    }

    @Override
    public String toString() {
      return "CollectionFieldType{"
          + "elementType="
          + elementType
          + ", isFinal="
          + isMonomorphic()
          + ", trackingRef="
          + trackingRef()
          + '}';
    }
  }

  /**
   * Class for map field type, which store map key/value type information. Nested map generics
   * example:
   *
   * <pre>{@code
   * new TypeToken<Map<List<String>>, String>() {}
   * }</pre>
   */
  public static class MapFieldType extends FieldType {
    private final FieldType keyType;
    private final FieldType valueType;

    public MapFieldType(
        boolean isFinal, boolean trackingRef, FieldType keyType, FieldType valueType) {
      super(isFinal, trackingRef);
      this.keyType = keyType;
      this.valueType = valueType;
    }

    public FieldType getKeyType() {
      return keyType;
    }

    public FieldType getValueType() {
      return valueType;
    }

    @Override
    public TypeRef<?> toTypeToken(ClassResolver classResolver) {
      // TODO support preserve element TypeExtMeta, it will be lost when building other TypeRef
      return mapOf(
          keyType.toTypeToken(classResolver),
          valueType.toTypeToken(classResolver),
          new TypeExtMeta(trackingRef));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      MapFieldType that = (MapFieldType) o;
      return Objects.equals(keyType, that.keyType) && Objects.equals(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), keyType, valueType);
    }

    @Override
    public String toString() {
      return "MapFieldType{"
          + "keyType="
          + keyType
          + ", valueType="
          + valueType
          + ", isFinal="
          + isMonomorphic()
          + ", trackingRef="
          + trackingRef()
          + '}';
    }
  }

  public static class EnumFieldType extends FieldType {
    private static final EnumFieldType INSTANCE = new EnumFieldType();

    private EnumFieldType() {
      super(true, false);
    }

    @Override
    public TypeRef<?> toTypeToken(ClassResolver classResolver) {
      return TypeRef.of(NonexistentClass.NonexistentEnum.class);
    }

    public static EnumFieldType getInstance() {
      return INSTANCE;
    }
  }

  public static class ArrayFieldType extends FieldType {
    private final FieldType componentType;
    private final int dimensions;

    public ArrayFieldType(
        boolean isMonomorphic, boolean trackingRef, FieldType componentType, int dimensions) {
      super(isMonomorphic, trackingRef);
      this.componentType = componentType;
      this.dimensions = dimensions;
    }

    @Override
    public TypeRef<?> toTypeToken(ClassResolver classResolver) {
      TypeRef<?> componentTypeRef = componentType.toTypeToken(classResolver);
      Class<?> componentRawType = componentTypeRef.getRawType();
      if (NonexistentClass.class.isAssignableFrom(componentRawType)) {
        return TypeRef.of(
            // We embed `isMonomorphic` flag in ObjectArraySerializer, so this flag can be ignored
            // here.
            NonexistentClass.getNonexistentClass(
                componentType instanceof EnumFieldType, dimensions, true),
            new TypeExtMeta(trackingRef));
      } else {
        return TypeRef.of(
            Array.newInstance(componentRawType, new int[dimensions]).getClass(),
            new TypeExtMeta(trackingRef));
      }
    }

    public int getDimensions() {
      return dimensions;
    }

    public FieldType getComponentType() {
      return componentType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ArrayFieldType that = (ArrayFieldType) o;
      return dimensions == that.dimensions && Objects.equals(componentType, that.componentType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), componentType, dimensions);
    }

    @Override
    public String toString() {
      return "ArrayFieldType{"
          + "componentType="
          + componentType
          + ", dimensions="
          + dimensions
          + ", isMonomorphic="
          + isMonomorphic
          + ", trackingRef="
          + trackingRef
          + '}';
    }
  }

  /** Class for field type which isn't registered and not collection/map type too. */
  public static class ObjectFieldType extends FieldType {

    public ObjectFieldType(boolean isFinal, boolean trackingRef) {
      super(isFinal, trackingRef);
    }

    @Override
    public TypeRef<?> toTypeToken(ClassResolver classResolver) {
      return isMonomorphic()
          ? TypeRef.of(FinalObjectTypeStub.class, new TypeExtMeta(trackingRef))
          : TypeRef.of(Object.class, new TypeExtMeta(trackingRef));
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  /** Build field type from generics, nested generics will be extracted too. */
  static FieldType buildFieldType(ClassResolver classResolver, Field field) {
    Preconditions.checkNotNull(field);
    GenericType genericType = GenericType.build(field.getGenericType());
    return buildFieldType(classResolver, genericType);
  }

  /** Build field type from generics, nested generics will be extracted too. */
  private static FieldType buildFieldType(ClassResolver classResolver, GenericType genericType) {
    Preconditions.checkNotNull(genericType);
    Class<?> rawType = genericType.getCls();
    boolean isMonomorphic = genericType.isMonomorphic();
    boolean trackingRef = genericType.trackingRef(classResolver);
    if (COLLECTION_TYPE.isSupertypeOf(genericType.getTypeRef())) {
      return new CollectionFieldType(
          isMonomorphic,
          trackingRef,
          buildFieldType(
              classResolver,
              genericType.getTypeParameter0() == null
                  ? GenericType.build(Object.class)
                  : genericType.getTypeParameter0()));
    } else if (MAP_TYPE.isSupertypeOf(genericType.getTypeRef())) {
      return new MapFieldType(
          isMonomorphic,
          trackingRef,
          buildFieldType(
              classResolver,
              genericType.getTypeParameter0() == null
                  ? GenericType.build(Object.class)
                  : genericType.getTypeParameter0()),
          buildFieldType(
              classResolver,
              genericType.getTypeParameter1() == null
                  ? GenericType.build(Object.class)
                  : genericType.getTypeParameter1()));
    } else {
      Short classId = classResolver.getRegisteredClassId(rawType);
      if (classId != null && classId != ClassResolver.NO_CLASS_ID) {
        return new RegisteredFieldType(isMonomorphic, trackingRef, classId);
      } else {
        if (rawType.isEnum()) {
          return EnumFieldType.getInstance();
        }
        if (rawType.isArray()) {
          Tuple2<Class<?>, Integer> info = TypeUtils.getArrayComponentInfo(rawType);
          return new ArrayFieldType(
              isMonomorphic,
              trackingRef,
              buildFieldType(classResolver, GenericType.build(info.f0)),
              info.f1);
        }
        return new ObjectFieldType(isMonomorphic, trackingRef);
      }
    }
  }

  public static ClassDef buildClassDef(Fury fury, Class<?> cls) {
    return buildClassDef(fury, cls, true);
  }

  public static ClassDef buildClassDef(Fury fury, Class<?> cls, boolean resolveParent) {
    return ClassDefEncoder.buildClassDef(
        fury.getClassResolver(), cls, buildFields(fury, cls, resolveParent), true);
  }

  /** Build class definition from fields of class. */
  public static ClassDef buildClassDef(
      ClassResolver classResolver, Class<?> type, List<Field> fields) {
    return buildClassDef(classResolver, type, fields, true);
  }

  public static ClassDef buildClassDef(
      ClassResolver classResolver, Class<?> type, List<Field> fields, boolean isObjectType) {
    return ClassDefEncoder.buildClassDef(classResolver, type, fields, isObjectType);
  }

  public ClassDef replaceRootClassTo(ClassResolver classResolver, Class<?> targetCls) {
    String name = targetCls.getName();
    List<FieldInfo> fieldInfos =
        fieldsInfo.stream()
            .map(
                fieldInfo -> {
                  if (fieldInfo.definedClass.equals(classSpec.entireClassName)) {
                    return new FieldInfo(name, fieldInfo.fieldName, fieldInfo.fieldType);
                  } else {
                    return fieldInfo;
                  }
                })
            .collect(Collectors.toList());
    return ClassDefEncoder.buildClassDefWithFieldInfos(
        classResolver, targetCls, fieldInfos, isObjectType);
  }
}
