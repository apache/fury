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

import static org.apache.fury.type.TypeUtils.COLLECTION_TYPE;
import static org.apache.fury.type.TypeUtils.MAP_TYPE;
import static org.apache.fury.type.TypeUtils.collectionOf;
import static org.apache.fury.type.TypeUtils.mapOf;

import com.google.common.reflect.TypeToken;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.builder.MetaSharedCodecBuilder;
import org.apache.fury.collection.IdentityObjectIntMap;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.CompatibleSerializer;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.DescriptorGrouper;
import org.apache.fury.type.FinalObjectTypeStub;
import org.apache.fury.type.GenericType;
import org.apache.fury.util.MurmurHash3;
import org.apache.fury.util.Platform;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;

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
 * @see FuryBuilder#withMetaContextShare
 * @see ReflectionUtils#getFieldOffset
 */
@SuppressWarnings("UnstableApiUsage")
public class ClassDef implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ClassDef.class);

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

  private final String className;
  private final List<FieldInfo> fieldsInfo;
  private final Map<String, String> extMeta;
  // Unique id for class def. If class def are same between processes, then the id will
  // be same too.
  private long id;

  // cache for serialization.
  private transient byte[] serialized;

  private ClassDef(String className, List<FieldInfo> fieldsInfo, Map<String, String> extMeta) {
    this.className = className;
    this.fieldsInfo = fieldsInfo;
    this.extMeta = extMeta;
  }

  /**
   * Returns class name.
   *
   * @see Class#getName()
   */
  public String getClassName() {
    return className;
  }

  /** Contain all fields info including all parent classes. */
  public List<FieldInfo> getFieldsInfo() {
    return fieldsInfo;
  }

  /** Returns ext meta for the class. */
  public Map<String, String> getExtMeta() {
    return extMeta;
  }

  /**
   * Returns an unique id for class def. If class def are same between processes, then the id will
   * be same too.
   */
  public long getId() {
    return id;
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
    return Objects.equals(className, classDef.className)
        && Objects.equals(fieldsInfo, classDef.fieldsInfo)
        && Objects.equals(extMeta, classDef.extMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, fieldsInfo, extMeta);
  }

  /** Write class definition to buffer. */
  public void writeClassDef(MemoryBuffer buffer) {
    byte[] serialized = this.serialized;
    if (serialized == null) {
      MemoryBuffer buf = MemoryUtils.buffer(32);
      IdentityObjectIntMap<String> map = new IdentityObjectIntMap<>(8, 0.5f);
      writeSharedString(buf, map, className);
      buf.writeVarUint32Small7(fieldsInfo.size());
      for (FieldInfo fieldInfo : fieldsInfo) {
        writeSharedString(buf, map, fieldInfo.definedClass);
        byte[] bytes = fieldInfo.fieldName.getBytes(StandardCharsets.UTF_8);
        buf.writePrimitiveArrayWithSize(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length);
        fieldInfo.fieldType.write(buf);
      }
      buf.writeVarUint32Small7(extMeta.size());
      extMeta.forEach(
          (k, v) -> {
            byte[] keyBytes = k.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = v.getBytes(StandardCharsets.UTF_8);
            buf.writePrimitiveArrayWithSize(keyBytes, Platform.BYTE_ARRAY_OFFSET, keyBytes.length);
            buf.writePrimitiveArrayWithSize(
                valueBytes, Platform.BYTE_ARRAY_OFFSET, valueBytes.length);
          });
      serialized = this.serialized = buf.getBytes(0, buf.writerIndex());
      id = MurmurHash3.murmurhash3_x64_128(serialized, 0, serialized.length, 47)[0];
      // this id will be part of generated codec, a negative number won't be allowed in class name.
      id = Math.abs(id);
    }
    buffer.writeBytes(serialized);
    buffer.writeInt64(id);
  }

  private static void writeSharedString(
      MemoryBuffer buffer, IdentityObjectIntMap<String> map, String str) {
    int newId = map.size;
    int id = map.putOrGet(str, newId);
    if (id >= 0) {
      // TODO use flagged varint.
      buffer.writeBoolean(true);
      buffer.writeVarUint32Small7(id);
    } else {
      buffer.writeBoolean(false);
      byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
      buffer.writePrimitiveArrayWithSize(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length);
    }
  }

  /** Read class definition from buffer. */
  public static ClassDef readClassDef(MemoryBuffer buffer) {
    List<String> strings = new ArrayList<>();
    String className = readSharedString(buffer, strings);
    List<FieldInfo> fieldInfos = new ArrayList<>();
    int numFields = buffer.readVarUint32Small7();
    for (int i = 0; i < numFields; i++) {
      String definedClass = readSharedString(buffer, strings);
      String fieldName = new String(buffer.readBytesAndSize(), StandardCharsets.UTF_8);
      fieldInfos.add(new FieldInfo(definedClass, fieldName, FieldType.read(buffer)));
    }
    int extMetaSize = buffer.readVarUint32Small7();
    Map<String, String> extMeta = new HashMap<>();
    for (int i = 0; i < extMetaSize; i++) {
      extMeta.put(
          new String(buffer.readBytesAndSize(), StandardCharsets.UTF_8),
          new String(buffer.readBytesAndSize(), StandardCharsets.UTF_8));
    }
    long id = buffer.readInt64();
    ClassDef classDef = new ClassDef(className, fieldInfos, extMeta);
    classDef.id = id;
    return classDef;
  }

  private static String readSharedString(MemoryBuffer buffer, List<String> strings) {
    String str;
    if (buffer.readBoolean()) {
      return strings.get(buffer.readVarUint32Small7());
    } else {
      str = new String(buffer.readBytesAndSize(), StandardCharsets.UTF_8);
      strings.add(str);
      return str;
    }
  }

  public static ClassDef buildClassDef(Class<?> cls, Fury fury) {
    Comparator<Descriptor> comparator =
        DescriptorGrouper.getPrimitiveComparator(fury.compressInt(), fury.compressLong());
    DescriptorGrouper descriptorGrouper =
        new DescriptorGrouper(
            fury.getClassResolver().getAllDescriptorsMap(cls, true).values(),
            false,
            Function.identity(),
            comparator,
            DescriptorGrouper.COMPARATOR_BY_TYPE_AND_NAME);
    ClassResolver classResolver = fury.getClassResolver();
    List<Field> fields = new ArrayList<>();
    descriptorGrouper
        .getPrimitiveDescriptors()
        .forEach(descriptor -> fields.add(descriptor.getField()));
    descriptorGrouper
        .getBoxedDescriptors()
        .forEach(descriptor -> fields.add(descriptor.getField()));
    descriptorGrouper
        .getFinalDescriptors()
        .forEach(descriptor -> fields.add(descriptor.getField()));
    descriptorGrouper
        .getOtherDescriptors()
        .forEach(descriptor -> fields.add(descriptor.getField()));
    descriptorGrouper
        .getCollectionDescriptors()
        .forEach(descriptor -> fields.add(descriptor.getField()));
    descriptorGrouper.getMapDescriptors().forEach(descriptor -> fields.add(descriptor.getField()));
    return buildClassDef(classResolver, cls, fields);
  }

  /** Build class definition from fields of class. */
  public static ClassDef buildClassDef(
      ClassResolver classResolver, Class<?> type, List<Field> fields) {
    return buildClassDef(classResolver, type, fields, new HashMap<>());
  }

  public static ClassDef buildClassDef(
      ClassResolver classResolver, Class<?> type, List<Field> fields, Map<String, String> extMeta) {
    List<FieldInfo> fieldInfos = new ArrayList<>();
    for (Field field : fields) {
      FieldInfo fieldInfo =
          new FieldInfo(
              field.getDeclaringClass().getName(),
              field.getName(),
              buildFieldType(classResolver, field));
      fieldInfos.add(fieldInfo);
    }
    return new ClassDef(type.getName(), fieldInfos, extMeta);
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

    private FieldInfo(String definedClass, String fieldName, FieldType fieldType) {
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

    /** Returns type of current field. */
    public FieldType getFieldType() {
      return fieldType;
    }

    /**
     * Convert this field into a {@link Descriptor}, the corresponding {@link Field} field will be
     * null. Don't invoke this method if class does have <code>fieldName</code> field. In such case,
     * reflection should be used to get the descriptor.
     */
    public Descriptor toDescriptor(ClassResolver classResolver) {
      TypeToken<?> typeToken = fieldType.toTypeToken(classResolver);
      // This field doesn't exist in peer class, so any legal modifier will be OK.
      int stubModifiers = ReflectionUtils.getField(getClass(), "fieldName").getModifiers();
      return new Descriptor(typeToken, fieldName, stubModifiers, definedClass);
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
      return "FieldInfo{" + "fieldName='" + fieldName + '\'' + ", fieldType=" + fieldType + '}';
    }
  }

  public abstract static class FieldType implements Serializable {
    public FieldType(boolean isMonomorphic) {
      this.isMonomorphic = isMonomorphic;
    }

    private final boolean isMonomorphic;

    public boolean isMonomorphic() {
      return isMonomorphic;
    }

    /**
     * Convert a serializable field type to type token. If field type is a generic type with
     * generics, the generics will be built up recursively. The final leaf object type will be built
     * from class id or class stub.
     *
     * @see FinalObjectTypeStub
     */
    public abstract TypeToken<?> toTypeToken(ClassResolver classResolver);

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FieldType fieldType = (FieldType) o;
      return isMonomorphic == fieldType.isMonomorphic;
    }

    @Override
    public int hashCode() {
      return Objects.hash(isMonomorphic);
    }

    public void write(MemoryBuffer buffer) {
      buffer.writeBoolean(isMonomorphic);
      if (this instanceof RegisteredFieldType) {
        buffer.writeByte(0);
        buffer.writeInt16(((RegisteredFieldType) this).getClassId());
      } else if (this instanceof CollectionFieldType) {
        buffer.writeByte(1);
        ((CollectionFieldType) this).elementType.write(buffer);
      } else if (this instanceof MapFieldType) {
        buffer.writeByte(2);
        MapFieldType mapFieldType = (MapFieldType) this;
        mapFieldType.keyType.write(buffer);
        mapFieldType.valueType.write(buffer);
      } else {
        Preconditions.checkArgument(this instanceof ObjectFieldType);
        buffer.writeByte(3);
      }
    }

    public static FieldType read(MemoryBuffer buffer) {
      boolean isFinal = buffer.readBoolean();
      byte typecode = buffer.readByte();
      switch (typecode) {
        case 0:
          return new RegisteredFieldType(isFinal, buffer.readInt16());
        case 1:
          return new CollectionFieldType(isFinal, read(buffer));
        case 2:
          return new MapFieldType(isFinal, read(buffer), read(buffer));
        case 3:
          return new ObjectFieldType(isFinal);
        default:
          throw new IllegalStateException(String.format("Unsupported type code %s", typecode));
      }
    }
  }

  /** Class for field type which is registered. */
  public static class RegisteredFieldType extends FieldType {
    private final short classId;

    public RegisteredFieldType(boolean isFinal, short classId) {
      super(isFinal);
      this.classId = classId;
    }

    public short getClassId() {
      return classId;
    }

    @Override
    public TypeToken<?> toTypeToken(ClassResolver classResolver) {
      return TypeToken.of(classResolver.getRegisteredClass(classId));
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

    public CollectionFieldType(boolean isFinal, FieldType elementType) {
      super(isFinal);
      this.elementType = elementType;
    }

    public FieldType getElementType() {
      return elementType;
    }

    @Override
    public TypeToken<?> toTypeToken(ClassResolver classResolver) {
      return collectionOf(elementType.toTypeToken(classResolver));
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

    public MapFieldType(boolean isFinal, FieldType keyType, FieldType valueType) {
      super(isFinal);
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
    public TypeToken<?> toTypeToken(ClassResolver classResolver) {
      return mapOf(keyType.toTypeToken(classResolver), valueType.toTypeToken(classResolver));
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
          + '}';
    }
  }

  /** Class for field type which isn't registered and not collection/map type too. */
  public static class ObjectFieldType extends FieldType {

    public ObjectFieldType(boolean isFinal) {
      super(isFinal);
    }

    @Override
    public TypeToken<?> toTypeToken(ClassResolver classResolver) {
      return isMonomorphic() ? TypeToken.of(FinalObjectTypeStub.class) : TypeToken.of(Object.class);
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
    Class<?> rawType = field.getType();
    boolean isFinal = GenericType.isFinalByDefault(rawType);
    if (Collection.class.isAssignableFrom(rawType)) {
      GenericType genericType = GenericType.build(field.getGenericType());
      return new CollectionFieldType(
          isFinal,
          buildFieldType(
              classResolver,
              genericType.getTypeParameter0() == null
                  ? GenericType.build(Object.class)
                  : genericType.getTypeParameter0()));
    } else if (Map.class.isAssignableFrom(rawType)) {
      GenericType genericType = GenericType.build(field.getGenericType());
      return new MapFieldType(
          isFinal,
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
        return new RegisteredFieldType(isFinal, classId);
      } else {
        return new ObjectFieldType(isFinal);
      }
    }
  }

  /** Build field type from generics, nested generics will be extracted too. */
  private static FieldType buildFieldType(ClassResolver classResolver, GenericType genericType) {
    Preconditions.checkNotNull(genericType);
    boolean isFinal = genericType.isMonomorphic();
    if (COLLECTION_TYPE.isSupertypeOf(genericType.getTypeToken())) {
      return new CollectionFieldType(
          isFinal,
          buildFieldType(
              classResolver,
              genericType.getTypeParameter0() == null
                  ? GenericType.build(Object.class)
                  : genericType.getTypeParameter0()));
    } else if (MAP_TYPE.isSupertypeOf(genericType.getTypeToken())) {
      return new MapFieldType(
          isFinal,
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
      Short classId = classResolver.getRegisteredClassId(genericType.getCls());
      if (classId != null && classId != ClassResolver.NO_CLASS_ID) {
        return new RegisteredFieldType(isFinal, classId);
      } else {
        return new ObjectFieldType(isFinal);
      }
    }
  }
}
