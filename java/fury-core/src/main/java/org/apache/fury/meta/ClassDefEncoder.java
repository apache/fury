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

import static org.apache.fury.meta.ClassDef.COMPRESSION_FLAG;
import static org.apache.fury.meta.ClassDef.OBJECT_TYPE_FLAG;
import static org.apache.fury.meta.ClassDef.SCHEMA_COMPATIBLE_FLAG;
import static org.apache.fury.meta.ClassDef.SIZE_TWO_BYTES_FLAG;
import static org.apache.fury.meta.Encoders.fieldNameEncodingsList;
import static org.apache.fury.meta.Encoders.pkgEncodingsList;
import static org.apache.fury.meta.Encoders.typeNameEncodingsList;
import static org.apache.fury.util.MathUtils.toInt;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.meta.ClassDef.FieldInfo;
import org.apache.fury.meta.ClassDef.FieldType;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.DescriptorGrouper;
import org.apache.fury.util.MurmurHash3;

/**
 * An encoder which encode {@link ClassDef} into binary. See spec documentation:
 * docs/specification/java_serialization_spec.md <a
 * href="https://fury.apache.org/docs/specification/fury_java_serialization_spec">...</a>
 */
class ClassDefEncoder {
  static List<Field> buildFields(Fury fury, Class<?> cls, boolean resolveParent) {
    Comparator<Descriptor> comparator =
        DescriptorGrouper.getPrimitiveComparator(fury.compressInt(), fury.compressLong());
    DescriptorGrouper descriptorGrouper =
        new DescriptorGrouper(
            fury.getClassResolver()::isMonomorphic,
            fury.getClassResolver().getAllDescriptorsMap(cls, resolveParent).values(),
            false,
            Function.identity(),
            comparator,
            DescriptorGrouper.COMPARATOR_BY_TYPE_AND_NAME);
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
    return fields;
  }

  static List<FieldInfo> buildFieldsInfo(ClassResolver resolver, Class<?> cls) {
    return buildFieldsInfo(resolver, buildFields(resolver.getFury(), cls, true));
  }

  static List<FieldInfo> buildFieldsInfo(ClassResolver resolver, List<Field> fields) {
    List<FieldInfo> fieldInfos = new ArrayList<>();
    for (Field field : fields) {
      FieldInfo fieldInfo =
          new FieldInfo(
              field.getDeclaringClass().getName(),
              field.getName(),
              ClassDef.buildFieldType(resolver, field));
      fieldInfos.add(fieldInfo);
    }
    return fieldInfos;
  }

  /** Build class definition from fields of class. */
  static ClassDef buildClassDef(
      ClassResolver classResolver, Class<?> type, List<Field> fields, boolean isObjectType) {
    return buildClassDefWithFieldInfos(
        classResolver, type, buildFieldsInfo(classResolver, fields), isObjectType);
  }

  static ClassDef buildClassDefWithFieldInfos(
      ClassResolver classResolver,
      Class<?> type,
      List<ClassDef.FieldInfo> fieldInfos,
      boolean isObjectType) {
    Map<String, List<FieldInfo>> classLayers = getClassFields(type, fieldInfos);
    fieldInfos = new ArrayList<>(fieldInfos.size());
    classLayers.values().forEach(fieldInfos::addAll);
    MemoryBuffer encodeClassDef = encodeClassDef(classResolver, type, classLayers, isObjectType);
    byte[] classDefBytes = encodeClassDef.getBytes(0, encodeClassDef.writerIndex());
    return new ClassDef(
        Encoders.buildClassSpec(type),
        fieldInfos,
        isObjectType,
        encodeClassDef.getInt64(0),
        classDefBytes);
  }

  // see spec documentation: docs/specification/java_serialization_spec.md
  // https://fury.apache.org/docs/specification/fury_java_serialization_spec
  static MemoryBuffer encodeClassDef(
      ClassResolver classResolver,
      Class<?> type,
      Map<String, List<FieldInfo>> classLayers,
      boolean isObjectType) {
    MemoryBuffer classDefBuf = MemoryBuffer.newHeapBuffer(128);
    for (Map.Entry<String, List<FieldInfo>> entry : classLayers.entrySet()) {
      String className = entry.getKey();
      List<FieldInfo> fields = entry.getValue();
      // | num fields + register flag | header + package name | header + class name
      // | header + type id + field name | next field info | ... |
      int currentClassHeader = (fields.size() << 1);
      if (classResolver.isRegisteredById(type)) {
        currentClassHeader |= 1;
        classDefBuf.writeVarUint32Small7(currentClassHeader);
        classDefBuf.writeVarUint32Small7(classResolver.getRegisteredClassId(type));
      } else {
        classDefBuf.writeVarUint32Small7(currentClassHeader);
        Class<?> currentType = getType(type, className);
        String ns, typename;
        if (classResolver.isRegisteredByName(type)) {
          Tuple2<String, String> nameTuple = classResolver.getRegisteredNameTuple(type);
          ns = nameTuple.f0;
          typename = nameTuple.f1;
        } else {
          Tuple2<String, String> encoded = Encoders.encodePkgAndClass(currentType);
          ns = encoded.f0;
          typename = encoded.f1;
        }
        writePkgName(classDefBuf, ns);
        writeTypeName(classDefBuf, typename);
      }
      writeFieldsInfo(classDefBuf, fields);
    }
    byte[] compressed =
        classResolver
            .getFury()
            .getConfig()
            .getMetaCompressor()
            .compress(classDefBuf.getHeapMemory(), 0, classDefBuf.writerIndex());
    boolean isCompressed = false;
    if (compressed.length < classDefBuf.writerIndex()) {
      isCompressed = true;
      classDefBuf = MemoryBuffer.fromByteArray(compressed);
      classDefBuf.writerIndex(compressed.length);
    }
    long hash =
        MurmurHash3.murmurhash3_x64_128(
            classDefBuf.getHeapMemory(), 0, classDefBuf.writerIndex(), 47)[0];
    long header;
    int numClasses = classLayers.size() - 1; // num class must be greater than 0
    if (numClasses > 0b1110) {
      header = 0b1111;
    } else {
      header = numClasses;
    }
    header |= SCHEMA_COMPATIBLE_FLAG;
    if (isObjectType) {
      header |= OBJECT_TYPE_FLAG;
    }
    if (isCompressed) {
      header |= COMPRESSION_FLAG;
    }
    // this id will be part of generated codec, a negative number won't be allowed in class name.
    hash <<= 8;
    header |= Math.abs(hash);
    MemoryBuffer buffer = MemoryUtils.buffer(classDefBuf.writerIndex() + 10);
    int len = classDefBuf.writerIndex() + toInt(numClasses > 0b1110);
    if (len > 255) {
      header |= SIZE_TWO_BYTES_FLAG;
      buffer.writeInt64(header);
      buffer.writeInt16((short) len);
    } else {
      buffer.writeInt64(header);
      buffer.writeByte(len);
    }
    if (numClasses > 0b1110) {
      buffer.writeVarUint32Small7(numClasses - 0b1110);
    }
    buffer.writeBytes(classDefBuf.getHeapMemory(), 0, classDefBuf.writerIndex());
    return buffer;
  }

  private static Class<?> getType(Class<?> cls, String type) {
    Class<?> c = cls;
    while (cls != null) {
      if (type.equals(cls.getName())) {
        return cls;
      }
      cls = cls.getSuperclass();
    }
    throw new IllegalStateException(
        String.format("Class %s doesn't have %s as super class", c, type));
  }

  static Map<String, List<FieldInfo>> getClassFields(Class<?> type, List<FieldInfo> fieldsInfo) {
    Map<String, List<FieldInfo>> sortedClassFields = new LinkedHashMap<>();
    if (fieldsInfo.isEmpty()) {
      sortedClassFields.put(type.getName(), new ArrayList<>());
      return sortedClassFields;
    }
    Map<String, List<FieldInfo>> classFields = groupClassFields(fieldsInfo);
    for (Class<?> clz : ReflectionUtils.getAllClasses(type, true)) {
      List<FieldInfo> fieldInfos = classFields.get(clz.getName());
      if (fieldInfos != null) {
        sortedClassFields.put(clz.getName(), fieldInfos);
      } else if (type.getName().equals(clz.getName())) {
        sortedClassFields.put(clz.getName(), new ArrayList<>());
      }
    }
    classFields = sortedClassFields;
    return classFields;
  }

  static Map<String, List<FieldInfo>> groupClassFields(List<FieldInfo> fieldsInfo) {
    Map<String, List<FieldInfo>> classFields = new HashMap<>();
    for (FieldInfo fieldInfo : fieldsInfo) {
      String definedClass = fieldInfo.getDefinedClass();
      classFields.computeIfAbsent(definedClass, k -> new ArrayList<>()).add(fieldInfo);
    }
    return classFields;
  }

  /** Write field type and name info. */
  static void writeFieldsInfo(MemoryBuffer buffer, List<FieldInfo> fields) {
    for (FieldInfo fieldInfo : fields) {
      FieldType fieldType = fieldInfo.getFieldType();
      // `3 bits size + 2 bits field name encoding + polymorphism flag + nullability flag + ref
      // tracking flag`
      int header = ((fieldType.isMonomorphic() ? 1 : 0) << 2);
      header |= ((fieldType.trackingRef() ? 1 : 0));
      // Encoding `UTF8/ALL_TO_LOWER_SPECIAL/LOWER_UPPER_DIGIT_SPECIAL/TAG_ID`
      MetaString metaString = Encoders.encodeFieldName(fieldInfo.getFieldName());
      int encodingFlags = fieldNameEncodingsList.indexOf(metaString.getEncoding());
      byte[] encoded = metaString.getBytes();
      int size = (encoded.length - 1);
      if (fieldInfo.hasTypeTag()) {
        size = fieldInfo.getTypeTag();
        encodingFlags = 3;
      }
      header |= (byte) (encodingFlags << 3);
      boolean bigSize = size >= 7;
      if (bigSize) {
        header |= 0b11100000;
        buffer.writeByte(header);
        buffer.writeVarUint32Small7(size - 7);
      } else {
        header |= (size << 5);
        buffer.writeByte(header);
      }
      if (!fieldInfo.hasTypeTag()) {
        buffer.writeBytes(encoded);
      }
      fieldType.write(buffer, false);
    }
  }

  private static void writePkgName(MemoryBuffer buffer, String pkg) {
    // - Package name encoding(omitted when class is registered):
    //    - encoding algorithm: `UTF8/ALL_TO_LOWER_SPECIAL/LOWER_UPPER_DIGIT_SPECIAL`
    //    - Header: `6 bits size | 2 bits encoding flags`.
    //      The `6 bits size: 0~63`  will be used to indicate size `0~62`,
    //      the value `63` the size need more byte to read, the encoding will encode `size - 62` as
    // a varint next.
    MetaString pkgMetaString = Encoders.encodePackage(pkg);
    byte[] encoded = pkgMetaString.getBytes();
    writeName(buffer, encoded, pkgEncodingsList.indexOf(pkgMetaString.getEncoding()));
  }

  private static void writeTypeName(MemoryBuffer buffer, String typeName) {
    // - Class name encoding(omitted when class is registered):
    //     - encoding algorithm:
    // `UTF8/LOWER_UPPER_DIGIT_SPECIAL/FIRST_TO_LOWER_SPECIAL/ALL_TO_LOWER_SPECIAL`
    //     - header: `6 bits size | 2 bits encoding flags`.
    //       The `6 bits size: 0~63`  will be used to indicate size `1~64`,
    //       the value `63` the size need more byte to read, the encoding will encode `size - 63` as
    // a varint next.
    MetaString metaString = Encoders.encodeTypeName(typeName);
    byte[] encoded = metaString.getBytes();
    writeName(buffer, encoded, typeNameEncodingsList.indexOf(metaString.getEncoding()));
  }

  static final int BIG_NAME_THRESHOLD = 0b111111;

  private static void writeName(MemoryBuffer buffer, byte[] encoded, int encoding) {
    boolean bigSize = encoded.length >= BIG_NAME_THRESHOLD;
    if (bigSize) {
      int header = (BIG_NAME_THRESHOLD << 2) | encoding;
      buffer.writeByte(header);
      buffer.writeVarUint32Small7(encoded.length - BIG_NAME_THRESHOLD);
    } else {
      int header = (encoded.length << 2) | encoding;
      buffer.writeByte(header);
    }
    buffer.writeBytes(encoded);
  }
}
