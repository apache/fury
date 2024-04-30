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

import static org.apache.fury.meta.ClassDef.EXT_FLAG;
import static org.apache.fury.meta.ClassDef.SCHEMA_COMPATIBLE_FLAG;
import static org.apache.fury.meta.ClassDef.SIZE_TWO_BYTES_FLAG;
import static org.apache.fury.meta.Encoders.fieldNameEncodingsList;
import static org.apache.fury.meta.Encoders.pkgEncodingsList;
import static org.apache.fury.meta.Encoders.typeNameEncodingsList;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.memory.Platform;
import org.apache.fury.meta.ClassDef.FieldInfo;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.DescriptorGrouper;
import org.apache.fury.util.MurmurHash3;
import org.apache.fury.util.Preconditions;

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
      ClassResolver classResolver, Class<?> type, List<Field> fields, Map<String, String> extMeta) {
    List<FieldInfo> fieldInfos = buildFieldsInfo(classResolver, fields);
    Map<String, List<FieldInfo>> classLayers = getClassFields(type, fieldInfos);
    fieldInfos = new ArrayList<>(fieldInfos.size());
    classLayers.values().forEach(fieldInfos::addAll);
    MemoryBuffer encodeClassDef = encodeClassDef(classResolver, type, classLayers, extMeta);
    byte[] classDefBytes = encodeClassDef.getBytes(0, encodeClassDef.writerIndex());
    return new ClassDef(
        type.getName(), fieldInfos, extMeta, encodeClassDef.getInt64(0), classDefBytes);
  }

  // see spec documentation: docs/specification/java_serialization_spec.md
  // https://fury.apache.org/docs/specification/fury_java_serialization_spec
  static MemoryBuffer encodeClassDef(
      ClassResolver classResolver,
      Class<?> type,
      Map<String, List<FieldInfo>> classLayers,
      Map<String, String> extMeta) {
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    buffer.increaseWriterIndex(9); // header + one byte size
    long header;
    int encodedSize = classLayers.size() - 1; // num class must be greater than 0
    if (encodedSize > 0b1110) {
      header = 0b1111;
      buffer.writeVarUint32Small7(encodedSize - 0b1110);
    } else {
      header = encodedSize;
    }
    header |= SCHEMA_COMPATIBLE_FLAG;
    if (!extMeta.isEmpty()) {
      header |= EXT_FLAG;
    }
    for (Map.Entry<String, List<FieldInfo>> entry : classLayers.entrySet()) {
      String className = entry.getKey();
      List<FieldInfo> fields = entry.getValue();
      // | num fields + register flag | header + package name | header + class name
      // | header + type id + field name | next field info | ... |
      int currentClassHeader = (fields.size() << 1);
      if (classResolver.isRegistered(type)) {
        currentClassHeader |= 1;
        buffer.writeVarUint32Small7(currentClassHeader);
        buffer.writeVarUint32Small7(classResolver.getRegisteredClassId(type));
      } else {
        buffer.writeVarUint32Small7(currentClassHeader);
        String pkg = ReflectionUtils.getPackage(className);
        String typeName = ReflectionUtils.getSimpleClassName(className);
        writePkgName(buffer, pkg);
        writeTypeName(buffer, typeName);
      }
      writeFieldsInfo(buffer, fields);
    }
    if (!extMeta.isEmpty()) {
      buffer.writeVarUint32Small7(extMeta.size());
      for (Map.Entry<String, String> entry : extMeta.entrySet()) {
        String k = entry.getKey();
        String v = entry.getValue();
        byte[] keyBytes = k.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = v.getBytes(StandardCharsets.UTF_8);
        buffer.writePrimitiveArrayWithSize(keyBytes, Platform.BYTE_ARRAY_OFFSET, keyBytes.length);
        buffer.writePrimitiveArrayWithSize(
            valueBytes, Platform.BYTE_ARRAY_OFFSET, valueBytes.length);
      }
    }
    byte[] encodedClassDef = buffer.getBytes(0, buffer.writerIndex());
    long hash = MurmurHash3.murmurhash3_x64_128(encodedClassDef, 0, encodedClassDef.length, 47)[0];
    // this id will be part of generated codec, a negative number won't be allowed in class name.
    hash <<= 8;
    header |= Math.abs(hash);
    int len = buffer.writerIndex() - 9;
    if (len > 255) {
      header |= SIZE_TWO_BYTES_FLAG;
    }
    buffer.putInt64(0, header);
    if (len > 255) {
      MemoryBuffer buf = MemoryBuffer.newHeapBuffer(len + 1);
      buf.writeInt64(header);
      buf.writeInt16((short) len);
      buf.writeBytes(buffer.getBytes(9, len));
      buffer = buf;
    } else {
      buffer.putByte(8, (byte) len);
    }
    return buffer;
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

  private static void writeFieldsInfo(MemoryBuffer buffer, List<FieldInfo> fields) {
    for (FieldInfo fieldInfo : fields) {
      ClassDef.FieldType fieldType = fieldInfo.getFieldType();
      // `3 bits size + 2 bits field name encoding + polymorphism flag + nullability flag + ref
      // tracking flag`
      int header = ((fieldType.isMonomorphic() ? 1 : 0) << 2);
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
      if (fieldType instanceof ClassDef.RegisteredFieldType) {
        short classId = ((ClassDef.RegisteredFieldType) fieldType).getClassId();
        buffer.writeVarUint32Small7(3 + classId);
      } else if (fieldType instanceof ClassDef.CollectionFieldType) {
        buffer.writeVarUint32Small7(2);
        // TODO remove it when new collection deserialization jit finished.
        ((ClassDef.CollectionFieldType) fieldType).getElementType().write(buffer);
      } else if (fieldType instanceof ClassDef.MapFieldType) {
        buffer.writeVarUint32Small7(1);
        // TODO remove it when new map deserialization jit finished.
        ClassDef.MapFieldType mapFieldType = (ClassDef.MapFieldType) fieldType;
        mapFieldType.getKeyType().write(buffer);
        mapFieldType.getValueType().write(buffer);
      } else {
        Preconditions.checkArgument(fieldType instanceof ClassDef.ObjectFieldType);
        buffer.writeVarUint32Small7(0);
      }
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
    int pkgHeader = (encoded.length << 2) | pkgEncodingsList.indexOf(pkgMetaString.getEncoding());
    writeName(buffer, encoded, pkgHeader, 62);
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
    int header = (encoded.length << 2) | typeNameEncodingsList.indexOf(metaString.getEncoding());
    writeName(buffer, encoded, header, 63);
  }

  private static void writeName(MemoryBuffer buffer, byte[] encoded, int header, int max) {
    boolean bigSize = encoded.length > max;
    if (bigSize) {
      header |= 0b11111100;
      buffer.writeVarUint32Small7(header);
      buffer.writeVarUint32Small7(encoded.length - max);
    } else {
      buffer.writeByte(header);
    }
    buffer.writeBytes(encoded);
  }
}
