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
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.DescriptorGrouper;
import org.apache.fury.util.MurmurHash3;
import org.apache.fury.util.Preconditions;

class ClassDefEncoder {
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

  static ClassDef buildClassDef(
      ClassResolver classResolver, Class<?> type, List<Field> fields, Map<String, String> extMeta) {
    List<ClassDef.FieldInfo> fieldInfos = new ArrayList<>();
    for (Field field : fields) {
      ClassDef.FieldInfo fieldInfo =
          new ClassDef.FieldInfo(
              field.getDeclaringClass().getName(),
              field.getName(),
              ClassDef.buildFieldType(classResolver, field));
      fieldInfos.add(fieldInfo);
    }
    MemoryBuffer encodeClassDef = encodeClassDef(classResolver, type, fieldInfos, extMeta);
    byte[] classDefBytes = encodeClassDef.getBytes(0, encodeClassDef.writerIndex());
    return new ClassDef(
        type.getName(), fieldInfos, extMeta, encodeClassDef.getInt64(0), classDefBytes);
  }

  // see spec documentation: docs/specification/java_serialization_spec.md
  // https://fury.apache.org/docs/specification/fury_java_serialization_spec
  static MemoryBuffer encodeClassDef(
      ClassResolver classResolver,
      Class<?> type,
      List<ClassDef.FieldInfo> fieldsInfo,
      Map<String, String> extMeta) {
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    long header;
    Map<String, List<ClassDef.FieldInfo>> classFields = getClassFields(type, fieldsInfo);
    int encodedSize = classFields.size() - 1; // num class must be greater than 0
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
    for (Map.Entry<String, List<ClassDef.FieldInfo>> entry : classFields.entrySet()) {
      String className = entry.getKey();
      List<ClassDef.FieldInfo> fields = entry.getValue();
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
      extMeta.forEach(
          (k, v) -> {
            byte[] keyBytes = k.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = v.getBytes(StandardCharsets.UTF_8);
            buffer.writePrimitiveArrayWithSize(
                keyBytes, Platform.BYTE_ARRAY_OFFSET, keyBytes.length);
            buffer.writePrimitiveArrayWithSize(
                valueBytes, Platform.BYTE_ARRAY_OFFSET, valueBytes.length);
          });
    }
    byte[] encodedClassDef = buffer.getBytes(0, buffer.writerIndex());
    long hash = MurmurHash3.murmurhash3_x64_128(encodedClassDef, 0, encodedClassDef.length, 47)[0];
    // this id will be part of generated codec, a negative number won't be allowed in class name.
    hash <<= 8;
    header |= hash;
    header = Math.abs(header);
    MemoryBuffer newBuf = MemoryBuffer.newHeapBuffer(buffer.writerIndex() + 2);
    if (buffer.writerIndex() > 255) {
      header |= SIZE_TWO_BYTES_FLAG;
    }
    newBuf.writeInt64(header);
    if (buffer.writerIndex() > 255) {
      newBuf.writeInt16((short) buffer.writerIndex());
    } else {
      newBuf.writeByte(buffer.writerIndex());
    }
    newBuf.writeBytes(buffer.getHeapMemory(), 0, buffer.writerIndex());
    return buffer;
  }

  private static Map<String, List<ClassDef.FieldInfo>> getClassFields(
      Class<?> type, List<ClassDef.FieldInfo> fieldsInfo) {
    Map<String, List<ClassDef.FieldInfo>> classFields = new HashMap<>();
    for (ClassDef.FieldInfo fieldInfo : fieldsInfo) {
      String definedClass = fieldInfo.getDefinedClass();
      classFields.computeIfAbsent(definedClass, k -> new ArrayList<>()).add(fieldInfo);
    }
    Map<String, List<ClassDef.FieldInfo>> sortedClassFields = new LinkedHashMap<>();
    for (Class<?> clz : ReflectionUtils.getAllClasses(type, true)) {
      List<ClassDef.FieldInfo> fieldInfos = classFields.get(clz.getName());
      if (fieldInfos != null) {
        sortedClassFields.put(clz.getName(), fieldInfos);
      }
    }
    classFields = sortedClassFields;
    return classFields;
  }

  private static void writeFieldsInfo(MemoryBuffer buffer, List<ClassDef.FieldInfo> fields) {
    for (ClassDef.FieldInfo fieldInfo : fields) {
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
        buffer.writeVarUint32Small7(header);
        buffer.writeVarUint32Small7(size - 7);
      } else {
        buffer.writeVarUint32Small7(header);
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
    int pkgHeader =
        (encoded.length << 2) | typeNameEncodingsList.indexOf(pkgMetaString.getEncoding());
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
      header |= 0b11111111;
      buffer.writeVarUint32Small7(header);
      buffer.writeVarUint32Small7(encoded.length - max);
    } else {
      buffer.writeVarUint32Small7(header);
    }
    buffer.writeBytes(encoded);
  }
}
