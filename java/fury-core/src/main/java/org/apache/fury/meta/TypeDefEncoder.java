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

import static org.apache.fury.meta.Encoders.fieldNameEncodingsList;
import static org.apache.fury.meta.Encoders.pkgEncodingsList;
import static org.apache.fury.meta.Encoders.typeNameEncodingsList;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.meta.ClassDef.FieldInfo;
import org.apache.fury.meta.ClassDef.FieldType;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.XtypeResolver;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.DescriptorGrouper;
import org.apache.fury.type.Types;
import org.apache.fury.util.MurmurHash3;
import org.apache.fury.util.Preconditions;

/**
 * An encoder which encode {@link ClassDef} into binary. See spec documentation:
 * docs/specification/fury_xlang_serialization_spec.md <a
 * href="https://fury.apache.org/docs/specification/fury_xlang_serialization_spec">...</a>
 */
class TypeDefEncoder {

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

  /**
   * Build class definition from fields of class.
   */
  static ClassDef buildTypeDef(
    ClassResolver classResolver, Class<?> type) {
    DescriptorGrouper descriptorGrouper =
      classResolver
        .createDescriptorGrouper(
          classResolver.getAllDescriptorsMap(type, true).values(),
          false,
          Function.identity());
    List<Field> fields = descriptorGrouper.getSortedDescriptors().stream()
      .map(Descriptor::getField).collect(Collectors.toList());
    return buildClassDefWithFieldInfos(
      classResolver, type, buildFieldsInfo(classResolver, fields));
  }

  static ClassDef buildClassDefWithFieldInfos(
    XtypeResolver classResolver,
    Class<?> type,
    List<FieldInfo> fieldInfos) {
    Map<String, FieldInfo> fieldInfoMap = getClassFields(type, fieldInfos);
    fieldInfos = new ArrayList<>(fieldInfoMap.values());
    MemoryBuffer encodeClassDef = encodeClassDef(classResolver, type, fieldInfos);
    byte[] classDefBytes = encodeClassDef.getBytes(0, encodeClassDef.writerIndex());
    return new ClassDef(
      Encoders.buildClassSpec(type),
      fieldInfos,
      true,
      encodeClassDef.getInt64(0),
      classDefBytes);
  }

  static final int SMALL_NUM_FIELDS_THRESHOLD = 0b11111;
  static final int REGISTER_BY_NAME_FLAG = 0b100000;
  static final int COMPRESS_META_FLAG = 0b1 << 13;
  static final int META_SIZE_THRESHOLD = 0b1 << 12;
  static final int NUM_HASH_BITS = 50;

  // see spec documentation: docs/specification/xlang_serialization_spec.md
  // https://fury.apache.org/docs/specification/fury_xlang_serialization_spec
  static MemoryBuffer encodeClassDef(
    XtypeResolver resolver,
    Class<?> type,
    List<FieldInfo> fields) {
    ClassInfo classInfo = resolver.getClassInfo(type);
    Preconditions.checkArgument(Types.isStructType(classInfo.getXtypeId()),
      "%s is not a struct", type);
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(128);
    buffer.writeByte(-1); // placeholder for header, update later
    int currentClassHeader = fields.size();
    if (fields.size() >= SMALL_NUM_FIELDS_THRESHOLD) {
      currentClassHeader = SMALL_NUM_FIELDS_THRESHOLD;
      buffer.writeVarUint32(fields.size() - SMALL_NUM_FIELDS_THRESHOLD);
    }
    if (resolver.isRegisteredById(type)) {
      buffer.writeVarUint32(classInfo.getXtypeId());
    } else {
      Preconditions.checkArgument(resolver.isRegisteredByName(type));
      currentClassHeader |= REGISTER_BY_NAME_FLAG;
      String ns = classInfo.decodeNamespace();
      String typename = classInfo.decodeTypeName();
      writePkgName(buffer, ns);
      writeTypeName(buffer, typename);
    }
    buffer.putByte(0, currentClassHeader);
    writeFieldsInfo(buffer, fields);

    byte[] compressed =
      resolver
        .getFury()
        .getMetaCompressor()
        .compress(buffer.getHeapMemory(), 0, buffer.writerIndex());
    boolean isCompressed = false;
    if (compressed.length < buffer.writerIndex()) {
      isCompressed = true;
      buffer = MemoryBuffer.fromByteArray(compressed);
      buffer.writerIndex(compressed.length);
    }
    int metaSize = buffer.writerIndex();
    if (metaSize > META_SIZE_THRESHOLD) {
      throw new UnsupportedOperationException("Too big metadata size: " + metaSize);
    }
    long hash =
      MurmurHash3.murmurhash3_x64_128(
        buffer.getHeapMemory(), 0, metaSize, 47)[0];
    long header = 0;
    hash <<= 14;
    // this id will be part of generated codec, a negative number won't be allowed in class name.
    header |= Math.abs(hash);
    if (isCompressed) {
      header |= COMPRESS_META_FLAG;
    }
    header |= metaSize;

    MemoryBuffer result = MemoryUtils.buffer(metaSize + 8);
    result.writeInt64(header);
    result.writeByte(metaSize);
    result.writeBytes(buffer.getHeapMemory(), 0, metaSize);
    return result;
  }

  static Map<String, FieldInfo> getClassFields(Class<?> type, List<FieldInfo> fieldsInfo) {
    Map<String, FieldInfo> sortedClassFields = new LinkedHashMap<>();
    Map<String, List<FieldInfo>> classFields = ClassDefEncoder.groupClassFields(fieldsInfo);
    for (Class<?> clz : ReflectionUtils.getAllClasses(type, true)) {
      List<FieldInfo> fieldInfos = classFields.get(clz.getName());
      if (fieldInfos != null) {
        for (FieldInfo fieldInfo : fieldInfos) {
          sortedClassFields.put(fieldInfo.getFieldName(), fieldInfo);
        }
      }
    }
    return sortedClassFields;
  }

  /**
   * Write field type and name info.
   */
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
      if (fieldInfo.hasTag()) {
        size = fieldInfo.getTag();
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
      if (!fieldInfo.hasTag()) {
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
