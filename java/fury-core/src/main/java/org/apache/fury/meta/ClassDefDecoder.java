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

import static org.apache.fury.meta.Encoders.fieldNameEncodings;
import static org.apache.fury.meta.Encoders.pkgEncodings;
import static org.apache.fury.meta.Encoders.typeNameEncodings;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.meta.ClassDef.FieldType;
import org.apache.fury.meta.MetaString.Encoding;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.util.Preconditions;

/**
 * See spec documentation: docs/specification/java_serialization_spec.md <a
 * href="https://fury.apache.org/docs/specification/fury_java_serialization_spec">...</a>
 */
class ClassDefDecoder {
  public static ClassDef decodeClassDef(ClassResolver classResolver, MemoryBuffer buffer) {
    int idx = buffer.readerIndex();
    long id = buffer.readInt64();
    byte[] encoded = buffer.getBytes(idx, buffer.readerIndex() - idx);
    long bulkValue = buffer.readInt64();
    long header = bulkValue & 0xff;
    long hash = bulkValue >>> 8;
    int numClasses = (int) (header & 0b1111);
    if (numClasses == 0b1111) {
      numClasses += buffer.readVarUint32Small7();
    }
    numClasses += 1;
    String className = null;
    List<ClassDef.FieldInfo> classFields = new ArrayList<>();
    for (int i = 0; i < numClasses; i++) {
      // | num fields + register flag | header + package name | header + class name
      // | header + type id + field name | next field info | ... |
      int currentClassHeader = buffer.readVarUint32Small7();
      boolean isRegistered = (currentClassHeader & 0b1) != 0;
      int numFields = currentClassHeader >>> 1;
      String fullClassName;
      if (isRegistered) {
        int registeredId = buffer.readVarUint32Small7();
        fullClassName = classResolver.getClassInfo((short) registeredId).getCls().getName();
      } else {
        String pkg = readPkgName(buffer);
        String typeName = readTypeName(buffer);
        fullClassName = ReflectionUtils.getFullClassName(pkg, typeName);
      }
      className = fullClassName;
      List<ClassDef.FieldInfo> fieldInfos = readFieldsInfo(buffer, fullClassName, numFields);
      classFields.addAll(fieldInfos);
    }
    boolean hasExtMeta = (header & 0b100000) != 0;
    Map<String, String> extMeta = new HashMap<>();
    if (hasExtMeta) {
      int extMetaSize = buffer.readVarUint32Small7();
      for (int i = 0; i < extMetaSize; i++) {
        extMeta.put(
            new String(buffer.readBytesAndSize(), StandardCharsets.UTF_8),
            new String(buffer.readBytesAndSize(), StandardCharsets.UTF_8));
      }
    }
    return new ClassDef(className, classFields, extMeta, id, encoded);
  }

  private static List<ClassDef.FieldInfo> readFieldsInfo(
      MemoryBuffer buffer, String className, int numFields) {
    List<ClassDef.FieldInfo> fieldInfos = new ArrayList<>(numFields);
    for (int i = 0; i < numFields; i++) {
      byte header = buffer.readByte();
      //  `3 bits size + 2 bits field name encoding + polymorphism flag + nullability flag + ref
      // tracking flag`
      // TODO(chaokunyang) read type tag
      int encodingFlags = (header >>> 3) & 0b11;
      boolean useTagID = encodingFlags == 3;
      Preconditions.checkArgument(!useTagID, "Type tag not supported currently");
      int size = header >>> 5;
      if (size == 7) {
        size += buffer.readVarUint32Small7();
      }
      size += 1; // not +1 when use tag id.
      Encoding encoding = fieldNameEncodings[encodingFlags];
      String fieldName = Encoders.FIELD_NAME_DECODER.decode(buffer.readBytes(size), encoding);
      boolean isMonomorphic = (header & 0b100) != 0;
      int typeId = buffer.readVarUint32Small14();
      FieldType fieldType = FieldType.read(buffer, isMonomorphic, typeId);
      fieldInfos.add(new ClassDef.FieldInfo(className, fieldName, fieldType));
    }
    return fieldInfos;
  }

  private static String readPkgName(MemoryBuffer buffer) {
    // - Package name encoding(omitted when class is registered):
    //    - encoding algorithm: `UTF8/ALL_TO_LOWER_SPECIAL/LOWER_UPPER_DIGIT_SPECIAL`
    //    - Header: `6 bits size | 2 bits encoding flags`.
    //      The `6 bits size: 0~63`  will be used to indicate size `0~62`,
    //      the value `63` the size need more byte to read, the encoding will encode `size - 62` as
    // a varint next.
    byte header = buffer.readByte();
    int encodingFlags = header & 0b11;
    Encoding encoding = pkgEncodings[encodingFlags];
    return readName(Encoders.PACKAGE_DECODER, buffer, header, encoding, 62);
  }

  private static String readTypeName(MemoryBuffer buffer) {
    // - Class name encoding(omitted when class is registered):
    //     - encoding algorithm:
    // `UTF8/LOWER_UPPER_DIGIT_SPECIAL/FIRST_TO_LOWER_SPECIAL/ALL_TO_LOWER_SPECIAL`
    //     - header: `6 bits size | 2 bits encoding flags`.
    //       The `6 bits size: 0~63`  will be used to indicate size `1~64`,
    //       the value `63` the size need more byte to read, the encoding will encode `size - 63` as
    // a varint next.
    byte header = buffer.readByte();
    int encodingFlags = header & 0b11;
    Encoding encoding = typeNameEncodings[encodingFlags];
    return readName(Encoders.TYPE_NAME_DECODER, buffer, header, encoding, 63);
  }

  private static String readName(
      MetaStringDecoder decoder, MemoryBuffer buffer, byte header, Encoding encoding, int max) {
    int size = header >> 2;
    if (size == max) {
      size = buffer.readVarUint32Small7() + max;
    }
    return decoder.decode(buffer.readBytes(size), encoding);
  }
}
