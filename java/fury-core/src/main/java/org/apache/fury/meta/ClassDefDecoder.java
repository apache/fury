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
import static org.apache.fury.meta.ClassDef.SIZE_TWO_BYTES_FLAG;
import static org.apache.fury.meta.ClassDefEncoder.BIG_NAME_THRESHOLD;
import static org.apache.fury.meta.Encoders.fieldNameEncodings;
import static org.apache.fury.meta.Encoders.pkgEncodings;
import static org.apache.fury.meta.Encoders.typeNameEncodings;

import java.util.ArrayList;
import java.util.List;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.meta.ClassDef.FieldType;
import org.apache.fury.meta.MetaString.Encoding;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.NonexistentClass;
import org.apache.fury.util.Preconditions;

/**
 * An decoder which decode binary into {@link ClassDef}. See spec documentation:
 * docs/specification/java_serialization_spec.md <a
 * href="https://fury.apache.org/docs/specification/fury_java_serialization_spec">...</a>
 */
class ClassDefDecoder {
  public static ClassDef decodeClassDef(ClassResolver classResolver, MemoryBuffer buffer, long id) {
    boolean sizeTwoBytes = (id & SIZE_TWO_BYTES_FLAG) != 0;
    MemoryBuffer encoded = MemoryBuffer.newHeapBuffer(32);
    encoded.writeInt64(id);
    int size;
    if (sizeTwoBytes) {
      size = buffer.readInt16() & 0xffff;
      encoded.writeInt16((short) size);
    } else {
      size = buffer.readByte() & 0xff;
      encoded.writeByte(size);
    }
    byte[] encodedClassDef = buffer.readBytes(size);
    encoded.writeBytes(encodedClassDef);
    if ((id & COMPRESSION_FLAG) != 0) {
      encodedClassDef =
          classResolver
              .getFury()
              .getConfig()
              .getMetaCompressor()
              .decompress(encodedClassDef, 0, size);
    }
    MemoryBuffer classDefBuf = MemoryBuffer.fromByteArray(encodedClassDef);
    long header = id & 0xff;
    int numClasses = (int) (header & 0b1111);
    if (numClasses == 0b1111) {
      numClasses += classDefBuf.readVarUint32Small7();
    }
    numClasses += 1;
    String className;
    List<ClassDef.FieldInfo> classFields = new ArrayList<>();
    ClassSpec classSpec = null;
    for (int i = 0; i < numClasses; i++) {
      // | num fields + register flag | header + package name | header + class name
      // | header + type id + field name | next field info | ... |
      int currentClassHeader = classDefBuf.readVarUint32Small7();
      boolean isRegistered = (currentClassHeader & 0b1) != 0;
      int numFields = currentClassHeader >>> 1;
      if (isRegistered) {
        short registeredId = (short) classDefBuf.readVarUint32Small7();
        if (classResolver.getRegisteredClass(registeredId) == null) {
          classSpec = new ClassSpec(NonexistentClass.NonexistentMetaShared.class);
          className = classSpec.entireClassName;
        } else {
          Class<?> cls = classResolver.getClassInfo(registeredId).getCls();
          className = cls.getName();
          classSpec = new ClassSpec(cls);
        }
      } else {
        String pkg = readPkgName(classDefBuf);
        String typeName = readTypeName(classDefBuf);
        classSpec = Encoders.decodePkgAndClass(pkg, typeName);
        className = classSpec.entireClassName;
        if (classResolver.isRegisteredByName(className)) {
          Class<?> cls = classResolver.getRegisteredClass(className);
          className = cls.getName();
          classSpec = new ClassSpec(cls);
        }
      }
      List<ClassDef.FieldInfo> fieldInfos = readFieldsInfo(classDefBuf, className, numFields);
      classFields.addAll(fieldInfos);
    }
    Preconditions.checkNotNull(classSpec);
    boolean isObjectType = (header & ClassDef.OBJECT_TYPE_FLAG) != 0;
    return new ClassDef(
        classSpec, classFields, isObjectType, id, encoded.getBytes(0, encoded.writerIndex()));
  }

  private static List<ClassDef.FieldInfo> readFieldsInfo(
      MemoryBuffer buffer, String className, int numFields) {
    List<ClassDef.FieldInfo> fieldInfos = new ArrayList<>(numFields);
    for (int i = 0; i < numFields; i++) {
      int header = buffer.readByte() & 0xff;
      //  `3 bits size + 2 bits field name encoding + polymorphism flag + nullability flag + ref
      // tracking flag`
      // TODO(chaokunyang) read type tag
      int encodingFlags = (header >>> 3) & 0b11;
      boolean useTagID = encodingFlags == 3;
      Preconditions.checkArgument(
          !useTagID, "Type tag not supported currently, parsed fieldInfos %s", fieldInfos);
      int size = header >>> 5;
      if (size == 7) {
        size += buffer.readVarUint32Small7();
      }
      size += 1;
      Encoding encoding = fieldNameEncodings[encodingFlags];
      String fieldName = Encoders.FIELD_NAME_DECODER.decode(buffer.readBytes(size), encoding);
      boolean isMonomorphic = (header & 0b100) != 0;
      boolean trackingRef = (header & 0b001) != 0;
      int typeId = buffer.readVarUint32Small14();
      FieldType fieldType = FieldType.read(buffer, isMonomorphic, trackingRef, typeId);
      fieldInfos.add(new ClassDef.FieldInfo(className, fieldName, fieldType));
    }
    return fieldInfos;
  }

  private static String readPkgName(MemoryBuffer buffer) {
    // - Package name encoding(omitted when class is registered):
    //    - encoding algorithm: `UTF8/ALL_TO_LOWER_SPECIAL/LOWER_UPPER_DIGIT_SPECIAL`
    //    - Header: `6 bits size | 2 bits encoding flags`.
    //      The `6 bits size: 0~63`  will be used to indicate size `0~63`,
    //      the value `63` the size need more byte to read, the encoding will encode `size - 63` as
    // a varint next.
    return readName(Encoders.PACKAGE_DECODER, buffer, pkgEncodings);
  }

  private static String readTypeName(MemoryBuffer buffer) {
    // - Class name encoding(omitted when class is registered):
    //     - encoding algorithm:
    // `UTF8/LOWER_UPPER_DIGIT_SPECIAL/FIRST_TO_LOWER_SPECIAL/ALL_TO_LOWER_SPECIAL`
    //     - header: `6 bits size | 2 bits encoding flags`.
    //      The `6 bits size: 0~63`  will be used to indicate size `0~63`,
    //       the value `63` the size need more byte to read, the encoding will encode `size - 63` as
    // a varint next.
    return readName(Encoders.TYPE_NAME_DECODER, buffer, typeNameEncodings);
  }

  private static String readName(
      MetaStringDecoder decoder, MemoryBuffer buffer, Encoding[] encodings) {
    int header = buffer.readByte() & 0xff;
    int encodingFlags = header & 0b11;
    Encoding encoding = encodings[encodingFlags];
    int size = header >> 2;
    if (size == BIG_NAME_THRESHOLD) {
      size = buffer.readVarUint32Small7() + BIG_NAME_THRESHOLD;
    }
    return decoder.decode(buffer.readBytes(size), encoding);
  }
}
