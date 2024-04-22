package org.apache.fury.meta;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.meta.MetaString.Encoding;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.DescriptorGrouper;
import org.apache.fury.util.MurmurHash3;
import org.apache.fury.util.Platform;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;

class ClassDefEncoder {
  private static final ConcurrentMap<String, MetaString> metaStringCache =
      new ConcurrentHashMap<>();

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

  // Overall spec:
  // ```
  // |      8 bytes meta header      |   variable bytes   |  variable bytes   | variable bytes |
  // +-------------------------------+--------------------+-------------------+----------------+
  // | 7 bytes hash + 1 bytes header | current class meta | parent class meta |      ...       |
  // ```
  // Single layer:
  // ```
  // |      unsigned varint       |      meta string      |     meta string     |  field info:
  // variable bytes   | variable bytes  | ... |
  // +----------------------------+-----------------------+---------------------+-------------------------------+-----------------+-----+
  // | num fields + register flag | header + package name | header + class name | header + type id +
  // field name | next field info | ... |
  // ```
  // For more details, see spec documentation:
  // https://fury.apache.org/docs/specification/fury_java_serialization_spec
  private static MemoryBuffer encodeClassDef(
      ClassResolver classResolver,
      Class<?> type,
      List<ClassDef.FieldInfo> fieldsInfo,
      Map<String, String> extMeta) {
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    buffer.writeInt64(0);
    long header;
    Map<String, List<ClassDef.FieldInfo>> classFields = getClassFields(type, fieldsInfo);
    int size = classFields.size();
    if (size > 0b1110) {
      header = 0b1111;
      buffer.writeVarUint32Small7(size - 0b1110);
    } else {
      header = size;
    }
    header |= 0b10000;
    if (!extMeta.isEmpty()) {
      header |= 0b100000;
    }
    buffer.putInt64(0, header);
    for (Map.Entry<String, List<ClassDef.FieldInfo>> entry : classFields.entrySet()) {
      String className = entry.getKey();
      List<ClassDef.FieldInfo> fields = entry.getValue();
      // | num fields + register flag | header + package name | header + class name
      // | header + type id + field name | next field info | ... |
      int currentClassHeader = (fields.size() << 1);
      if (classResolver.isRegistered(type)) {
        currentClassHeader |= 1;
        buffer.writeVarUint32Small7(currentClassHeader);
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
    buffer.putInt64(0, header);
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
      MetaString metaString =
          metaStringCache.computeIfAbsent(
              fieldInfo.getFieldName(),
              k -> MetaStringEncoder.FIELD_NAME_ENCODER.encode(fieldInfo.getFieldName()));
      ClassDef.FieldType fieldType = fieldInfo.getFieldType();
      byte header = (byte) (fieldType.isMonomorphic() ? 1 : 0);
      header |= (byte) (metaString.getEncoding().getValue() << 3);
      if (metaString.stripLastChar()) {
        header |= 0b1000000;
      }
      buffer.writeByte(header);
      // TODO(chaokunyang) Only write field name when tag id not used
      buffer.writePrimitiveArrayWithSize(
          metaString.getBytes(), Platform.BYTE_ARRAY_OFFSET, metaString.getBytes().length);

      if (fieldType instanceof ClassDef.RegisteredFieldType) {
        short classId = ((ClassDef.RegisteredFieldType) fieldType).getClassId();
        buffer.writeVarUint32(((3 + classId) << 1));
      } else if (fieldType instanceof ClassDef.CollectionFieldType) {
        buffer.writeByte((2 << 1));
        ((ClassDef.CollectionFieldType) fieldType).getElementType().write(buffer);
      } else if (fieldType instanceof ClassDef.MapFieldType) {
        buffer.writeByte((1 << 1));
        ClassDef.MapFieldType mapFieldType = (ClassDef.MapFieldType) fieldType;
        mapFieldType.getKeyType().write(buffer);
        mapFieldType.getValueType().write(buffer);
      } else {
        Preconditions.checkArgument(fieldType instanceof ClassDef.ObjectFieldType);
        buffer.writeByte(header);
      }
    }
  }

  private static void writePkgName(MemoryBuffer buffer, String pkg) {
    // Package name encoding algorithm: `UTF8/LOWER_SPECIAL/LOWER_UPPER_DIGIT_SPECIAL`
    // - Header:
    //  - If meta string encoding is `LOWER_SPECIAL` and the length of encoded string `<=` 63, then
    // header will be
    //    `6 bits size | strip last char flag < 1 | 0b1`.
    //  - If meta string encoding is `LOWER_UPPER_DIGIT_SPECIAL` and the length of encoded string
    // `<=` 31, then
    //    header will be `5 bits size | strip last char flag < 2 | 0b11`.
    //  - Otherwise, encode string using `UTF8`, header: `size << 3 | strip last char flag < 2 |
    // 0b01` as an
    //    unsigned varint.
    MetaString pkgMetaString =
        metaStringCache.computeIfAbsent(pkg, MetaStringEncoder.PACKAGE_ENCODER::encode);
    int stripLastChar = pkgMetaString.stripLastChar() ? 1 : 0;
    byte[] encoded = pkgMetaString.getBytes();
    Encoding encoding = pkgMetaString.getEncoding();
    int pkgHeader = encoded.length;
    if (encoding == Encoding.LOWER_SPECIAL && encoded.length <= 63) {
      pkgHeader = (pkgHeader << 2) | (stripLastChar << 1) | 0b1;
    } else if (encoding == Encoding.LOWER_UPPER_DIGIT_SPECIAL && encoded.length <= 31) {
      pkgHeader = (pkgHeader << 3) | (stripLastChar << 2) | 0b11;
    } else {
      pkgHeader = (pkgHeader << 3) | (stripLastChar << 2) | 0b01;
      if (encoding != Encoding.UTF_8) {
        encoded = pkg.getBytes(StandardCharsets.UTF_8);
      }
    }
    buffer.writeVarUint32Small7(pkgHeader);
    buffer.writeBytes(encoded);
  }

  private static void writeTypeName(MemoryBuffer buffer, String typeName) {
    // - Encoding algorithm:
    // `UTF8/LOWER_UPPER_DIGIT_SPECIAL/FIRST_TO_LOWER_SPECIAL/ALL_TO_LOWER_SPECIAL`
    // - header:
    //   - If meta string encoding is `LOWER_UPPER_DIGIT_SPECIAL/ALL_TO_LOWER_SPECIAL` and the
    // length of encoded string
    //       `<=` 31, then header will be `5 bits size | strip last char flag << 2 | encoding flag |
    // 0b1`.
    //     - encoding flag 0: LOWER_UPPER_DIGIT_SPECIAL
    //     - encoding flag 1: ALL_TO_LOWER_SPECIAL
    //   - Otherwise, use `FIRST_TO_LOWER_SPECIAL/UTF8` encoding only, header:
    //       `size << 3 | strip last char flag | encoding flag | 0b0` as an unsigned varint.
    //     - encoding flag 0: FIRST_TO_LOWER_SPECIAL. If use this encoding, only first char is upper
    // case, the size
    //       won't exceed 16 mostly, thus the header can be written in one byte.
    //     - encoding flag 1: UTF8
    MetaString clsMetaString =
        metaStringCache.computeIfAbsent(typeName, MetaStringEncoder.TYPE_NAME_ENCODER::encode);
    int stripLastChar = clsMetaString.stripLastChar() ? 1 : 0;
    byte[] encoded = clsMetaString.getBytes();
    Encoding encoding = clsMetaString.getEncoding();
    int typeHeader = encoded.length;
    if (encoded.length <= 31
        && (encoding == Encoding.LOWER_UPPER_DIGIT_SPECIAL
            || encoding == Encoding.ALL_TO_LOWER_SPECIAL)) {
      int encodingFlag = encoding == Encoding.LOWER_UPPER_DIGIT_SPECIAL ? 0b00 : 0b10;
      typeHeader = (typeHeader << 3) | stripLastChar << 2 | encodingFlag | 0b1;
    } else {
      int encodingFlag = encoding == Encoding.FIRST_TO_LOWER_SPECIAL ? 0b00 : 0b10;
      typeHeader = (typeHeader << 3) | stripLastChar << 2 | encodingFlag | 0b1;
      if (encoding != Encoding.UTF_8) {
        encoded = typeName.getBytes(StandardCharsets.UTF_8);
      }
    }
    buffer.writeVarUint32Small7(typeHeader);
    buffer.writeBytes(encoded);
  }
}
