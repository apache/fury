/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.serializer;

import com.google.common.base.Preconditions;
import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.type.Type;
import io.fury.util.MathUtils;
import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * String serializer based on {@link sun.misc.Unsafe} and {@link MethodHandle} for speed.
 *
 * <p>Note that string operations is very common in serialization, and jvm inline and branch
 * elimination is not reliable even in c2 compiler, so we try to inline and avoid checks as we can
 * manually.
 *
 * @author chaokunyang
 */
@SuppressWarnings("unchecked")
public final class StringSerializer extends Serializer<String> {
  private static final long STRING_CODER_FIELD_OFFSET;
  private static final long STRING_VALUE_FIELD_OFFSET;
  private static final boolean STRING_VALUE_FIELD_IS_CHARS;
  private static final boolean STRING_VALUE_FIELD_IS_BYTES;
  private static final long STRING_OFFSET_FIELD_OFFSET;
  // String length field for android.
  private static final long STRING_COUNT_FIELD_OFFSET;
  private static final byte LATIN1 = 0;
  private static final byte UTF16 = 1;
  private static final int DEFAULT_BUFFER_SIZE = 1024;
  // A long mask used to clear all-higher bits of char in a super-word way.
  private static final long MULTI_CHARS_NON_ASCII_MASK;

  static {
    Field valueField = ReflectionUtils.getFieldNullable(String.class, "value");
    // Java8 string
    STRING_VALUE_FIELD_IS_CHARS = valueField != null && valueField.getType() == char[].class;
    // Java11 string
    STRING_VALUE_FIELD_IS_BYTES = valueField != null && valueField.getType() == byte[].class;
    STRING_VALUE_FIELD_OFFSET = ReflectionUtils.getFieldOffset(String.class, "value");
    STRING_CODER_FIELD_OFFSET = ReflectionUtils.getFieldOffset(String.class, "coder");
    STRING_OFFSET_FIELD_OFFSET = ReflectionUtils.getFieldOffset(String.class, "offset");
    STRING_COUNT_FIELD_OFFSET = ReflectionUtils.getFieldOffset(String.class, "count");
    Preconditions.checkArgument(STRING_OFFSET_FIELD_OFFSET == -1, "Current jdk not supported");
    Preconditions.checkArgument(STRING_COUNT_FIELD_OFFSET == -1, "Current jdk not supported");
    if (Platform.IS_LITTLE_ENDIAN) {
      // ascii chars will be 0xXX,0x00;0xXX,0x00 in byte order;
      // Using 0x00,0xff(0xff00) to clear ascii bits.
      MULTI_CHARS_NON_ASCII_MASK = 0xff00ff00ff00ff00L;
    } else {
      // ascii chars will be 0x00,0xXX;0x00,0xXX in byte order;
      // Using 0x00,0xff(0x00ff) to clear ascii bits.
      MULTI_CHARS_NON_ASCII_MASK = 0x00ff00ff00ff00ffL;
    }
  }

  private final boolean compressString;
  private byte[] byteArray = new byte[DEFAULT_BUFFER_SIZE];
  private int smoothByteArrayLength = DEFAULT_BUFFER_SIZE;

  public StringSerializer(Fury fury) {
    super(fury, String.class, fury.trackingReference() && !fury.isStringReferenceIgnored());
    compressString = fury.compressString();
  }

  @Override
  public short getCrossLanguageTypeId() {
    return Type.STRING.getId();
  }

  @Override
  public void write(MemoryBuffer buffer, String value) {
    writeJavaString(buffer, value);
  }

  @Override
  public void crossLanguageWrite(MemoryBuffer buffer, String value) {
    writeUTF8String(buffer, value);
  }

  @Override
  public String read(MemoryBuffer buffer) {
    return readJavaString(buffer);
  }

  @Override
  public String crossLanguageRead(MemoryBuffer buffer) {
    return readUTF8String(buffer);
  }

  public void writeString(MemoryBuffer buffer, String value) {
    if (isJava) {
      writeJavaString(buffer, value);
    } else {
      writeUTF8String(buffer, value);
    }
  }

  public String readString(MemoryBuffer buffer) {
    if (isJava) {
      return readJavaString(buffer);
    } else {
      return readUTF8String(buffer);
    }
  }

  private byte[] getByteArray(int numElements) {
    byte[] byteArray = this.byteArray;
    if (byteArray.length < numElements) {
      byteArray = new byte[numElements];
      this.byteArray = byteArray;
    }
    if (byteArray.length > DEFAULT_BUFFER_SIZE) {
      smoothByteArrayLength =
          Math.max(((int) (smoothByteArrayLength * 0.9 + numElements * 0.1)), DEFAULT_BUFFER_SIZE);
      if (smoothByteArrayLength <= DEFAULT_BUFFER_SIZE) {
        this.byteArray = new byte[DEFAULT_BUFFER_SIZE];
      }
    }
    return byteArray;
  }

  // Invoked by fury JIT
  public void writeJavaString(MemoryBuffer buffer, String value) {
    if (STRING_VALUE_FIELD_IS_BYTES) {
      writeJDK11String(buffer, value);
    } else {
      if (!STRING_VALUE_FIELD_IS_CHARS) {
        throw new UnsupportedOperationException();
      }
      final char[] chars = (char[]) Platform.getObject(value, STRING_VALUE_FIELD_OFFSET);
      if (compressString) {
        if (isAscii(chars)) {
          writeJDK8Ascii(buffer, chars);
        } else {
          writeJDK8UTF16(buffer, chars);
        }
      } else {
        int numBytes = MathUtils.doubleExact(value.length());
        buffer.writePrimitiveArrayWithSizeEmbedded(chars, Platform.CHAR_ARRAY_OFFSET, numBytes);
      }
    }
  }

  public static boolean isAscii(char[] chars) {
    int numChars = chars.length;
    int vectorizedLen = numChars >> 2;
    int vectorizedChars = vectorizedLen << 2;
    int endOffset = Platform.CHAR_ARRAY_OFFSET + (vectorizedChars << 1);
    boolean isAscii = true;
    for (int offset = Platform.CHAR_ARRAY_OFFSET; offset < endOffset; offset += 8) {
      // check 4 chars in a vectorized way, 4 times faster than scalar check loop.
      // See benchmark in CompressStringSuite.asciiSuperWordCheck.
      long multiChars = Platform.getLong(chars, offset);
      if ((multiChars & MULTI_CHARS_NON_ASCII_MASK) != 0) {
        isAscii = false;
        break;
      }
    }
    if (isAscii) {
      for (int i = vectorizedChars; i < numChars; i++) {
        if (chars[i] > 0xFF) {
          isAscii = false;
          break;
        }
      }
    }
    return isAscii;
  }

  // Invoked by fury JIT
  public String readJavaString(MemoryBuffer buffer) {
    if (STRING_VALUE_FIELD_IS_BYTES) {
      if (Platform.JAVA_VERSION >= 17) {
        // Seems neither Unsafe.put nor MethodHandle are available in JDK17+,
        // `Unsafe.put` doesn't work on IDE, but works on command.
        // But `Unsafe.put` is 50% slower than `readStringChars`, so just inflate ant copy here.
        byte coder = buffer.readByte();
        if (coder == LATIN1) {
          return new String(readAsciiChars(buffer));
        } else {
          return new String(readUTF16Chars(buffer, coder));
        }
      } else {
        byte coder = buffer.readByte();
        byte[] value = buffer.readBytesWithSizeEmbedded();
        return newJava11StringByZeroCopy(value, coder);
      }
    } else {
      if (!STRING_VALUE_FIELD_IS_CHARS) {
        throw new UnsupportedOperationException();
      }
      if (compressString) {
        byte coder = buffer.readByte();
        if (coder == LATIN1) {
          return newJava8StringByZeroCopy(readAsciiChars(buffer));
        } else {
          return newJava8StringByZeroCopy(readUTF16Chars(buffer, coder));
        }
      } else {
        return newJava8StringByZeroCopy(buffer.readCharsWithSizeEmbedded());
      }
    }
  }

  static void writeJDK11String(MemoryBuffer buffer, String value) {
    byte[] bytes = (byte[]) Platform.getObject(value, STRING_VALUE_FIELD_OFFSET);
    byte coder = Platform.getByte(value, STRING_CODER_FIELD_OFFSET);
    buffer.writeByte(coder);
    buffer.writePrimitiveArrayWithSizeEmbedded(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length);
  }

  private void writeJDK8Ascii(MemoryBuffer buffer, char[] chars) {
    buffer.writeByte(LATIN1);
    final int strLen = chars.length;
    int writerIndex = buffer.writerIndex();
    // The `ensure` ensure next operations are safe without bound checks,
    // and inner heap buffer doesn't change.
    buffer.ensure(writerIndex + 5 + strLen);
    final byte[] targetArray = buffer.getHeapMemory();
    writerIndex += buffer.unsafeWritePositiveVarInt(strLen);
    if (targetArray != null) {
      final int targetIndex = buffer.unsafeHeapWriterIndex();
      for (int i = 0; i < strLen; i++) {
        targetArray[targetIndex + i] = (byte) chars[i];
      }
    } else {
      final byte[] tmpArray = getByteArray(strLen);
      // Write to heap memory then copy is 60% faster than unsafe write to direct memory.
      for (int i = 0; i < strLen; i++) {
        tmpArray[i] = (byte) chars[i];
      }
      buffer.put(writerIndex, tmpArray, 0, strLen);
    }
    buffer.writerIndex(writerIndex + strLen);
  }

  private void writeJDK8UTF16(MemoryBuffer buffer, char[] chars) {
    buffer.writeByte(UTF16);
    int strLen = chars.length;
    int numBytes = MathUtils.doubleExact(strLen);
    if (Platform.IS_LITTLE_ENDIAN) {
      // FIXME JDK11 utf16 string uses little-endian order.
      buffer.writePrimitiveArrayWithSizeEmbedded(chars, Platform.CHAR_ARRAY_OFFSET, numBytes);
    } else {
      // The `ensure` ensure next operations are safe without bound checks,
      // and inner heap buffer doesn't change.
      int writerIndex = buffer.writerIndex();
      buffer.ensure(writerIndex + 5 + numBytes);
      byte[] targetArray = buffer.getHeapMemory();
      writerIndex += buffer.unsafeWritePositiveVarInt(numBytes);
      if (targetArray != null) {
        // Write to heap memory then copy is 250% faster than unsafe write to direct memory.
        int charIndex = 0;
        for (int i = buffer.unsafeHeapWriterIndex(), end = i + numBytes; i < end; i += 2) {
          char c = chars[charIndex++];
          targetArray[i] = (byte) (c >> StringUTF16.HI_BYTE_SHIFT);
          targetArray[i + 1] = (byte) (c >> StringUTF16.LO_BYTE_SHIFT);
        }
      } else {
        byte[] tmpArray = getByteArray(strLen);
        int charIndex = 0;
        for (int i = 0; i < numBytes; i += 2) {
          char c = chars[charIndex++];
          tmpArray[i] = (byte) (c >> StringUTF16.HI_BYTE_SHIFT);
          tmpArray[i + 1] = (byte) (c >> StringUTF16.LO_BYTE_SHIFT);
        }
        buffer.put(writerIndex, tmpArray, 0, numBytes);
      }
      buffer.writerIndex(writerIndex + numBytes);
    }
  }

  private char[] readAsciiChars(MemoryBuffer buffer) {
    final int numBytes = buffer.readPositiveVarInt();
    char[] chars = new char[numBytes];
    byte[] targetArray = buffer.getHeapMemory();
    if (targetArray != null) {
      int srcIndex = buffer.unsafeHeapReaderIndex();
      for (int i = 0; i < numBytes; i++) {
        chars[i] = (char) (targetArray[srcIndex++] & 0xff);
      }
      buffer.increaseReaderIndexUnsafe(numBytes);
    } else {
      byte[] byteArray = getByteArray(numBytes);
      buffer.readBytes(byteArray, 0, numBytes);
      for (int i = 0; i < numBytes; i++) {
        chars[i] = (char) (byteArray[i] & 0xff);
      }
    }
    return chars;
  }

  private char[] readUTF16Chars(MemoryBuffer buffer, byte coder) {
    if (coder != UTF16) {
      throw new UnsupportedOperationException(String.format("Unsupported coder %s", coder));
    }
    int numBytes = buffer.readPositiveVarInt();
    int strLen = numBytes >> 1;
    char[] chars = new char[strLen];
    if (Platform.IS_LITTLE_ENDIAN) {
      // FIXME JDK11 utf16 string uses little-endian order.
      buffer.readChars(chars, Platform.CHAR_ARRAY_OFFSET, numBytes);
    } else {
      final byte[] targetArray = buffer.getHeapMemory();
      if (targetArray != null) {
        buffer.checkReadableBytes(numBytes);
        int charIndex = 0;
        for (int i = buffer.unsafeHeapReaderIndex(), end = i + numBytes; i < end; i += 2) {
          char c =
              (char)
                  ((targetArray[i] & 0xff << StringUTF16.HI_BYTE_SHIFT)
                      | ((targetArray[i + 1] & 0xff) << StringUTF16.LO_BYTE_SHIFT));
          chars[charIndex++] = c;
        }
        buffer.increaseReaderIndexUnsafe(numBytes);
      } else {
        final byte[] tmpArray = getByteArray(numBytes);
        buffer.readBytes(tmpArray, 0, numBytes);
        int charIndex = 0;
        for (int i = 0; i < numBytes; i += 2) {
          char c =
              (char)
                  ((tmpArray[i] & 0xff << StringUTF16.HI_BYTE_SHIFT)
                      | ((tmpArray[i + 1] & 0xff) << StringUTF16.LO_BYTE_SHIFT));
          chars[charIndex++] = c;
        }
      }
    }
    return chars;
  }

  public static String newJava8StringByZeroCopy(char[] data) {
    if (Platform.JAVA_VERSION != 8) {
      throw new IllegalStateException(
          String.format("Current java version is %s", Platform.JAVA_VERSION));
    }
    try {
      if (JAVA8_STRING_ZERO_COPY_CTR == null) {
        // 1. As documented in `Subsequent Modification of final Fields` in
        // https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#d5e34106
        // Maybe we can use `UNSAFE.putObject` to update String field to avoid reflection overhead.
        // 2. `setAccessible` is an illegal-reflective-access because zero-copy String constructor
        // isn't public, and `java.base/java.lang` isn't open to fury by default.
        // 3. JavaLangAccess#newStringUnsafe is used by jdk internally and won't be available
        // in jdk11 if `jdk.internal.misc` are not exported, so we don't use it.
        // StringBuffer#toString is a synchronized method, so we don't use it to create String.
        String str = Platform.newInstance(String.class);
        Platform.putObject(str, STRING_VALUE_FIELD_OFFSET, data);
        // unsafe is 800% faster than copy for length 230.
        return str;
      } else {
        // 25% faster than unsafe put field, only 10% slower than `new String(str)`
        return JAVA8_STRING_ZERO_COPY_CTR.apply(data, Boolean.TRUE);
      }
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public static String newJava11StringByZeroCopy(byte[] data, byte coder) {
    if (Platform.JAVA_VERSION < 9) {
      throw new IllegalStateException(
          String.format("Current java version is %s", Platform.JAVA_VERSION));
    }
    try {
      if (coder == LATIN1) {
        if (JAVA11_ASCII_STRING_ZERO_COPY_CTR == null) {
          String str = Platform.newInstance(String.class);
          // if --illegal-access=deny, this wont' take effect, the reset will be empty.
          Platform.putObject(str, STRING_VALUE_FIELD_OFFSET, data);
          Platform.putObject(str, STRING_CODER_FIELD_OFFSET, coder);
          return str;
        } else {
          // 700% faster than unsafe put field in java11, only 10% slower than `new String(str)` for
          // string length 230.
          // 50% faster than unsafe put field in java11 for string length 10.
          return JAVA11_ASCII_STRING_ZERO_COPY_CTR.apply(data);
        }
      } else {
        if (JAVA11_STRING_ZERO_COPY_CTR == null) {
          String str = Platform.newInstance(String.class);
          // if --illegal-access=deny, this won't take effect, the reset will be empty.
          Platform.putObject(str, STRING_VALUE_FIELD_OFFSET, data);
          Platform.putObject(str, STRING_CODER_FIELD_OFFSET, coder);
          return str;
        } else {
          // 700% faster than unsafe put field in java11, only 10% slower than `new String(str)` for
          // string length 230.
          // 50% faster than unsafe put field in java11 for string length 10.
          return (String) JAVA11_STRING_ZERO_COPY_CTR.invokeExact(data, coder);
        }
      }
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  private static final BiFunction<char[], Boolean, String> JAVA8_STRING_ZERO_COPY_CTR =
      getJava8StringZeroCopyCtr();
  private static final MethodHandle JAVA11_STRING_ZERO_COPY_CTR = getJava11StringZeroCopyCtr();
  private static final Function<byte[], String> JAVA11_ASCII_STRING_ZERO_COPY_CTR =
      getJava11AsciiStringZeroCopyCtr();

  private static BiFunction<char[], Boolean, String> getJava8StringZeroCopyCtr() {
    MethodHandles.Lookup lookup = getLookupByReflection();
    if (lookup == null) {
      return null;
    }
    MethodHandle handle = getJavaStringZeroCopyCtrHandle(lookup);
    if (handle == null) {
      return null;
    }
    try {
      // Faster than handle.invokeExact(data, boolean)
      CallSite callSite =
          LambdaMetafactory.metafactory(
              lookup,
              "apply",
              MethodType.methodType(BiFunction.class),
              handle.type().generic(),
              handle,
              handle.type());
      return (BiFunction) callSite.getTarget().invokeExact();
    } catch (Throwable e) {
      return null;
    }
  }

  private static MethodHandle getJava11StringZeroCopyCtr() {
    MethodHandles.Lookup lookup = getLookupByReflection();
    if (lookup == null) {
      return null;
    }
    return getJavaStringZeroCopyCtrHandle(lookup);
  }

  private static Function<byte[], String> getJava11AsciiStringZeroCopyCtr() {
    MethodHandles.Lookup lookup = getLookupByReflection();
    if (lookup == null) {
      return null;
    }
    // Can't create callSite like java8, will get error:
    //   java.lang.invoke.LambdaConversionException: Type mismatch for instantiated parameter 1:
    //   byte is not a subtype of class java.lang.Object
    try {
      Class clazz = Class.forName("java.lang.StringCoding");
      MethodHandles.Lookup caller = lookup.in(clazz);
      MethodHandle handle =
          caller.findStatic(
              clazz, "newStringLatin1", MethodType.methodType(String.class, byte[].class));
      // Faster than handle.invokeExact(data, byte)
      CallSite callSite =
          LambdaMetafactory.metafactory(
              caller,
              "apply",
              MethodType.methodType(Function.class),
              handle.type().generic(),
              handle,
              handle.type());
      return (Function<byte[], String>) callSite.getTarget().invokeExact();
    } catch (Throwable e) {
      return null;
    }
  }

  private static MethodHandle getJavaStringZeroCopyCtrHandle(MethodHandles.Lookup lookup) {
    Preconditions.checkArgument(Platform.JAVA_VERSION >= 8);
    if (Platform.JAVA_VERSION > 16) {
      return null;
    }
    try {
      if (Platform.JAVA_VERSION == 8) {
        return lookup.findConstructor(
            String.class, MethodType.methodType(void.class, char[].class, boolean.class));
      } else {
        return lookup.findConstructor(
            String.class, MethodType.methodType(void.class, byte[].class, byte.class));
      }
    } catch (Exception e) {
      return null;
    }
  }

  private static MethodHandles.Lookup getLookup() throws Exception {
    // This can supress illegal-access and work even --illegal-access=deny for jdk16-.
    // For JDK16+, this will fail at `lookupClass` field not found.
    // This will produce unknown behaviour on some version of lombok.
    MethodHandles.Lookup lookup = ReflectionUtils.unsafeCopy(MethodHandles.lookup());
    long lookupClassOffset =
        ReflectionUtils.getFieldOffset(MethodHandles.Lookup.class.getDeclaredField("lookupClass"));
    long allowedModesOffset =
        ReflectionUtils.getFieldOffset(MethodHandles.Lookup.class.getDeclaredField("allowedModes"));
    Platform.putObject(lookup, lookupClassOffset, String.class);
    Platform.putObject(lookup, allowedModesOffset, -1);
    return lookup;
  }

  private static MethodHandles.Lookup getLookupByReflection() {
    try {
      Constructor<MethodHandles.Lookup> constructor =
          MethodHandles.Lookup.class.getDeclaredConstructor(Class.class, int.class);
      constructor.setAccessible(true);
      return constructor.newInstance(
          String.class, -1 // Lookup.TRUSTED
          );
    } catch (Exception e) {
      return null;
    }
  }

  public void writeUTF8String(MemoryBuffer buffer, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    buffer.writePositiveVarInt(bytes.length);
    buffer.writeBytes(bytes);
  }

  public String readUTF8String(MemoryBuffer buffer) {
    int len = buffer.readPositiveVarInt();
    byte[] bytes = buffer.readBytes(len);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
