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

package org.apache.fury.serializer;

import static org.apache.fury.type.TypeUtils.PRIMITIVE_CHAR_ARRAY_TYPE;
import static org.apache.fury.type.TypeUtils.STRING_TYPE;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.annotation.CodegenInvoke;
import org.apache.fury.codegen.Expression;
import org.apache.fury.codegen.Expression.Invoke;
import org.apache.fury.codegen.Expression.StaticInvoke;
import org.apache.fury.memory.LittleEndian;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.type.Type;
import org.apache.fury.util.MathUtils;
import org.apache.fury.util.Platform;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;
import org.apache.fury.util.unsafe._JDKAccess;

/**
 * String serializer based on {@link sun.misc.Unsafe} and {@link MethodHandle} for speed.
 *
 * <p>Note that string operations is very common in serialization, and jvm inline and branch
 * elimination is not reliable even in c2 compiler, so we try to inline and avoid checks as we can
 * manually.
 */
@SuppressWarnings("unchecked")
public final class StringSerializer extends Serializer<String> {
  private static final boolean STRING_VALUE_FIELD_IS_CHARS;
  private static final boolean STRING_VALUE_FIELD_IS_BYTES;

  private static final byte LATIN1 = 0;
  private static final Byte LATIN1_BOXED = LATIN1;
  private static final byte UTF16 = 1;
  private static final Byte UTF16_BOXED = UTF16;
  private static final byte UTF8 = 2;
  private static final int DEFAULT_BUFFER_SIZE = 1024;
  // A long mask used to clear all-higher bits of char in a super-word way.
  private static final long MULTI_CHARS_NON_LATIN_MASK;

  // Make offset compatible with graalvm native image.
  private static final long STRING_VALUE_FIELD_OFFSET;

  private static class Offset {
    // Make offset compatible with graalvm native image.
    private static final long STRING_CODER_FIELD_OFFSET;

    static {
      try {
        STRING_CODER_FIELD_OFFSET =
            Platform.objectFieldOffset(String.class.getDeclaredField("coder"));
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static {
    Field valueField = ReflectionUtils.getFieldNullable(String.class, "value");
    // Java8 string
    STRING_VALUE_FIELD_IS_CHARS = valueField != null && valueField.getType() == char[].class;
    // Java11 string
    STRING_VALUE_FIELD_IS_BYTES = valueField != null && valueField.getType() == byte[].class;
    try {
      // Make offset compatible with graalvm native image.
      STRING_VALUE_FIELD_OFFSET =
          Platform.objectFieldOffset(String.class.getDeclaredField("value"));
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    // String length field for android.
    Preconditions.checkArgument(
        ReflectionUtils.getFieldNullable(String.class, "count") == null,
        "Current jdk not supported");
    Preconditions.checkArgument(
        ReflectionUtils.getFieldNullable(String.class, "offset") == null,
        "Current jdk not supported");
    if (Platform.IS_LITTLE_ENDIAN) {
      // latin chars will be 0xXX,0x00;0xXX,0x00 in byte order;
      // Using 0x00,0xff(0xff00) to clear latin bits.
      MULTI_CHARS_NON_LATIN_MASK = 0xff00ff00ff00ff00L;
    } else {
      // latin chars will be 0x00,0xXX;0x00,0xXX in byte order;
      // Using 0x00,0xff(0x00ff) to clear latin bits.
      MULTI_CHARS_NON_LATIN_MASK = 0x00ff00ff00ff00ffL;
    }
  }

  private final boolean compressString;
  private byte[] byteArray = new byte[DEFAULT_BUFFER_SIZE];
  private int smoothByteArrayLength = DEFAULT_BUFFER_SIZE;

  public StringSerializer(Fury fury) {
    super(fury, String.class, fury.trackingRef() && !fury.isStringRefIgnored());
    compressString = fury.compressString();
  }

  @Override
  public short getXtypeId() {
    return Type.STRING.getId();
  }

  @Override
  public void write(MemoryBuffer buffer, String value) {
    writeJavaString(buffer, value);
  }

  @Override
  public void xwrite(MemoryBuffer buffer, String value) {
    writeUTF8String(buffer, value);
  }

  @Override
  public String read(MemoryBuffer buffer) {
    return readJavaString(buffer);
  }

  @Override
  public String xread(MemoryBuffer buffer) {
    return readUTF8String(buffer);
  }

  public void writeString(MemoryBuffer buffer, String value) {
    if (isJava) {
      writeJavaString(buffer, value);
    } else {
      writeUTF8String(buffer, value);
    }
  }

  public Expression writeStringExpr(Expression strSerializer, Expression buffer, Expression str) {
    if (isJava) {
      if (STRING_VALUE_FIELD_IS_BYTES) {
        return new StaticInvoke(StringSerializer.class, "writeBytesString", buffer, str);
      } else {
        if (!STRING_VALUE_FIELD_IS_CHARS) {
          throw new UnsupportedOperationException();
        }
        if (compressString) {
          return new Invoke(strSerializer, "writeCharsStringCompressed", buffer, str);
        } else {
          return new Invoke(strSerializer, "writeCharsStringUncompressed", buffer, str);
        }
      }
    } else {
      return new Invoke(strSerializer, "writeUTF8String", buffer, str);
    }
  }

  // Invoked by jit
  public void writeCharsStringCompressed(MemoryBuffer buffer, String value) {
    final char[] chars = (char[]) Platform.getObject(value, STRING_VALUE_FIELD_OFFSET);
    if (isLatin(chars)) {
      writeCharsLatin(buffer, chars, chars.length);
    } else {
      writeCharsUTF16(buffer, chars, chars.length);
    }
  }

  // Invoked by jit
  public void writeCharsStringUncompressed(MemoryBuffer buffer, String value) {
    int numBytes = MathUtils.doubleExact(value.length());
    final char[] chars = (char[]) Platform.getObject(value, STRING_VALUE_FIELD_OFFSET);
    buffer.writePrimitiveArrayWithSize(chars, Platform.CHAR_ARRAY_OFFSET, numBytes);
  }

  public String readString(MemoryBuffer buffer) {
    if (isJava) {
      return readJavaString(buffer);
    } else {
      return readUTF8String(buffer);
    }
  }

  public Expression readStringExpr(Expression strSerializer, Expression buffer) {
    if (isJava) {
      if (STRING_VALUE_FIELD_IS_BYTES) {
        return new Invoke(strSerializer, "readBytesString", STRING_TYPE, buffer);
      } else {
        if (!STRING_VALUE_FIELD_IS_CHARS) {
          throw new UnsupportedOperationException();
        }
        if (compressString) {
          return new Invoke(strSerializer, "readCompressedCharsString", STRING_TYPE, buffer);
        } else {
          Expression chars = new Invoke(buffer, "readCharsAndSize", PRIMITIVE_CHAR_ARRAY_TYPE);
          return new StaticInvoke(
              StringSerializer.class, "newCharsStringZeroCopy", STRING_TYPE, chars);
        }
      }
    } else {
      return new Invoke(strSerializer, "readUTF8String", STRING_TYPE, buffer);
    }
  }

  @CodegenInvoke
  public String readBytesString(MemoryBuffer buffer) {
    long header = buffer.readVarUint36Small();
    byte coder = (byte) (header & 0b11);
    int numBytes = (int) (header >>> 2);
    buffer.checkReadableBytes(numBytes);
    byte[] bytes;
    byte[] heapMemory = buffer.getHeapMemory();
    if (heapMemory != null) {
      final int arrIndex = buffer._unsafeHeapReaderIndex();
      buffer._increaseReaderIndexUnsafe(numBytes);
      bytes = new byte[numBytes];
      System.arraycopy(heapMemory, arrIndex, bytes, 0, numBytes);
    } else {
      bytes = buffer.readBytes(numBytes);
    }
    if (coder != UTF8) {
      return newBytesStringZeroCopy(coder, bytes);
    } else {
      return new String(bytes, 0, numBytes, StandardCharsets.UTF_8);
    }
  }

  @CodegenInvoke
  public String readCompressedCharsString(MemoryBuffer buffer) {
    long header = buffer.readVarUint36Small();
    byte coder = (byte) (header & 0b11);
    int numBytes = (int) (header >>> 2);
    if (coder == LATIN1) {
      return newCharsStringZeroCopy(readLatinChars(buffer, numBytes));
    } else if (coder == UTF16) {
      return newCharsStringZeroCopy(readUTF16Chars(buffer, numBytes));
    } else {
      return readUtf8(buffer, coder, numBytes);
    }
  }

  private String readUtf8(MemoryBuffer buffer, byte coder, int numBytes) {
    Preconditions.checkArgument(coder == UTF8, UTF8);
    byte[] bytes = buffer.readBytes(numBytes);
    return new String(bytes, 0, numBytes, StandardCharsets.UTF_8);
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
      writeBytesString(buffer, value);
    } else {
      assert STRING_VALUE_FIELD_IS_CHARS;
      final char[] chars = (char[]) Platform.getObject(value, STRING_VALUE_FIELD_OFFSET);
      if (compressString) {
        if (isLatin(chars)) {
          writeCharsLatin(buffer, chars, chars.length);
        } else {
          writeCharsUTF16(buffer, chars, chars.length);
        }
      } else {
        int numBytes = MathUtils.doubleExact(value.length());
        buffer.writePrimitiveArrayWithSize(chars, Platform.CHAR_ARRAY_OFFSET, numBytes);
      }
    }
  }

  public static boolean isLatin(char[] chars) {
    int numChars = chars.length;
    int vectorizedLen = numChars >> 2;
    int vectorizedChars = vectorizedLen << 2;
    int endOffset = Platform.CHAR_ARRAY_OFFSET + (vectorizedChars << 1);
    boolean isLatin = true;
    for (int offset = Platform.CHAR_ARRAY_OFFSET; offset < endOffset; offset += 8) {
      // check 4 chars in a vectorized way, 4 times faster than scalar check loop.
      // See benchmark in CompressStringSuite.latinSuperWordCheck.
      long multiChars = Platform.getLong(chars, offset);
      if ((multiChars & MULTI_CHARS_NON_LATIN_MASK) != 0) {
        isLatin = false;
        break;
      }
    }
    if (isLatin) {
      for (int i = vectorizedChars; i < numChars; i++) {
        if (chars[i] > 0xFF) {
          isLatin = false;
          break;
        }
      }
    }
    return isLatin;
  }

  // Invoked by fury JIT
  public String readJavaString(MemoryBuffer buffer) {
    if (STRING_VALUE_FIELD_IS_BYTES) {
      return readBytesString(buffer);
    } else {
      assert STRING_VALUE_FIELD_IS_CHARS;
      if (compressString) {
        return readCompressedCharsString(buffer);
      } else {
        return newCharsStringZeroCopy(buffer.readCharsAndSize());
      }
    }
  }

  public static void writeBytesString(MemoryBuffer buffer, String value) {
    byte[] bytes = (byte[]) Platform.getObject(value, STRING_VALUE_FIELD_OFFSET);
    int bytesLen = bytes.length;
    long header =
        ((long) bytesLen << 2) | Platform.getByte(value, Offset.STRING_CODER_FIELD_OFFSET);
    int writerIndex = buffer.writerIndex();
    // The `ensure` ensure next operations are safe without bound checks,
    // and inner heap buffer doesn't change.
    buffer.ensure(writerIndex + 9 + bytesLen); // 1 byte coder + varint max 8 bytes
    final byte[] targetArray = buffer.getHeapMemory();
    if (targetArray != null) {
      // Some JDK11 Unsafe.copyMemory will `copyMemoryChecks`, and
      // jvm doesn't eliminate well in some jdk.
      final int targetIndex = buffer._unsafeHeapWriterIndex();
      int arrIndex = targetIndex;
      arrIndex += LittleEndian.putVarUint36Small(targetArray, arrIndex, header);
      writerIndex += arrIndex - targetIndex;
      System.arraycopy(bytes, 0, targetArray, arrIndex, bytesLen);
    } else {
      writerIndex += buffer._unsafePutVarUint36Small(writerIndex, header);
      long offHeapAddress = buffer.getUnsafeAddress();
      Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, null, offHeapAddress + writerIndex, bytesLen);
    }
    writerIndex += bytesLen;
    buffer._unsafeWriterIndex(writerIndex);
  }

  public void writeCharsLatin(MemoryBuffer buffer, char[] chars, final int strLen) {
    int writerIndex = buffer.writerIndex();
    // The `ensure` ensure next operations are safe without bound checks,
    // and inner heap buffer doesn't change.
    buffer.ensure(writerIndex + 9 + strLen);
    long header = ((long) strLen << 2) | LATIN1;
    final byte[] targetArray = buffer.getHeapMemory();
    if (targetArray != null) {
      int arrIndex = buffer._unsafeHeapWriterIndex();
      int written = LittleEndian.putVarUint36Small(targetArray, arrIndex, header);
      arrIndex += written;
      writerIndex += written + strLen;
      for (int i = 0; i < strLen; i++) {
        targetArray[arrIndex + i] = (byte) chars[i];
      }
      buffer._unsafeWriterIndex(writerIndex);
    } else {
      writerIndex += buffer._unsafePutVarUint36Small(writerIndex, header);
      final byte[] tmpArray = getByteArray(strLen);
      // Write to heap memory then copy is 60% faster than unsafe write to direct memory.
      for (int i = 0; i < strLen; i++) {
        tmpArray[i] = (byte) chars[i];
      }
      buffer.put(writerIndex, tmpArray, 0, strLen);
      writerIndex += strLen;
      buffer._unsafeWriterIndex(writerIndex);
    }
  }

  public void writeCharsUTF16(MemoryBuffer buffer, char[] chars, int strLen) {
    int numBytes = MathUtils.doubleExact(strLen);
    long header = ((long) numBytes << 2) | UTF16;
    // The `ensure` ensure next operations are safe without bound checks,
    // and inner heap buffer doesn't change.
    int writerIndex = buffer.writerIndex();
    buffer.ensure(writerIndex + 9 + numBytes);
    byte[] targetArray = buffer.getHeapMemory();
    if (targetArray != null) {
      int arrIndex = buffer._unsafeHeapWriterIndex();
      int written = LittleEndian.putVarUint36Small(targetArray, arrIndex, header);
      arrIndex += written;
      writerIndex += written + numBytes;
      if (Platform.IS_LITTLE_ENDIAN) {
        // FIXME JDK11 utf16 string uses little-endian order.
        Platform.UNSAFE.copyMemory(
            chars,
            Platform.CHAR_ARRAY_OFFSET,
            targetArray,
            Platform.BYTE_ARRAY_OFFSET + arrIndex,
            numBytes);
      } else {
        heapWriteCharsUTF16BE(chars, arrIndex, numBytes, targetArray);
      }
    } else {
      writerIndex = offHeapWriteCharsUTF16(buffer, chars, writerIndex, header, numBytes);
    }
    buffer._unsafeWriterIndex(writerIndex);
  }

  private static void heapWriteCharsUTF16BE(
      char[] chars, int arrIndex, int numBytes, byte[] targetArray) {
    // Write to heap memory then copy is 250% faster than unsafe write to direct memory.
    int charIndex = 0;
    for (int i = arrIndex, end = i + numBytes; i < end; i += 2) {
      char c = chars[charIndex++];
      targetArray[i] = (byte) (c >> StringUTF16.HI_BYTE_SHIFT);
      targetArray[i + 1] = (byte) (c >> StringUTF16.LO_BYTE_SHIFT);
    }
  }

  private int offHeapWriteCharsUTF16(
      MemoryBuffer buffer, char[] chars, int writerIndex, long header, int numBytes) {
    writerIndex += buffer._unsafePutVarUint36Small(writerIndex, header);
    byte[] tmpArray = getByteArray(numBytes);
    int charIndex = 0;
    for (int i = 0; i < numBytes; i += 2) {
      char c = chars[charIndex++];
      tmpArray[i] = (byte) (c >> StringUTF16.HI_BYTE_SHIFT);
      tmpArray[i + 1] = (byte) (c >> StringUTF16.LO_BYTE_SHIFT);
    }
    buffer.put(writerIndex, tmpArray, 0, numBytes);
    writerIndex += numBytes;
    return writerIndex;
  }

  private char[] readLatinChars(MemoryBuffer buffer, int numBytes) {
    char[] chars = new char[numBytes];
    buffer.checkReadableBytes(numBytes);
    byte[] targetArray = buffer.getHeapMemory();
    if (targetArray != null) {
      int srcIndex = buffer._unsafeHeapReaderIndex();
      for (int i = 0; i < numBytes; i++) {
        chars[i] = (char) (targetArray[srcIndex++] & 0xff);
      }
      buffer._increaseReaderIndexUnsafe(numBytes);
    } else {
      byte[] byteArray = getByteArray(numBytes);
      buffer.readBytes(byteArray, 0, numBytes);
      for (int i = 0; i < numBytes; i++) {
        chars[i] = (char) (byteArray[i] & 0xff);
      }
    }
    return chars;
  }

  private char[] readUTF16Chars(MemoryBuffer buffer, int numBytes) {
    char[] chars = new char[numBytes >> 1];
    if (Platform.IS_LITTLE_ENDIAN) {
      // FIXME JDK11 utf16 string uses little-endian order.
      buffer.readChars(chars, Platform.CHAR_ARRAY_OFFSET, numBytes);
    } else {
      buffer.checkReadableBytes(numBytes);
      final byte[] targetArray = buffer.getHeapMemory();
      if (targetArray != null) {
        int charIndex = 0;
        for (int i = buffer._unsafeHeapReaderIndex(), end = i + numBytes; i < end; i += 2) {
          char c =
              (char)
                  ((targetArray[i] & 0xff << StringUTF16.HI_BYTE_SHIFT)
                      | ((targetArray[i + 1] & 0xff) << StringUTF16.LO_BYTE_SHIFT));
          chars[charIndex++] = c;
        }
        buffer._increaseReaderIndexUnsafe(numBytes);
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

  private static final MethodHandles.Lookup STRING_LOOK_UP =
      _JDKAccess._trustedLookup(String.class);
  private static final BiFunction<char[], Boolean, String> CHARS_STRING_ZERO_COPY_CTR =
      getCharsStringZeroCopyCtr();
  private static final BiFunction<byte[], Byte, String> BYTES_STRING_ZERO_COPY_CTR =
      getBytesStringZeroCopyCtr();
  private static final Function<byte[], String> LATIN_BYTES_STRING_ZERO_COPY_CTR =
      getLatinBytesStringZeroCopyCtr();

  public static String newCharsStringZeroCopy(char[] data) {
    if (!STRING_VALUE_FIELD_IS_CHARS) {
      throw new IllegalStateException("String value isn't char[], current java isn't supported");
    }
    // 25% faster than unsafe put field, only 10% slower than `new String(str)`
    return CHARS_STRING_ZERO_COPY_CTR.apply(data, Boolean.TRUE);
  }

  // coder param first to make inline call args
  // `(buffer.readByte(), buffer.readBytesWithSizeEmbedded())` work.
  public static String newBytesStringZeroCopy(byte coder, byte[] data) {
    if (coder == LATIN1) {
      // 700% faster than unsafe put field in java11, only 10% slower than `new String(str)` for
      // string length 230.
      // 50% faster than unsafe put field in java11 for string length 10.
      if (LATIN_BYTES_STRING_ZERO_COPY_CTR != null) {
        return LATIN_BYTES_STRING_ZERO_COPY_CTR.apply(data);
      } else {
        // JDK17 removed newStringLatin1
        return BYTES_STRING_ZERO_COPY_CTR.apply(data, LATIN1_BOXED);
      }
    } else if (coder == UTF16) {
      // avoid byte box cost.
      return BYTES_STRING_ZERO_COPY_CTR.apply(data, UTF16_BOXED);
    } else {
      // 700% faster than unsafe put field in java11, only 10% slower than `new String(str)` for
      // string length 230.
      // 50% faster than unsafe put field in java11 for string length 10.
      // `invokeExact` must pass exact params with exact types:
      // `(Object) data, coder` will throw WrongMethodTypeException
      return BYTES_STRING_ZERO_COPY_CTR.apply(data, coder);
    }
  }

  private static BiFunction<char[], Boolean, String> getCharsStringZeroCopyCtr() {
    if (!STRING_VALUE_FIELD_IS_CHARS) {
      return null;
    }
    MethodHandle handle = getJavaStringZeroCopyCtrHandle();
    if (handle == null) {
      return null;
    }
    try {
      // Faster than handle.invokeExact(data, boolean)
      CallSite callSite =
          LambdaMetafactory.metafactory(
              STRING_LOOK_UP,
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

  private static BiFunction<byte[], Byte, String> getBytesStringZeroCopyCtr() {
    if (!STRING_VALUE_FIELD_IS_BYTES) {
      return null;
    }
    MethodHandle handle = getJavaStringZeroCopyCtrHandle();
    if (handle == null) {
      return null;
    }
    // Faster than handle.invokeExact(data, byte)
    try {
      MethodType instantiatedMethodType =
          MethodType.methodType(handle.type().returnType(), new Class[] {byte[].class, Byte.class});
      CallSite callSite =
          LambdaMetafactory.metafactory(
              STRING_LOOK_UP,
              "apply",
              MethodType.methodType(BiFunction.class),
              handle.type().generic(),
              handle,
              instantiatedMethodType);
      return (BiFunction) callSite.getTarget().invokeExact();
    } catch (Throwable e) {
      return null;
    }
  }

  private static Function<byte[], String> getLatinBytesStringZeroCopyCtr() {
    if (!STRING_VALUE_FIELD_IS_BYTES) {
      return null;
    }
    if (STRING_LOOK_UP == null) {
      return null;
    }
    try {
      Class<?> clazz = Class.forName("java.lang.StringCoding");
      MethodHandles.Lookup caller = STRING_LOOK_UP.in(clazz);
      // JDK17 removed this method.
      MethodHandle handle =
          caller.findStatic(
              clazz, "newStringLatin1", MethodType.methodType(String.class, byte[].class));
      // Faster than handle.invokeExact(data, byte)
      return _JDKAccess.makeFunction(caller, handle, Function.class);
    } catch (Throwable e) {
      return null;
    }
  }

  private static MethodHandle getJavaStringZeroCopyCtrHandle() {
    Preconditions.checkArgument(Platform.JAVA_VERSION >= 8);
    if (STRING_LOOK_UP == null) {
      return null;
    }
    try {
      if (STRING_VALUE_FIELD_IS_CHARS) {
        return STRING_LOOK_UP.findConstructor(
            String.class, MethodType.methodType(void.class, char[].class, boolean.class));
      } else {
        return STRING_LOOK_UP.findConstructor(
            String.class, MethodType.methodType(void.class, byte[].class, byte.class));
      }
    } catch (Exception e) {
      return null;
    }
  }

  public void writeUTF8String(MemoryBuffer buffer, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    buffer.writeVarUint32(bytes.length);
    buffer.writeBytes(bytes);
  }

  public String readUTF8String(MemoryBuffer buffer) {
    int numBytes = buffer.readVarUint32Small14();
    buffer.checkReadableBytes(numBytes);
    final byte[] targetArray = buffer.getHeapMemory();
    if (targetArray != null) {
      String str =
          new String(
              targetArray, buffer._unsafeHeapReaderIndex(), numBytes, StandardCharsets.UTF_8);
      buffer.increaseReaderIndex(numBytes);
      return str;
    } else {
      final byte[] tmpArray = getByteArray(numBytes);
      buffer.readBytes(tmpArray, 0, numBytes);
      return new String(tmpArray, 0, numBytes, StandardCharsets.UTF_8);
    }
  }
}
