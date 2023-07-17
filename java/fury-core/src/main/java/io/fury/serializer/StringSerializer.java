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

import static io.fury.type.TypeUtils.PRIMITIVE_CHAR_ARRAY_TYPE;
import static io.fury.type.TypeUtils.STRING_TYPE;

import com.google.common.base.Preconditions;
import io.fury.Fury;
import io.fury.codegen.Expression;
import io.fury.codegen.Expression.Invoke;
import io.fury.codegen.Expression.StaticInvoke;
import io.fury.memory.MemoryBuffer;
import io.fury.memory.MemoryUtils;
import io.fury.type.Type;
import io.fury.util.MathUtils;
import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;
import io.fury.util.unsafe._JDKAccess;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
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
  private static final Byte LATIN1_BOXED = LATIN1;
  private static final byte UTF16 = 1;
  private static final Byte UTF16_BOXED = UTF16;
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
        return new StaticInvoke(StringSerializer.class, "writeJDK11String", buffer, str);
      } else {
        if (!STRING_VALUE_FIELD_IS_CHARS) {
          throw new UnsupportedOperationException();
        }
        if (compressString) {
          return new Invoke(strSerializer, "writeJava8StringCompressed", buffer, str);
        } else {
          return new Invoke(strSerializer, "writeJava8StringUncompressed", buffer, str);
        }
      }
    } else {
      return new Invoke(strSerializer, "writeUTF8String", buffer, str);
    }
  }

  // Invoked by jit
  public void writeJava8StringCompressed(MemoryBuffer buffer, String value) {
    final char[] chars = (char[]) Platform.getObject(value, STRING_VALUE_FIELD_OFFSET);
    if (isAscii(chars)) {
      writeJDK8Ascii(buffer, chars);
    } else {
      writeJDK8UTF16(buffer, chars);
    }
  }

  // Invoked by jit
  public void writeJava8StringUncompressed(MemoryBuffer buffer, String value) {
    int numBytes = MathUtils.doubleExact(value.length());
    final char[] chars = (char[]) Platform.getObject(value, STRING_VALUE_FIELD_OFFSET);
    buffer.writePrimitiveArrayWithSizeEmbedded(chars, Platform.CHAR_ARRAY_OFFSET, numBytes);
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
        // Expression coder = inlineInvoke(buffer, "readByte", BYTE_TYPE);
        // Expression value = inlineInvoke(buffer, "readBytesWithSizeEmbedded", BINARY_TYPE);
        // return new StaticInvoke(
        //     StringSerializer.class, "newJava11StringByZeroCopy", STRING_TYPE, coder, value);
        return new Invoke(strSerializer, "readJava11String", STRING_TYPE, buffer);
      } else {
        if (!STRING_VALUE_FIELD_IS_CHARS) {
          throw new UnsupportedOperationException();
        }
        if (compressString) {
          return new Invoke(strSerializer, "readJava8CompressedString", STRING_TYPE, buffer);
        } else {
          Expression chars =
              new Invoke(buffer, "readCharsWithSizeEmbedded", PRIMITIVE_CHAR_ARRAY_TYPE);
          return new StaticInvoke(
              StringSerializer.class, "newJava8StringByZeroCopy", STRING_TYPE, chars);
        }
      }
    } else {
      return new Invoke(strSerializer, "readUTF8String", STRING_TYPE, buffer);
    }
  }

  // Invoked by jit.
  public String readJava11String(MemoryBuffer buffer) {
    byte[] heapMemory = buffer.getHeapMemory();
    if (heapMemory != null) {
      final int targetIndex = buffer.unsafeHeapReaderIndex();
      int arrIndex = targetIndex;
      byte coder = heapMemory[arrIndex++];
      // The encoding algorithm are based on kryo UnsafeMemoryOutput.writeVarInt
      // varint are written using little endian byte order.
      // inline the implementation here since java can't return varIntBytes and varint
      // at the same time.
      int b = heapMemory[arrIndex++];
      int numBytes = b & 0x7F;
      if ((b & 0x80) != 0) {
        b = heapMemory[arrIndex++];
        numBytes |= (b & 0x7F) << 7;
        if ((b & 0x80) != 0) {
          b = heapMemory[arrIndex++];
          numBytes |= (b & 0x7F) << 14;
          if ((b & 0x80) != 0) {
            b = heapMemory[arrIndex++];
            numBytes |= (b & 0x7F) << 21;
            if ((b & 0x80) != 0) {
              b = heapMemory[arrIndex];
              numBytes |= (b & 0x7F) << 28;
            }
          }
        }
      }
      final byte[] bytes = new byte[numBytes];
      System.arraycopy(heapMemory, arrIndex, bytes, 0, numBytes);
      buffer.increaseReaderIndexUnsafe(arrIndex - targetIndex + numBytes);
      return newJava11StringByZeroCopy(coder, bytes);
    } else {
      byte coder = buffer.readByte();
      final int numBytes = buffer.readPositiveVarInt();
      byte[] bytes = buffer.readBytes(numBytes);
      return newJava11StringByZeroCopy(coder, bytes);
    }
  }

  // Invoked by jit
  public String readJava8CompressedString(MemoryBuffer buffer) {
    byte coder = buffer.readByte();
    if (coder == LATIN1) {
      return newJava8StringByZeroCopy(readAsciiChars(buffer));
    } else {
      return newJava8StringByZeroCopy(readUTF16Chars(buffer, coder));
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
      return readJava11String(buffer);
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

  public static void writeJDK11String(MemoryBuffer buffer, String value) {
    byte[] bytes = (byte[]) Platform.getObject(value, STRING_VALUE_FIELD_OFFSET);
    byte coder = Platform.getByte(value, STRING_CODER_FIELD_OFFSET);
    int bytesLen = bytes.length;
    int writerIndex = buffer.writerIndex();
    // The `ensure` ensure next operations are safe without bound checks,
    // and inner heap buffer doesn't change.
    buffer.ensure(writerIndex + 9 + bytesLen); // 1 byte coder + varint max 8 bytes
    final byte[] targetArray = buffer.getHeapMemory();
    if (targetArray != null) {
      // Some JDK11 Unsafe.copyMemory will `copyMemoryChecks`, and
      // jvm doesn't eliminate well in some jdk.
      final int targetIndex = buffer.unsafeHeapWriterIndex();
      int arrIndex = targetIndex;
      targetArray[arrIndex++] = coder;
      arrIndex += MemoryUtils.writePositiveVarInt(targetArray, arrIndex, bytesLen);
      writerIndex += arrIndex - targetIndex;
      System.arraycopy(bytes, 0, targetArray, arrIndex, bytesLen);
      writerIndex += bytesLen;
    } else {
      buffer.unsafePut(writerIndex++, coder);
      writerIndex += buffer.unsafePutPositiveVarInt(writerIndex, bytesLen);
      long offHeapAddress = buffer.getUnsafeAddress();
      Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, null, offHeapAddress + writerIndex, bytesLen);
      writerIndex += bytesLen;
    }
    buffer.unsafeWriterIndex(writerIndex);
  }

  public void writeJDK8Ascii(MemoryBuffer buffer, char[] chars) {
    final int strLen = chars.length;
    int writerIndex = buffer.writerIndex();
    // The `ensure` ensure next operations are safe without bound checks,
    // and inner heap buffer doesn't change.
    buffer.ensure(writerIndex + 9 + strLen);
    final byte[] targetArray = buffer.getHeapMemory();
    if (targetArray != null) {
      final int targetIndex = buffer.unsafeHeapWriterIndex();
      int arrIndex = targetIndex;
      targetArray[arrIndex++] = LATIN1;
      arrIndex += MemoryUtils.writePositiveVarInt(targetArray, arrIndex, strLen);
      writerIndex += arrIndex - targetIndex + strLen;
      for (int i = 0; i < strLen; i++) {
        targetArray[arrIndex + i] = (byte) chars[i];
      }
      buffer.unsafeWriterIndex(writerIndex);
    } else {
      buffer.unsafePut(writerIndex++, LATIN1);
      writerIndex += buffer.unsafePutPositiveVarInt(writerIndex, strLen);
      final byte[] tmpArray = getByteArray(strLen);
      // Write to heap memory then copy is 60% faster than unsafe write to direct memory.
      for (int i = 0; i < strLen; i++) {
        tmpArray[i] = (byte) chars[i];
      }
      buffer.put(writerIndex, tmpArray, 0, strLen);
      writerIndex += strLen;
      buffer.unsafeWriterIndex(writerIndex);
    }
  }

  public void writeJDK8UTF16(MemoryBuffer buffer, char[] chars) {
    int strLen = chars.length;
    int numBytes = MathUtils.doubleExact(strLen);
    if (Platform.IS_LITTLE_ENDIAN) {
      buffer.writeByte(UTF16);
      // FIXME JDK11 utf16 string uses little-endian order.
      buffer.writePrimitiveArrayWithSizeEmbedded(chars, Platform.CHAR_ARRAY_OFFSET, numBytes);
    } else {
      // The `ensure` ensure next operations are safe without bound checks,
      // and inner heap buffer doesn't change.
      int writerIndex = buffer.writerIndex();
      buffer.ensure(writerIndex + 9 + numBytes);
      byte[] targetArray = buffer.getHeapMemory();
      if (targetArray != null) {
        final int targetIndex = buffer.unsafeHeapWriterIndex();
        int arrIndex = targetIndex;
        targetArray[arrIndex++] = UTF16;
        arrIndex += MemoryUtils.writePositiveVarInt(targetArray, arrIndex, strLen);
        // Write to heap memory then copy is 250% faster than unsafe write to direct memory.
        int charIndex = 0;
        for (int i = arrIndex, end = i + numBytes; i < end; i += 2) {
          char c = chars[charIndex++];
          targetArray[i] = (byte) (c >> StringUTF16.HI_BYTE_SHIFT);
          targetArray[i + 1] = (byte) (c >> StringUTF16.LO_BYTE_SHIFT);
        }
        writerIndex += arrIndex - targetIndex + numBytes;
      } else {
        buffer.unsafePut(writerIndex++, UTF16);
        writerIndex += buffer.unsafePutPositiveVarInt(writerIndex, numBytes);
        byte[] tmpArray = getByteArray(strLen);
        int charIndex = 0;
        for (int i = 0; i < numBytes; i += 2) {
          char c = chars[charIndex++];
          tmpArray[i] = (byte) (c >> StringUTF16.HI_BYTE_SHIFT);
          tmpArray[i + 1] = (byte) (c >> StringUTF16.LO_BYTE_SHIFT);
        }
        buffer.put(writerIndex, tmpArray, 0, numBytes);
        writerIndex += numBytes;
      }
      buffer.unsafeWriterIndex(writerIndex);
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

  private static final MethodHandles.Lookup STRING_LOOK_UP =
      _JDKAccess._trustedLookup(String.class);
  private static final BiFunction<char[], Boolean, String> JAVA8_STRING_ZERO_COPY_CTR =
      getJava8StringZeroCopyCtr();
  private static final BiFunction<byte[], Byte, String> JAVA11_STRING_ZERO_COPY_CTR =
      getJava11StringZeroCopyCtr();
  private static final Function<byte[], String> JAVA11_ASCII_STRING_ZERO_COPY_CTR =
      getJava11AsciiStringZeroCopyCtr();

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

  // coder param first to make inline call args
  // `(buffer.readByte(), buffer.readBytesWithSizeEmbedded())` work.
  public static String newJava11StringByZeroCopy(byte coder, byte[] data) {
    if (Platform.JAVA_VERSION < 9) {
      throw new IllegalStateException(
          String.format("Current java version is %s", Platform.JAVA_VERSION));
    }
    if (coder == LATIN1) {
      // 700% faster than unsafe put field in java11, only 10% slower than `new String(str)` for
      // string length 230.
      // 50% faster than unsafe put field in java11 for string length 10.
      if (JAVA11_ASCII_STRING_ZERO_COPY_CTR != null) {
        return JAVA11_ASCII_STRING_ZERO_COPY_CTR.apply(data);
      } else {
        // JDK17 removed newStringLatin1
        return JAVA11_STRING_ZERO_COPY_CTR.apply(data, LATIN1_BOXED);
      }
    } else if (coder == UTF16) {
      // avoid byte box cost.
      return JAVA11_STRING_ZERO_COPY_CTR.apply(data, UTF16_BOXED);
    } else {
      // 700% faster than unsafe put field in java11, only 10% slower than `new String(str)` for
      // string length 230.
      // 50% faster than unsafe put field in java11 for string length 10.
      // `invokeExact` must pass exact params with exact types:
      // `(Object) data, coder` will throw WrongMethodTypeException
      return JAVA11_STRING_ZERO_COPY_CTR.apply(data, coder);
    }
  }

  private static BiFunction<char[], Boolean, String> getJava8StringZeroCopyCtr() {
    if (Platform.JAVA_VERSION > 8) {
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

  private static BiFunction<byte[], Byte, String> getJava11StringZeroCopyCtr() {
    if (Platform.JAVA_VERSION < 9) {
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

  private static Function<byte[], String> getJava11AsciiStringZeroCopyCtr() {
    if (Platform.JAVA_VERSION < 9) {
      return null;
    }
    if (STRING_LOOK_UP == null) {
      return null;
    }
    try {
      Class clazz = Class.forName("java.lang.StringCoding");
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
      if (Platform.JAVA_VERSION == 8) {
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
