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

package org.apache.fory.serializer;

import java.nio.ByteOrder;

/** UTF16 utils. */
class StringUTF16 {
  static final int HI_BYTE_SHIFT;
  static final int LO_BYTE_SHIFT;
  static final boolean IS_BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

  static {
    if (IS_BIG_ENDIAN) {
      HI_BYTE_SHIFT = 8;
      LO_BYTE_SHIFT = 0;
    } else {
      HI_BYTE_SHIFT = 0;
      LO_BYTE_SHIFT = 8;
    }
  }

  // Won't be faster than `putChars`
  // static void putCharsSplit(char[] src, final int charLen, byte[] target, int targetOffset) {
  //   for (int i = 0; i < charLen; i++) {
  //     target[targetOffset+ i<<1] = (byte) (src[i] >> HI_BYTE_SHIFT);
  //   }
  //   for (int i = 0; i < charLen; i++) {
  //     target[targetOffset + i<<1 + 1] = (byte) (src[i] >> LO_BYTE_SHIFT);
  //   }
  // }

  // static void putChars(char[] str, int off, byte[] val, int index, int end) {
  //   while (off < end) {
  //     putChar(val, index++, str[off++]);
  //   }
  // }

  // static void putChar(byte[] val, int index, int c) {
  //   assert index >= 0 && index < length(val) : "Trusted caller missed bounds check";
  //   index <<= 1;
  //   // FIXME JDK11 utf16 string uses little-endian order
  //   val[index++] = (byte) (c >> HI_BYTE_SHIFT);
  //   val[index] = (byte) (c >> LO_BYTE_SHIFT);
  // }

  // static int length(byte[] value) {
  //   return value.length >> 1;
  // }
}
