package org.apache.fury.memory;

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

import org.apache.fury.util.Platform;

public class LittleEndian {
  public static int putVarUint36Small(byte[] arr, int index, long v) {
    if (v >>> 7 == 0) {
      arr[index] = (byte) v;
      return 1;
    }
    if (v >>> 14 == 0) {
      arr[index++] = (byte) ((v & 0x7F) | 0x80);
      arr[index] = (byte) (v >>> 7);
      return 2;
    }
    return bigWriteUint36(arr, index, v);
  }

  private static int bigWriteUint36(byte[] arr, int index, long v) {
    if (v >>> 21 == 0) {
      arr[index++] = (byte) ((v & 0x7F) | 0x80);
      arr[index++] = (byte) (v >>> 7 | 0x80);
      arr[index] = (byte) (v >>> 14);
      return 3;
    }
    if (v >>> 28 == 0) {
      arr[index++] = (byte) ((v & 0x7F) | 0x80);
      arr[index++] = (byte) (v >>> 7 | 0x80);
      arr[index++] = (byte) (v >>> 14 | 0x80);
      arr[index] = (byte) (v >>> 21);
      return 4;
    }
    arr[index++] = (byte) ((v & 0x7F) | 0x80);
    arr[index++] = (byte) (v >>> 7 | 0x80);
    arr[index++] = (byte) (v >>> 14 | 0x80);
    arr[index++] = (byte) (v >>> 21 | 0x80);
    arr[index] = (byte) (v >>> 28);
    return 5;
  }

  public static void putInt32(Object o, long pos, int value) {
    if (!Platform.IS_LITTLE_ENDIAN) {
      value = Integer.reverseBytes(value);
    }
    Platform.putInt(o, pos, value);
  }

  public static int getInt32(Object o, long pos) {
    int i = Platform.getInt(o, pos);
    return Platform.IS_LITTLE_ENDIAN ? i : Integer.reverseBytes(i);
  }

  public static long getInt64(Object o, long pos) {
    long v = Platform.getLong(o, pos);
    return Platform.IS_LITTLE_ENDIAN ? v : Long.reverseBytes(v);
  }

  public static void putFloat32(Object o, long pos, float value) {
    int v = Float.floatToRawIntBits(value);
    if (!Platform.IS_LITTLE_ENDIAN) {
      v = Integer.reverseBytes(v);
    }
    Platform.putInt(o, pos, v);
  }

  public static void putFloat64(Object o, long pos, double value) {
    long v = Double.doubleToRawLongBits(value);
    if (!Platform.IS_LITTLE_ENDIAN) {
      v = Long.reverseBytes(v);
    }
    Platform.putLong(o, pos, v);
  }
}
