/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.benchmark.data;

import java.io.Serializable;
import java.util.Arrays;

public class Sample implements Serializable {
  public int intValue;
  public long longValue;
  public float floatValue;
  public double doubleValue;
  public short shortValue;
  public char charValue;
  public boolean booleanValue;

  public Integer intValueBoxed;
  public Long longValueBoxed;
  public Float floatValueBoxed;
  public Double doubleValueBoxed;
  public Short shortValueBoxed;
  public Character charValueBoxed;
  public Boolean booleanValueBoxed;

  public int[] intArray;
  public long[] longArray;
  public float[] floatArray;
  public double[] doubleArray;
  public short[] shortArray;
  public char[] charArray;
  public boolean[] booleanArray;

  public String string; // Can be null.
  public Sample sample; // Can be null.

  public Sample() {}

  public static void main(String[] args) {
    Object obj = new MediaContent().populate(false);
    System.out.println(obj);
    System.out.println(obj.equals(new MediaContent().populate(false)));
  }

  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((booleanValueBoxed == null) ? 0 : booleanValueBoxed.hashCode());
    result = prime * result + ((charValueBoxed == null) ? 0 : charValueBoxed.hashCode());
    result = prime * result + ((doubleValueBoxed == null) ? 0 : doubleValueBoxed.hashCode());
    result = prime * result + ((floatValueBoxed == null) ? 0 : floatValueBoxed.hashCode());
    result = prime * result + ((intValueBoxed == null) ? 0 : intValueBoxed.hashCode());
    result = prime * result + ((longValueBoxed == null) ? 0 : longValueBoxed.hashCode());
    result = prime * result + ((shortValueBoxed == null) ? 0 : shortValueBoxed.hashCode());
    result = prime * result + Arrays.hashCode(booleanArray);
    result = prime * result + (booleanValue ? 1231 : 1237);
    result = prime * result + Arrays.hashCode(charArray);
    result = prime * result + charValue;
    result = prime * result + Arrays.hashCode(doubleArray);
    long temp;
    temp = Double.doubleToLongBits(doubleValue);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + Arrays.hashCode(floatArray);
    result = prime * result + Float.floatToIntBits(floatValue);
    result = prime * result + Arrays.hashCode(intArray);
    result = prime * result + intValue;
    result = prime * result + Arrays.hashCode(longArray);
    result = prime * result + (int) (longValue ^ (longValue >>> 32));
    result = prime * result + Arrays.hashCode(shortArray);
    result = prime * result + shortValue;
    result = prime * result + ((string == null) ? 0 : string.hashCode());
    return result;
  }

  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null) {
      return false;
    }
    if (getClass() != object.getClass()) {
      return false;
    }
    Sample other = (Sample) object;
    if (booleanValueBoxed == null) {
      if (other.booleanValueBoxed != null) {
        return false;
      }
    } else if (!booleanValueBoxed.equals(other.booleanValueBoxed)) {
      return false;
    }
    if (charValueBoxed == null) {
      if (other.charValueBoxed != null) {
        return false;
      }
    } else if (!charValueBoxed.equals(other.charValueBoxed)) {
      return false;
    }
    if (doubleValueBoxed == null) {
      if (other.doubleValueBoxed != null) {
        return false;
      }
    } else if (!doubleValueBoxed.equals(other.doubleValueBoxed)) {
      return false;
    }
    if (floatValueBoxed == null) {
      if (other.floatValueBoxed != null) {
        return false;
      }
    } else if (!floatValueBoxed.equals(other.floatValueBoxed)) {
      return false;
    }
    if (intValueBoxed == null) {
      if (other.intValueBoxed != null) {
        return false;
      }
    } else if (!intValueBoxed.equals(other.intValueBoxed)) {
      return false;
    }
    if (longValueBoxed == null) {
      if (other.longValueBoxed != null) {
        return false;
      }
    } else if (!longValueBoxed.equals(other.longValueBoxed)) {
      return false;
    }
    if (shortValueBoxed == null) {
      if (other.shortValueBoxed != null) {
        return false;
      }
    } else if (!shortValueBoxed.equals(other.shortValueBoxed)) {
      return false;
    }
    if (!Arrays.equals(booleanArray, other.booleanArray)) {
      return false;
    }
    if (booleanValue != other.booleanValue) {
      return false;
    }
    if (!Arrays.equals(charArray, other.charArray)) {
      return false;
    }
    if (charValue != other.charValue) {
      return false;
    }
    if (!Arrays.equals(doubleArray, other.doubleArray)) {
      return false;
    }
    if (Double.doubleToLongBits(doubleValue) != Double.doubleToLongBits(other.doubleValue)) {
      return false;
    }
    if (!Arrays.equals(floatArray, other.floatArray)) {
      return false;
    }
    if (Float.floatToIntBits(floatValue) != Float.floatToIntBits(other.floatValue)) {
      return false;
    }
    if (!Arrays.equals(intArray, other.intArray)) {
      return false;
    }
    if (intValue != other.intValue) {
      return false;
    }
    if (!Arrays.equals(longArray, other.longArray)) {
      return false;
    }
    if (longValue != other.longValue) {
      return false;
    }
    if (!Arrays.equals(shortArray, other.shortArray)) {
      return false;
    }
    if (shortValue != other.shortValue) {
      return false;
    }
    if (string == null) {
      if (other.string != null) {
        return false;
      }
    } else if (!string.equals(other.string)) {
      return false;
    }
    return true;
  }

  public Sample populate(boolean circularReference) {
    intValue = 123;
    longValue = 1230000;
    floatValue = 12.345f;
    doubleValue = 1.234567;
    shortValue = 12345;
    charValue = '!';
    booleanValue = true;

    intValueBoxed = 321;
    longValueBoxed = 3210000L;
    floatValueBoxed = 54.321f;
    doubleValueBoxed = 7.654321;
    shortValueBoxed = 32100;
    charValueBoxed = '$';
    booleanValueBoxed = Boolean.FALSE;

    intArray = new int[] {-1234, -123, -12, -1, 0, 1, 12, 123, 1234};
    longArray = new long[] {-123400, -12300, -1200, -100, 0, 100, 1200, 12300, 123400};
    floatArray = new float[] {-12.34f, -12.3f, -12, -1, 0, 1, 12, 12.3f, 12.34f};
    doubleArray = new double[] {-1.234, -1.23, -12, -1, 0, 1, 12, 1.23, 1.234};
    shortArray = new short[] {-1234, -123, -12, -1, 0, 1, 12, 123, 1234};
    charArray = "asdfASDF".toCharArray();
    booleanArray = new boolean[] {true, false, false, true};

    string = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    if (circularReference) {
      sample = this;
    }
    return this;
  }
}
