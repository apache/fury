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

package org.apache.fory.util.unsafe;

import java.util.ArrayList;
import org.apache.fory.memory.Platform;

/** Unsafe collection utils. */
// CHECKSTYLE.OFF:TypeName
public class _Collections {
  // CHECKSTYLE.ON:TypeName

  // Make offset compatible with graalvm native image.
  private static class Offset {
    private static final long ARRAY_LIST_SIZE_OFFSET;
    private static final long ARRAY_LIST_ARRAY_OFFSET;

    static {
      try {
        ARRAY_LIST_SIZE_OFFSET =
            Platform.objectFieldOffset(ArrayList.class.getDeclaredField("size"));
        ARRAY_LIST_ARRAY_OFFSET =
            Platform.objectFieldOffset(ArrayList.class.getDeclaredField("elementData"));
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final boolean FAST_MODE;

  static {
    boolean fastMode;
    try {
      fastMode = Offset.ARRAY_LIST_ARRAY_OFFSET != -1;
    } catch (Throwable e) {
      fastMode = false;
    }
    FAST_MODE = fastMode;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static void setArrayListElements(ArrayList list, Object[] elements) {
    if (FAST_MODE) {
      Platform.putInt(list, Offset.ARRAY_LIST_SIZE_OFFSET, elements.length);
      Platform.putObject(list, Offset.ARRAY_LIST_ARRAY_OFFSET, elements);
    } else {
      for (Object element : elements) {
        list.add(element);
      }
    }
  }
}
