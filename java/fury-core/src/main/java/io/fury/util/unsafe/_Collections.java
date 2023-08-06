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

package io.fury.util.unsafe;

import io.fury.util.Platform;
import io.fury.util.Utils;
import java.lang.reflect.Field;
import java.util.ArrayList;

/**
 * Unsafe collection utils.
 *
 * @author chaokunyang
 */
// CHECKSTYLE.OFF:TypeName
public class _Collections {
  // CHECKSTYLE.ON:TypeName
  private static final long ARRAY_LIST_SIZE_OFFSET;
  private static final long ARRAY_LIST_ARRAY_OFFSET;

  static {
    Field arrayListSizeField = null;
    Field arrayListArrayField = null;
    try {
      arrayListSizeField = ArrayList.class.getDeclaredField("size");
      arrayListArrayField = ArrayList.class.getDeclaredField("elementData");
    } catch (NoSuchFieldException e) {
      Utils.ignore(e);
    }
    if (arrayListSizeField != null && arrayListArrayField != null) {
      ARRAY_LIST_SIZE_OFFSET = Platform.objectFieldOffset(arrayListSizeField);
      ARRAY_LIST_ARRAY_OFFSET = Platform.objectFieldOffset(arrayListArrayField);
    } else {
      ARRAY_LIST_SIZE_OFFSET = -1;
      ARRAY_LIST_ARRAY_OFFSET = -1;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static void setArrayListElements(ArrayList list, Object[] elements) {
    if (ARRAY_LIST_SIZE_OFFSET != -1) {
      Platform.putInt(list, ARRAY_LIST_SIZE_OFFSET, elements.length);
      Platform.putObject(list, ARRAY_LIST_ARRAY_OFFSET, elements);
    } else {
      for (Object element : elements) {
        list.add(element);
      }
    }
  }
}
