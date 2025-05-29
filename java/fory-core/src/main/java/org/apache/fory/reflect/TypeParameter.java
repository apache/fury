/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fory.reflect;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Objects;

// Derived from Guava 32.1.2 com.google.common.reflect.TypeParameter
// https://github.com/google/guava/blob/9f6a3840/guava/src/com/google/common/reflect/TypeParameter.java
public abstract class TypeParameter<X> {
  final TypeVariable<?> typeVariable;

  public TypeParameter() {
    Type superclass = getClass().getGenericSuperclass();
    if (!(superclass instanceof ParameterizedType)) {
      throw new IllegalArgumentException(superclass + "isn't parameterized");
    }
    this.typeVariable =
        (TypeVariable<?>) ((ParameterizedType) superclass).getActualTypeArguments()[0];
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TypeParameter<?> that = (TypeParameter<?>) o;

    return Objects.equals(typeVariable, that.typeVariable);
  }

  @Override
  public int hashCode() {
    return typeVariable != null ? typeVariable.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "TypeParameter{" + "typeVariable=" + typeVariable + '}';
  }
}
