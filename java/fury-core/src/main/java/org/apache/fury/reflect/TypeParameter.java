package org.apache.fury.reflect;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Objects;

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
