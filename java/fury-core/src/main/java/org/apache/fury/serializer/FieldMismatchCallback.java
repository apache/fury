package org.apache.fury.serializer;

import java.lang.reflect.Field;

/** Callback interface for handling field mismatch during deserialization. */
public interface FieldMismatchCallback {

  /**
   * Called when a field mismatch is detected during deserialization.
   *
   * @param modifiedClass The class that is being deserialized
   * @param deserializedTypeName The name of the type that was deserialized
   * @param deserializedFieldName The name of the field that was deserialized
   * @return A FieldAdjustment that contains the target Field and a method to map the deserialized
   *     value to the target field.
   */
  FieldAdjustment onMismatch(
      Class<?> modifiedClass, String deserializedTypeName, String deserializedFieldName);

  abstract class FieldAdjustment {
    private final Field targetField;

    public FieldAdjustment(Field targetField) {
      this.targetField = targetField;
    }

    public Field getTargetField() {
      return targetField;
    }

    public abstract Object adjustValue(Object deserializedValue);
  }
}
