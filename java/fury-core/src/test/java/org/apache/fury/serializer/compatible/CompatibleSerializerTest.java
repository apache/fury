package org.apache.fury.serializer.compatible;

import org.apache.fury.Fury;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
import org.apache.fury.serializer.compatible.classes.ClassCompleteField;
import org.apache.fury.serializer.compatible.classes.ClassMissingField;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test COMPATIBILITY mode that supports - same field type and name can be deserialized to other
 * class with different name - scrambled field order to make sure it could handle different field
 * order - missing or extra field from source class to target class - generic class
 */
public class CompatibleSerializerTest extends Assert {

  Fury getFury(Class<?>... classes) {
    Fury instance =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withMetaShare(true)
            .withScopedMetaShare(true)
            .requireClassRegistration(false)
            .withAsyncCompilation(true)
            .serializeEnumByName(true)
            .build();
    if (classes != null) {
      for (Class<?> clazz : classes) {
        instance.register(clazz);
      }
    }
    ;
    return instance;
  }

  @Test
  void testTargetHasLessFieldComparedToSourceClass() throws InterruptedException {

    ClassCompleteField<String> subclass = new ClassCompleteField<>("subclass", "subclass2");
    ClassCompleteField<ClassCompleteField<String>> classCompleteField =
        new ClassCompleteField<>(subclass, subclass);
    byte[] serialized = getFury(ClassCompleteField.class).serializeJavaObject(classCompleteField);
    ClassMissingField<ClassMissingField<String>> classMissingField =
        getFury(ClassMissingField.class).deserializeJavaObject(serialized, ClassMissingField.class);

    assertEq(classCompleteField, classMissingField);
  }

  @Test
  void testTargetHasMoreFieldComparedToSourceClass() throws InterruptedException {

    ClassMissingField<String> subclass = new ClassMissingField<>("subclass");
    ClassMissingField classMissingField = new ClassMissingField(subclass);
    byte[] serialized = getFury(ClassMissingField.class).serializeJavaObject(classMissingField);

    ClassCompleteField classCompleteField =
        getFury(ClassCompleteField.class)
            .deserializeJavaObject(serialized, ClassCompleteField.class);

    assertEq(classCompleteField, classMissingField);
  }

  void assertEq(ClassCompleteField classCompleteField, ClassMissingField classMissingField) {
    assertEqSubClass(
        (ClassCompleteField) classCompleteField.getPrivateFieldSubClass(),
        (ClassMissingField) classMissingField.getPrivateFieldSubClass());
    assertEquals(classCompleteField.getPrivateMap(), classMissingField.getPrivateMap());
    assertEquals(classCompleteField.getPrivateList(), classMissingField.getPrivateList());
    assertEquals(classCompleteField.getPrivateString(), classMissingField.getPrivateString());
    assertEquals(classCompleteField.getPrivateInt(), classMissingField.getPrivateInt());
  }

  void assertEqSubClass(
      ClassCompleteField classCompleteField, ClassMissingField classMissingField) {
    assertEquals(
        classCompleteField.getPrivateFieldSubClass(), classMissingField.getPrivateFieldSubClass());
    assertEquals(classCompleteField.getPrivateMap(), classMissingField.getPrivateMap());
    assertEquals(classCompleteField.getPrivateList(), classMissingField.getPrivateList());
    assertEquals(classCompleteField.getPrivateString(), classMissingField.getPrivateString());
    assertEquals(classCompleteField.getPrivateInt(), classMissingField.getPrivateInt());
  }
}
