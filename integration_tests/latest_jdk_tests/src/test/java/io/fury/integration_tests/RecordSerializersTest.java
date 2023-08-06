package io.fury.integration_tests;

import io.fury.util.RecordComponent;
import io.fury.util.RecordUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

public class RecordSerializersTest {

  public record Foo(int f1, String f2, List<String> f3) {
  }

  @Test
  public void testIsRecord() {
    Assert.assertTrue(RecordUtils.isRecord(Foo.class));
  }

  @Test
  public void testGetRecordComponents() {
    RecordComponent[] recordComponents = RecordUtils.getRecordComponents(Foo.class);
    Assert.assertNotNull(recordComponents);
    java.lang.reflect.RecordComponent[] expectComponents = Foo.class.getRecordComponents();
    Assert.assertEquals(recordComponents.length, expectComponents.length);
    Assert.assertEquals(recordComponents[0].getDeclaringRecord(), expectComponents[0].getDeclaringRecord());
    Assert.assertEquals(recordComponents[0].getType(), expectComponents[0].getType());
    Assert.assertEquals(recordComponents[0].getName(), expectComponents[0].getName());
  }

  @Test
  public void testGetRecordGenerics() {
    RecordComponent[] recordComponents = RecordUtils.getRecordComponents(Foo.class);
    Assert.assertNotNull(recordComponents);
    Type genericType = recordComponents[2].getGenericType();
    ParameterizedType parameterizedType = (ParameterizedType) genericType;
    Assert.assertEquals(parameterizedType.getActualTypeArguments()[0], String.class);
  }
}
