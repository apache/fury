package io.fury.integration_tests;

import io.fury.Fury;
import org.testng.Assert;

@SuppressWarnings("unchecked")
public class TestUtils {
  public static <T> T serDe(Fury fury, T obj) {
    byte[] bytes = fury.serialize(obj);
    return (T) (fury.deserialize(bytes));
  }

  public static Object serDeCheck(Fury fury, Object obj) {
    Object o = serDe(fury, obj);
    Assert.assertEquals(o, obj);
    return o;
  }
}
