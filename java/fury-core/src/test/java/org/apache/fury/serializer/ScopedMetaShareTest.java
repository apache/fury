package org.apache.fury.serializer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.test.bean.Foo;
import org.testng.Assert;
import org.testng.annotations.Test;

// Most scoped meta share tests are located in CompatibleTest module.
public class ScopedMetaShareTest extends FuryTestBase {

  // Test registration doesn't skip write class meta
  @Test
  public void testRegister() throws Exception {
    Supplier<FuryBuilder> builder =
        () ->
            builder()
                .withCodegen(true)
                .withCompatibleMode(CompatibleMode.COMPATIBLE)
                .withScopedMetaShare(true);
    Object foo = Foo.create();
    Class<?> fooClass = Foo.createCompatibleClass1();
    Object newFoo = fooClass.newInstance();
    ReflectionUtils.unsafeCopy(foo, newFoo);
    Fury fury = builder.get().build();
    fury.register(Foo.class);
    Fury newFury = builder.get().withClassLoader(fooClass.getClassLoader()).build();
    newFury.register(fooClass);
    {
      byte[] foo1Bytes = newFury.serialize(newFoo);
      Object deserialized = fury.deserialize(foo1Bytes);
      Assert.assertEquals(deserialized.getClass(), Foo.class);
      Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newFoo));
      byte[] fooBytes = fury.serialize(deserialized);
      Assert.assertTrue(ReflectionUtils.objectFieldsEquals(newFury.deserialize(fooBytes), newFoo));
    }
    {
      byte[] bytes1 = fury.serialize(foo);
      Object o1 = newFury.deserialize(bytes1);
      Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(o1, foo));
      Object o2 = fury.deserialize(newFury.serialize(o1));
      List<String> fields =
          Arrays.stream(fooClass.getDeclaredFields())
              .map(f -> f.getDeclaringClass().getSimpleName() + f.getName())
              .collect(Collectors.toList());
      Assert.assertTrue(ReflectionUtils.objectFieldsEquals(new HashSet<>(fields), o2, foo));
    }
    {
      Object o3 = fury.deserialize(newFury.serialize(foo));
      Assert.assertTrue(ReflectionUtils.objectFieldsEquals(o3, foo));
    }
  }
}
