package org.apache.fury.reflect;

import lombok.AllArgsConstructor;
import org.apache.fury.reflect.FieldAccessor.GeneratedAccessor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldAccessorTest {
  @AllArgsConstructor
  private static final class TestStruct {
    private int f1;
    private boolean f2;
    private String f3;
  }

  @Test
  public void testGeneratedAccessor() throws Exception {
    TestStruct struct = new TestStruct(10, true, "str");
    GeneratedAccessor f1 = new GeneratedAccessor(TestStruct.class.getDeclaredField("f1"));
    Assert.assertEquals(f1.get(struct), 10);
    f1.set(struct, 20);
    Assert.assertEquals(f1.get(struct), 20);
    GeneratedAccessor f2 = new GeneratedAccessor(TestStruct.class.getDeclaredField("f2"));
    Assert.assertEquals(f2.get(struct), true);
    f2.set(struct, false);
    Assert.assertEquals(f2.get(struct), false);
    GeneratedAccessor f3 = new GeneratedAccessor(TestStruct.class.getDeclaredField("f3"));
    Assert.assertEquals(f3.get(struct), "str");
    f3.set(struct, "a");
    Assert.assertEquals(f3.get(struct), "a");
  }
}
