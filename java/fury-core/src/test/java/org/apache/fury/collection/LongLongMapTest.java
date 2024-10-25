package org.apache.fury.collection;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class LongLongMapTest {

  @Test
  public void testPut() {
    LongLongMap<String> map = new LongLongMap<>(10, 0.5f);
    map.put(1,1, "a");
    map.put(1,2, "b");
    map.put(1,3, "c");
    map.put(2,1, "d");
    map.put(3,1, "f");
    Assert.assertEquals(map.get(1, 1), "a");
    Assert.assertEquals(map.get(1, 2), "b");
    Assert.assertEquals(map.get(1, 3), "c");
    Assert.assertEquals(map.get(2, 1), "d");
    Assert.assertEquals(map.get(3, 1), "f");
    for (int i = 1; i < 100; i++) {
      map.put(i, i, "a" + i);
      Assert.assertEquals(map.get(i, i), "a" + i);
    }
  }
}
