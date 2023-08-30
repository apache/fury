package io.fury.collection;

import static org.testng.Assert.*;

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FuryObjectMapTest {

  @Test
  public void testIterable() {
    FuryObjectMap<String, String> map = new ObjectMap<>(4, 0.2f);
    Map<String, String> hashMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      map.put("k" + i, "v" + i);
      hashMap.put("k" + i, "v" + i);
    }
    Map<String, String> hashMap2 = new HashMap<>();
    for (Map.Entry<String, String> entry : map.iterable()) {
      hashMap2.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(hashMap2, hashMap);
  }

  @Test
  public void testForEach() {
    FuryObjectMap<String, String> map = new ObjectMap<>(4, 0.2f);
    Map<String, String> hashMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      map.put("k" + i, "v" + i);
      hashMap.put("k" + i, "v" + i);
    }
    Map<String, String> hashMap2 = new HashMap<>();
    map.forEach(hashMap2::put);
    Assert.assertEquals(hashMap2, hashMap);
  }
}
