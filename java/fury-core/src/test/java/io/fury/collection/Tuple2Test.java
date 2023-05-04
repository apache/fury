package io.fury.collection;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class Tuple2Test {

  @Test
  public void testEquals() {
    assertEquals(Tuple2.of(1, "a"), Tuple2.of(1, "a"));
    assertEquals(Tuple2.of(1, "a").hashCode(), Tuple2.of(1, "a").hashCode());
  }
}