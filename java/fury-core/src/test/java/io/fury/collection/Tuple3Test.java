package io.fury.collection;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class Tuple3Test {

  @Test
  public void testEquals() {
    assertEquals(Tuple3.of(1, "a", 1.1), Tuple3.of(1, "a", 1.1));
    assertEquals(Tuple3.of(1, "a", 1.1).hashCode(), Tuple3.of(1, "a", 1.1).hashCode());
  }
}