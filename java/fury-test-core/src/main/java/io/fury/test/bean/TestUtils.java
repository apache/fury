package io.fury.test.bean;

import java.util.Random;

public class TestUtils {
  public static String random(int size, int rand) {
    return random(size, new Random(rand));
  }

  public static String random(int size, Random random) {
    char[] chars = new char[size];
    char start = ' ';
    char end = 'z' + 1;
    int gap = end - start;
    for (int i = 0; i < size; i++) {
      chars[i] = (char) (start + random.nextInt(gap));
    }
    return new String(chars);
  }
}
