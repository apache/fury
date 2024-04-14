package org.apache.fury.meta;

import org.apache.fury.util.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetaStringTest {

  @Test
  public void testEncodeMetaStringLowerSpecial() {
    // special chars not matter for encodeLowerSpecial
    MetaStringEncoder encoder = new MetaStringEncoder('_', '$');
    byte[] encoded = encoder.encodeLowerSpecial("abc_def");
    Assert.assertEquals(encoded.length, 5);
    MetaStringDecoder decoder = new MetaStringDecoder('_', '$');
    String decoded = decoder.decode(encoded, MetaString.Encoding.LOWER_SPECIAL, 7 * 5);
    Assert.assertEquals(decoded, "abc_def");
    for (int i = 0; i < 128; i++) {
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < i; j++) {
        builder.append((char) ('a' + j % 26));
      }
      String str = builder.toString();
      encoded = encoder.encodeLowerSpecial(str);
      decoded = decoder.decode(encoded, MetaString.Encoding.LOWER_SPECIAL, i);
      Assert.assertEquals(decoded, str);
    }
  }

  @Test
  public void testEncodeMetaStringLowerUpperDigitSpecial() {
    char specialChar1 = '.';
    char specialChar2 = '_';
    MetaStringEncoder encoder = new MetaStringEncoder(specialChar1, specialChar2);
    byte[] encoded = encoder.encodeLowerUpperDigitSpecial("ExampleInput123");
    Assert.assertEquals(encoded.length, 12);
    MetaStringDecoder decoder = new MetaStringDecoder(specialChar1, specialChar2);
    String decoded = decoder.decode(encoded, MetaString.Encoding.LOWER_UPPER_DIGIT_SPECIAL, 15 * 6);
    Assert.assertEquals(decoded, "ExampleInput123");

    for (int i = 0; i < 128; i++) {
      String str = createString(i, specialChar1, specialChar2);
      encoded = encoder.encodeLowerUpperDigitSpecial(str);
      decoded = decoder.decode(encoded, MetaString.Encoding.LOWER_UPPER_DIGIT_SPECIAL, i);
      Assert.assertEquals(decoded, str);
    }
  }

  private static String createString(int i, char specialChar1, char specialChar2) {
    StringBuilder builder = new StringBuilder();
    for (int j = 0; j < i; j++) {
      int n = j % 64;
      char c;
      if (n < 26) {
        c = (char) ('a' + n);
      } else if (n < 52) {
        c = (char) ('A' + n - 26);
      } else if (n < 62) {
        c = (char) ('0' + n - 52);
      } else if (n == 62) {
        c = specialChar1;
      } else {
        c = specialChar2;
      }
      builder.append(c);
    }
    StringUtils.shuffle(builder);
    return builder.toString();
  }

  @Test
  public void testMetaString() {
    char specialChar1 = '.';
    char specialChar2 = '_';
    MetaStringEncoder encoder = new MetaStringEncoder(specialChar1, specialChar2);
    for (int i = 0; i < 128; i++) {
      String str = createString(i, specialChar1, specialChar2);
      MetaString metaString = encoder.encode(str);
      MetaStringDecoder decoder = new MetaStringDecoder(specialChar1, specialChar2);
      String newStr = decoder.decode(metaString.getBytes(), metaString.getEncoding(), metaString.getNumBits());
      Assert.assertEquals(newStr, str);
    }
  }
}
