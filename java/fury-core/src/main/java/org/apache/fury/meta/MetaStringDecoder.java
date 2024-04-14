package org.apache.fury.meta;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class MetaStringDecoder {
  private static final int FLAG_OFFSET = 8;

  private final char specialChar1;
  private final char specialChar2;

  public MetaStringDecoder(char specialChar1, char specialChar2) {
    this.specialChar1 = specialChar1;
    this.specialChar2 = specialChar2;
  }

  public String decode(byte[] encodedData, int numBits) {
    if (encodedData.length == 0) {
      return "";
    }
    // The very first byte signifies the encoding used
    MetaString.Encoding chosenEncoding = MetaString.Encoding.fromInt(encodedData[0] & 0xFF);
    // Extract actual data, skipping the first byte (encoding flag)
    encodedData = Arrays.copyOfRange(encodedData, 1, encodedData.length);
    return decode(encodedData, chosenEncoding, numBits);
  }

  public String decode(byte[] encodedData, MetaString.Encoding chosenEncoding, int numBits) {
    switch (chosenEncoding) {
      case LOWER_SPECIAL:
        return decodeLowerSpecial(encodedData, false, numBits);
      case LOWER_UPPER_DIGIT_SPECIAL:
        return decodeLowerUpperDigitSpecial(encodedData, false, numBits);
      case REP_FIRST_TO_LOWER_SPECIAL:
        return decodeRepFirstLowerSpecial(encodedData, numBits);
      case REP_ALL_TO_LOWER_SPECIAL:
        return decodeRepMulLowerSpecial(encodedData);
      case UTF_8:
        return new String(encodedData, StandardCharsets.UTF_8);
      default:
        throw new IllegalStateException("Unexpected encoding flag: " + chosenEncoding);
    }
  }

  // Function to adjust indices by removing flag position if present
  private byte getValueWithFlagOffset(byte encodedValue, boolean hasFlag) {
    return hasFlag ? (byte) (encodedValue & ~(1 << FLAG_OFFSET)) : encodedValue;
  }

  // Decoding method for LOWER_SPECIAL
  private String decodeLowerSpecial(byte[] data, boolean hasFlag, int numBits) {
    StringBuilder decoded = new StringBuilder();
    int bitIndex = 0;
    int bitMask = 0b11111; // 5 bits for mask
    while (bitIndex + 5 <= numBits) {
      int byteIndex = bitIndex / 8;
      int intraByteIndex = bitIndex % 8;

      // Extract the 5-bit character value across byte boundaries if needed
      int charValue =
          ((data[byteIndex] & 0xFF) << 8)
              | (byteIndex + 1 < data.length ? (data[byteIndex + 1] & 0xFF) : 0);
      charValue =
          getValueWithFlagOffset((byte) ((charValue >> (11 - intraByteIndex)) & bitMask), hasFlag);
      bitIndex += 5;
      decoded.append(decodeLowerSpecialChar(charValue));
    }

    return decoded.toString();
  }

  // Decoding method for LOWER_UPPER_DIGIT_SPECIAL
  private String decodeLowerUpperDigitSpecial(byte[] data, boolean hasFlag, int numBits) {
    StringBuilder decoded = new StringBuilder();
    int bitIndex = 0;
    int bitMask = 0b111111; // 6 bits for mask
    while (bitIndex + 6 <= numBits) {
      int byteIndex = bitIndex / 8;
      int intraByteIndex = bitIndex % 8;

      // Extract the 6-bit character value across byte boundaries if needed
      int charValue =
          ((data[byteIndex] & 0xFF) << 8)
              | (byteIndex + 1 < data.length ? (data[byteIndex + 1] & 0xFF) : 0);
      charValue =
          getValueWithFlagOffset((byte) ((charValue >> (10 - intraByteIndex)) & bitMask), hasFlag);
      bitIndex += 6;
      decoded.append(decodeLowerUpperDigitSpecialChar(charValue));
    }

    return decoded.toString();
  }

  // Decoding special char for LOWER_SPECIAL based on your custom mapping
  private char decodeLowerSpecialChar(int charValue) {
    if (charValue >= 0 && charValue <= 25) {
      return (char) ('a' + charValue);
    } else if (charValue == 26) {
      return '.';
    } else if (charValue == 27) {
      return '_';
    } else if (charValue == 28) {
      return '$';
    } else if (charValue == 29) {
      return '|';
    } else {
      throw new IllegalArgumentException("Invalid character value for LOWER_SPECIAL: " + charValue);
    }
  }

  // Decoding special char for LOWER_UPPER_DIGIT_SPECIAL based on your custom mapping
  private char decodeLowerUpperDigitSpecialChar(int charValue) {
    if (charValue >= 0 && charValue <= 25) {
      return (char) ('a' + charValue);
    } else if (charValue >= 26 && charValue <= 51) {
      return (char) ('A' + (charValue - 26));
    } else if (charValue >= 52 && charValue <= 61) {
      return (char) ('0' + (charValue - 52));
    } else if (charValue == 62) {
      return specialChar1;
    } else if (charValue == 63) {
      return specialChar2;
    } else {
      throw new IllegalArgumentException(
          "Invalid character value for LOWER_UPPER_DIGIT_SPECIAL: " + charValue);
    }
  }

  // Placeholder function for REP_FIRST_LOWER_SPECIAL decoding
  private String decodeRepFirstLowerSpecial(byte[] data, int len) {
    // Assuming first byte (after the flag) is the count of repetition and the second is the
    // repeated character
    int count = data[0] & 0xFF;
    char repeatedChar = (char) data[1];

    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < count; i++) {
      builder.append(repeatedChar);
    }

    // Append the rest of the decoded string starting from the third byte
    String restOfString = decodeLowerSpecial(Arrays.copyOfRange(data, 2, data.length), false, len);
    return builder.append(restOfString).toString();
  }

  // Placeholder function for REP_MUL_LOWER_SPECIAL decoding
  private String decodeRepMulLowerSpecial(byte[] data) {
    // Assuming some kind of run-length encoding for duplicate characters.
    // Example implementation; real implementation depends on the encoding details
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < data.length; i++) {
      // Let's assume each run is encoded in two bytes: one for the character and
      // one for the run count (which needs to be a small number for simplicity)
      char character = (char) data[i];
      i++;
      int runLength = data[i] & 0xFF;
      for (int run = 0; run < runLength; run++) {
        builder.append(character);
      }
    }

    return builder.toString();
  }
}
