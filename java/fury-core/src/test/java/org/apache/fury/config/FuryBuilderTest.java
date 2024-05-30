package org.apache.fury.config;

import org.apache.fury.meta.MetaCompressor;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class FuryBuilderTest {

  @Test
  public void testWithMetaCompressor() {
    Assert.assertThrows(IllegalArgumentException.class, () -> {
      new FuryBuilder().withMetaCompressor(new MetaCompressor() {
        @Override
        public byte[] compress(byte[] data, int offset, int size) {
          return new byte[0];
        }

        @Override
        public byte[] decompress(byte[] compressedData, int offset, int size) {
          return new byte[0];
        }
      });
      new FuryBuilder().withMetaCompressor(new MetaCompressor() {
        @Override
        public byte[] compress(byte[] data, int offset, int size) {
          return new byte[0];
        }

        @Override
        public byte[] decompress(byte[] compressedData, int offset, int size) {
          return new byte[0];
        }

        @Override
        public boolean equals(Object o) {
          if (this == o) {
            return true;
          }
          return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
          return getClass().hashCode();
        }
      });
    });
  }
}
