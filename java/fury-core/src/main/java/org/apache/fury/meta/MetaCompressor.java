package org.apache.fury.meta;

/**
 * An interface used to compress class metadata such as field names and types. The implementation of
 * this interface should be thread safe.
 */
public interface MetaCompressor {
  byte[] compress(byte[] data, int offset, int size);

  byte[] decompress(byte[] compressedData, int offset, int size);
}
