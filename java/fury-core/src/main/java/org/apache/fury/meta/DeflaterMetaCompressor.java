package org.apache.fury.meta;

import java.io.ByteArrayOutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/** A meta compressor based on {@link Deflater} compression algorithm. */
public class DeflaterMetaCompressor implements MetaCompressor {
  @Override
  public byte[] compress(byte[] input, int offset, int size) {
    Deflater deflater = new Deflater();
    deflater.setInput(input, offset, size);
    deflater.finish();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[128];
    while (!deflater.finished()) {
      int compressedSize = deflater.deflate(buffer);
      outputStream.write(buffer, 0, compressedSize);
    }
    return outputStream.toByteArray();
  }

  @Override
  public byte[] decompress(byte[] input, int offset, int size) {
    Inflater inflater = new Inflater();
    inflater.setInput(input, offset, size);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[128];
    try {
      while (!inflater.finished()) {
        int decompressedSize = inflater.inflate(buffer);
        outputStream.write(buffer, 0, decompressedSize);
      }
    } catch (DataFormatException e) {
      throw new RuntimeException(e);
    }
    return outputStream.toByteArray();
  }
}
