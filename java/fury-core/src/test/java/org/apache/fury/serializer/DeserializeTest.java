package org.apache.fury.serializer;

import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.codegen.JaninoUtils;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class DeserializeTest {

  @Test
  public void test() {
    ThreadSafeFury fury = Fury.builder()
        .withRefTracking(true)
        .requireClassRegistration(false)
        .withDeserializeUnexistedClass(true)
        .withCompatibleMode(CompatibleMode.COMPATIBLE)
//            .deserializeUnexistentEnumValueAsNull(true)
        .buildThreadSafeFury();
    byte[] data = readFileBytes("/Users/alsc/Downloads/code/incubator-fury/java/fury-core/src/main/resources/fury/testdata");
    Object res = fury.deserialize(data);
  }

  public static byte[] readFileBytes(String filePath) {
    try {
      File file = new File(filePath);
      long fileSize = file.length();
      if (fileSize > Integer.MAX_VALUE) {
        System.out.println("file too big...");
        return null;
      }
      FileInputStream fi = new FileInputStream(file);
      byte[] buffer = new byte[(int) fileSize];
      int offset = 0;
      int numRead = 0;
      while (offset < buffer.length && (numRead = fi.read(buffer, offset, buffer.length - offset)) >= 0) {
        offset += numRead;
      }
      // 确保所有数据均被读取
      if (offset != buffer.length) {
        throw new IOException("Could not completely read file " + file.getName());
      }
      fi.close();
      return buffer;
    } catch (IOException e) {
    }
    return null;
  }
}

