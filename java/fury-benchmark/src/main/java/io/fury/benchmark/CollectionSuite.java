package io.fury.benchmark;

import io.fury.Fury;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CollectionSuite {
  private static final Logger LOG = LoggerFactory.getLogger(CollectionSuite.class);

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      String commandLine = "io.*CollectionSuite.* -f 3 -wi 5 -i 5 -t 1 -w 2s -r 2s -rf csv";
      System.out.println(commandLine);
      args = commandLine.split(" ");
    }
    Main.main(args);
  }

  private static Fury fury = Fury.builder().build();
  private static List<Integer> list1 = new ArrayList<>(1024);
  private static byte[] list1Bytes;
  static {
    for (int i = 0; i < 1024; i++) {
      list1.add(i % 255);
    }
    list1Bytes = fury.serialize(list1);
    LOG.info("Size: {}", list1Bytes.length);
  }

  @Benchmark
  public Object serializeArrayList() {
    return fury.serialize(list1);
  }

  @Benchmark
  public Object deserializeArrayList() {
    return fury.deserialize(list1Bytes);
  }
  // Benchmark                              Mode  Cnt       Score        Error  Units
  // CollectionSuite.deserializeArrayList  thrpt    3  175281.624 ± 142913.891  ops/s
  // CollectionSuite.serializeArrayList    thrpt    3  137648.540 ± 158192.786  ops/s
}
