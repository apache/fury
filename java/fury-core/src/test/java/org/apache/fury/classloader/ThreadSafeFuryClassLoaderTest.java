package org.apache.fury.classloader;

import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ThreadSafeFuryClassLoaderTest {

  static class MyClassLoader extends ClassLoader {}

  @Test
  void testFuryThreadLocalUseProvidedClassLoader() throws InterruptedException {
    final MyClassLoader myClassLoader = new MyClassLoader();
    final ThreadSafeFury fury =
        Fury.builder()
            .withClassLoader(myClassLoader)
            .withLanguage(Language.JAVA)
            .requireClassRegistration(false)
            .buildThreadLocalFury();
    fury.setClassLoader(myClassLoader);

    Thread thread =
        new Thread(
            () -> {
              final ClassLoader t = fury.getClassLoader();
              Assert.assertEquals(t, myClassLoader);
            });
    thread.start();
    thread.join();
  }

  @Test
  void testFuryPoolUseProvidedClassLoader() throws InterruptedException {
    final MyClassLoader myClassLoader = new MyClassLoader();
    final ThreadSafeFury fury =
        Fury.builder()
            .withClassLoader(myClassLoader)
            .withLanguage(Language.JAVA)
            .requireClassRegistration(false)
            .buildThreadSafeFuryPool(1, 1);
    fury.setClassLoader(myClassLoader);

    Thread thread =
        new Thread(
            () -> {
              final ClassLoader t = fury.getClassLoader();
              Assert.assertEquals(t, myClassLoader);
            });
    thread.start();
    thread.join();
  }
}
