package org.apache.fury;

import java.util.function.Consumer;
import org.apache.fury.serializer.Serializer;

public abstract class AbstractThreadSafeFury implements ThreadSafeFury {
  @Override
  public void register(Class<?> clz) {
    processCallback(fury -> fury.register(clz));
  }

  @Override
  public void register(Class<?> cls, boolean createSerializer) {
    processCallback(fury -> fury.register(cls, createSerializer));
  }

  @Override
  public void register(Class<?> cls, Short id) {
    processCallback(fury -> fury.register(cls, id));
  }

  @Override
  public void register(Class<?> cls, Short id, boolean createSerializer) {
    processCallback(fury -> fury.register(cls, id, createSerializer));
  }

  @Override
  public <T> void registerSerializer(Class<T> type, Class<? extends Serializer> serializerClass) {
    processCallback(fury -> fury.registerSerializer(type, serializerClass));
  }

  protected abstract void processCallback(Consumer<Fury> callback);
}
