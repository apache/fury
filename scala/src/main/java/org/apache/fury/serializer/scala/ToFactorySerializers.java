package org.apache.fury.serializer.scala;

import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.serializer.Serializer;

import java.lang.reflect.Field;

public class ToFactorySerializers  {
  static final Class<?> IterableToFactoryClass = ReflectionUtils.loadClass(
    "scala.collection.IterableFactory$ToFactory");
  static final Class<?> MapToFactoryClass = ReflectionUtils.loadClass(
    "scala.collection.MapFactory$ToFactory");

  public static class IterableToFactorySerializer extends Serializer {
    private static final long fieldOffset;

    static {
      try {
        // for graalvm field offset auto rewrite
        Field field = Class.forName("scala.collection.IterableFactory$ToFactory").getDeclaredField("factory");
        fieldOffset = Platform.objectFieldOffset(field);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    public IterableToFactorySerializer(Fury fury) {
      super(fury, IterableToFactoryClass);
    }

    @Override
    public void write(MemoryBuffer buffer, Object value) {
      fury.writeRef(buffer, Platform.getObject(value, fieldOffset));
    }

    @Override
    public Object read(MemoryBuffer buffer) {
      Object o = Platform.newInstance(type);
      Platform.putObject(o, fieldOffset, fury.readRef(buffer));
      return o;
    }
  }

  public static class MapToFactorySerializer extends Serializer {
    private static final long fieldOffset;

    static {
      try {
        // for graalvm field offset auto rewrite
        Field field = Class.forName("scala.collection.MapFactory$ToFactory").getDeclaredField("factory");
        fieldOffset = Platform.objectFieldOffset(field);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    public MapToFactorySerializer(Fury fury) {
      super(fury, MapToFactoryClass);
    }

    @Override
    public void write(MemoryBuffer buffer, Object value) {
      fury.writeRef(buffer, Platform.getObject(value, fieldOffset));
    }

    @Override
    public Object read(MemoryBuffer buffer) {
      Object o = Platform.newInstance(type);
      Platform.putObject(o, fieldOffset, fury.readRef(buffer));
      return o;
    }
  }


}
