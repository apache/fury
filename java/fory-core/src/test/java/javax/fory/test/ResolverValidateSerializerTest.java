package javax.fory.test;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.fory.Fory;
import org.apache.fory.memory.MemoryBuffer;
import org.apache.fory.serializer.Serializer;
import org.apache.fory.serializer.collection.AbstractCollectionSerializer;
import org.apache.fory.serializer.collection.AbstractMapSerializer;
import org.testng.annotations.Test;

public class ResolverValidateSerializerTest {
  static final class InvalidList extends AbstractList<Object> {
    @Override
    public Object get(int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public int size() {
      return 0;
    }

    public static final class InvalidListSerializer extends Serializer<InvalidList> {
      public InvalidListSerializer(Fory fory) {
        super(fory, InvalidList.class);
      }

      @Override
      public void write(MemoryBuffer buffer, InvalidList value) {
        // no-op
      }

      @Override
      public InvalidList read(MemoryBuffer buffer) {
        return new InvalidList();
      }
    }
  }

  static final class ValidList extends AbstractList<Object> {
    @Override
    public Object get(int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public int size() {
      return 0;
    }

    public static final class ValidListSerializer extends AbstractCollectionSerializer<ValidList> {
      public ValidListSerializer(Fory fory) {
        super(fory, ValidList.class);
      }

      @Override
      public Collection<?> onCollectionWrite(MemoryBuffer buffer, ValidList value) {
        return Collections.emptyList();
      }

      @Override
      public ValidList read(MemoryBuffer buffer) {
        return onCollectionRead(Collections.emptyList());
      }

      @Override
      public ValidList onCollectionRead(Collection collection) {
        return new ValidList();
      }
    }
  }

  static final class InvalidMap extends AbstractMap<Object, Object> {
    @Override
    public Set<Entry<Object, Object>> entrySet() {
      return Collections.emptySet();
    }

    public static final class InvalidMapSerializer extends Serializer<InvalidMap> {
      public InvalidMapSerializer(Fory fory) {
        super(fory, InvalidMap.class);
      }

      @Override
      public void write(MemoryBuffer buffer, InvalidMap value) {
        // no-op
      }

      @Override
      public InvalidMap read(MemoryBuffer buffer) {
        return new InvalidMap();
      }
    }
  }

  static final class ValidMap extends AbstractMap<Object, Object> {
    @Override
    public Set<Entry<Object, Object>> entrySet() {
      return Collections.emptySet();
    }

    public static final class ValidMapSerializer extends AbstractMapSerializer<ValidMap> {
      public ValidMapSerializer(Fory fory) {
        super(fory, ValidMap.class);
      }

      @Override
      public Map<?, ?> onMapWrite(MemoryBuffer buffer, ValidMap value) {
        return Collections.emptyMap();
      }

      @Override
      public ValidMap onMapCopy(Map map) {
        return new ValidMap();
      }

      @Override
      public ValidMap read(MemoryBuffer buffer) {
        return onMapRead(Collections.emptyMap());
      }

      @Override
      public ValidMap onMapRead(Map map) {
        return new ValidMap();
      }
    }
  }

  @Test
  public void testListAndMapSerializerRegistration() {
    Fory fory = Fory.builder().withRefTracking(true).requireClassRegistration(false).build();
    // List invalid
    assertThrows(
        IllegalArgumentException.class,
        () -> fory.registerSerializer(InvalidList.class, InvalidList.InvalidListSerializer.class));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            fory.registerSerializer(
                InvalidList.class, new InvalidList.InvalidListSerializer(fory)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            fory.registerSerializer(
                InvalidList.class, f -> new InvalidList.InvalidListSerializer(f)));
    // List valid
    fory.register(ValidList.class);
    fory.registerSerializer(ValidList.class, new ValidList.ValidListSerializer(fory));
    Object listResult = fory.deserialize(fory.serialize(new ValidList()));
    assertTrue(listResult instanceof ValidList);
    // Map invalid
    assertThrows(
        IllegalArgumentException.class,
        () -> fory.registerSerializer(InvalidMap.class, InvalidMap.InvalidMapSerializer.class));
    assertThrows(
        IllegalArgumentException.class,
        () -> fory.registerSerializer(InvalidMap.class, new InvalidMap.InvalidMapSerializer(fory)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            fory.registerSerializer(InvalidMap.class, f -> new InvalidMap.InvalidMapSerializer(f)));
    // Map valid
    fory.register(ValidMap.class);
    fory.registerSerializer(ValidMap.class, new ValidMap.ValidMapSerializer(fory));
    Object mapResult = fory.deserialize(fory.serialize(new ValidMap()));
    assertTrue(mapResult instanceof ValidMap);
  }
}
