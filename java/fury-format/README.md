# Fury Row Format

Fury row format is heavily inspired by spark tungsten row format, but with changes:

- Use arrow schema to describe meta.
- The implementation support java/C++/python/etc..
- String support latin/utf16/utf8 encoding.
- Decimal use arrow decimal format.
- Variable-size field can be inline in fixed-size region if small enough.
- Allow skip padding by generate Row using aot to put offsets in generated code.
- Support adding fields without breaking compatibility.

The initial fury java row data structure implementation is modified from spark unsafe row/writer.

It is possible to register custom type handling and collection factories for the row format -
see Encoders.registerCustomCodec and Encoders.registerCustomCollectionFactory.

A short example:

```
@Data
public static class UuidType {
  public UUID f1;
  public UUID[] f2;
  public SortedSet<UUID> f3;
}

static class UuidEncoder implements CustomCodec.MemoryBufferCodec<UUID> {
  @Override
  public MemoryBuffer encode(final UUID value) {
    final MemoryBuffer result = MemoryBuffer.newHeapBuffer(16);
    result.putInt64(0, value.getMostSignificantBits());
    result.putInt64(8, value.getLeastSignificantBits());
    return result;
  }

  @Override
  public UUID decode(final MemoryBuffer value) {
    return new UUID(value.readInt64(), value.readInt64());
  }
}

static class SortedSetOfUuidDecoder implements CustomCollectionFactory<UUID, SortedSet<UUID>> {
  @Override
  public SortedSet<UUID> newCollection(final int size) {
    return new TreeSet<>(UnsignedUuidComparator.INSTANCE);
  }
}

Encoders.registerCustomCodec(UUID.class, new UuidEncoder());
Encoders.registerCustomCollectionFactory(
    SortedSet.class, UUID.class, new SortedSetOfUuidDecoder());

RowEncoder<UuidType> encoder = Encoders.bean(UuidType.class);
```
