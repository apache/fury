package org.apache.fury.resolver;

import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.meta.MetaString;

import static org.apache.fury.meta.Encoders.GENERIC_DECODER;
import static org.apache.fury.meta.Encoders.GENERIC_ENCODER;

public class EnumStringResolver {

    private final MetaStringResolver metaStringResolver;

    public EnumStringResolver(MetaStringResolver metaStringResolver) {
        this.metaStringResolver = metaStringResolver;
    }
    public EnumStringResolver() {
        this.metaStringResolver = new MetaStringResolver();
    }

    public MetaStringBytes getOrCreateMetaStringBytes(MetaString str){
        return metaStringResolver.getOrCreateMetaStringBytes(str);
    }

    public MetaStringBytes getOrCreateMetaStringBytes(Enum value){
        return metaStringResolver.getOrCreateMetaStringBytes(
                GENERIC_ENCODER.encode(value.name(), MetaString.Encoding.UTF_8));
    }

    public void writeMetaStringBytes(MemoryBuffer memoryBuffer, MetaStringBytes byteString){
        metaStringResolver.writeMetaStringBytes(memoryBuffer, byteString);
    }

    public void writeMetaStringBytes(MemoryBuffer memoryBuffer, Enum value){
        MetaStringBytes metaStringBytes = getOrCreateMetaStringBytes(value);
        metaStringResolver.writeMetaStringBytes(memoryBuffer, metaStringBytes);
    }

    public String readMetaString(MemoryBuffer memoryBuffer, MetaStringBytes byteString) {
        MetaStringBytes metaStringBytes = metaStringResolver.readMetaStringBytes(memoryBuffer, byteString);
        return metaStringBytes.decode(GENERIC_DECODER);
    }

    public String readMetaString(MemoryBuffer memoryBuffer) {
        return metaStringResolver.readMetaString(memoryBuffer);
    }
}
