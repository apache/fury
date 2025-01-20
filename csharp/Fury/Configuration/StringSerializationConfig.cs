using Fury.Serializer;

namespace Fury;

public record struct StringSerializationConfig(StringEncoding[] PreferredEncodings)
{
    public static StringSerializationConfig Default { get; } = new([StringEncoding.UTF8]);
}
