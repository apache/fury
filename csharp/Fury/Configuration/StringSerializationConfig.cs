using Fury.Serialization;

namespace Fury;

public record struct StringSerializationConfig(
    StringEncoding[] PreferredEncodings,
    bool WriteNumUtf16BytesForUtf8Encoding = false,
    int FastPathStringLengthThreshold = StringSerializationConfig.DefaultFastPathStringLengthThreshold
)
{
    public const int DefaultFastPathStringLengthThreshold = 127;

    public static StringSerializationConfig Default { get; } = new([StringEncoding.UTF8]);
}
