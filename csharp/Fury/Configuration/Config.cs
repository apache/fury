using System.Collections.Generic;
using Fury.Buffers;
using Fury.Serialization;

namespace Fury;

public sealed record Config(
    bool ReferenceTracking,
    ISerializationProvider SerializationProvider,
    StringSerializationConfig StringSerializationConfig
);
