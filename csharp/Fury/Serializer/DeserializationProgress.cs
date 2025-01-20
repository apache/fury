using System;

namespace Fury.Serializer;

public class DeserializationProgress : IDisposable
{
    public static DeserializationProgress Completed { get; } = new() { Status = DeserializationStatus.Completed };

    internal IDeserializer? Deserializer { get; set; }
    public DeserializationStatus Status { get; set; } = DeserializationStatus.InstanceNotCreated;

    public virtual void Dispose() { }
}

internal class DeserializationProgress<TDeserializer> : DeserializationProgress
    where TDeserializer : IDeserializer
{
    public new TDeserializer Deserializer => (TDeserializer)base.Deserializer!;

    public DeserializationProgress(TDeserializer deserializer)
    {
        base.Deserializer = deserializer;
    }
}
