using System;
using System.Threading;
using System.Threading.Tasks;

namespace Fury.Serializer;

public sealed class NotSupportedSerializer<TValue> : ISerializer<TValue>
    where TValue : notnull
{
    public void Write(SerializationContext context, in TValue value)
    {
        throw new NotSupportedException();
    }

    public void Write(SerializationContext context, object value)
    {
        throw new NotSupportedException();
    }
}

public sealed class NotSupportedDeserializer<TValue> : IDeserializer<TValue>
    where TValue : notnull
{
    public void CreateInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref Box<TValue> boxedInstance
    )
    {
        throw new NotSupportedException();
    }

    public void FillInstance(DeserializationContext context,
        DeserializationProgress progress,
        Box<TValue> boxedInstance)
    {
        throw new NotSupportedException();
    }

    public void CreateAndFillInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref TValue? instance
    )
    {
        throw new NotSupportedException();
    }

    public ValueTask<TValue> CreateAndFillInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        throw new NotSupportedException();
    }

    public void CreateInstance(DeserializationContext context, ref DeserializationProgress? progress,
        ref Box boxedInstance)
    {
        throw new NotSupportedException();
    }

    public void FillInstance(DeserializationContext context, DeserializationProgress progress, Box boxedInstance)
    {
        throw new NotSupportedException();
    }

    public ValueTask<Box> CreateInstanceAsync(DeserializationContext context,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public ValueTask FillInstanceAsync(DeserializationContext context,
        Box instance,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }
}
