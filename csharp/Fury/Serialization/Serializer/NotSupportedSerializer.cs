using System;
using System.Threading;
using System.Threading.Tasks;
using Fury.Context;
using JetBrains.Annotations;

namespace Fury.Serialization;

public sealed class NotSupportedSerializer<TTarget> : ISerializer<TTarget>
    where TTarget : notnull
{
    public bool Write(SerializationContext context, in TTarget value)
    {
        throw new NotSupportedException();
    }

    public bool Write(SerializationContext context, object value)
    {
        throw new NotSupportedException();
    }

    public void Reset() { }

    public void Dispose() { }
}

public sealed class NotSupportedDeserializer<TTarget> : IDeserializer<TTarget>
    where TTarget : notnull
{
    public bool CreateAndFillInstance(DeserializationContext context, ref TTarget? instance)
    {
        throw new NotSupportedException();
    }

    public ValueTask<TTarget> CreateAndFillInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        throw new NotSupportedException();
    }

    public bool CreateInstance(DeserializationContext context, ref Box boxedInstance)
    {
        throw new NotSupportedException();
    }

    public bool FillInstance(DeserializationContext context, Box boxedInstance)
    {
        throw new NotSupportedException();
    }

    public ValueTask<Box> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        throw new NotSupportedException();
    }

    public ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box instance,
        CancellationToken cancellationToken = default
    )
    {
        throw new NotSupportedException();
    }

    public void Reset() { }

    public void Dispose() { }
}
