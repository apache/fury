using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace Fury.Serializer;

public sealed class NotSupportedSerializer<TValue> : ISerializer<TValue>
    where TValue : notnull
{
    public void Write(SerializationContext context, in TValue value)
    {
        ThrowNotSupportedException();
    }

    public void Write(SerializationContext context, object value)
    {
        ThrowNotSupportedException();
    }

    private static void ThrowNotSupportedException()
    {
        ThrowHelper.ThrowNotSupportedException(ExceptionMessages.NotSupportedSerializer(typeof(TValue)));
    }
}

public sealed class NotSupportedDeserializer<TValue> : IDeserializer<TValue>
    where TValue : notnull
{
    public ValueTask<TValue> ReadAndCreateAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        ThrowNotSupportedException();
        return default!;
    }

    public ValueTask<Box> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        ThrowNotSupportedException();
        return default!;
    }

    public ValueTask ReadAndFillAsync(
        DeserializationContext context,
        Box instance,
        CancellationToken cancellationToken = default
    )
    {
        ThrowNotSupportedException();
        return default!;
    }

    [DoesNotReturn]
    private static void ThrowNotSupportedException()
    {
        ThrowHelper.ThrowNotSupportedException(ExceptionMessages.NotSupportedDeserializer(typeof(TValue)));
    }
}
