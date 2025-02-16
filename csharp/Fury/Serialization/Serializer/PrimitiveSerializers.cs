using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Fury.Context;

namespace Fury.Serialization;

internal sealed class PrimitiveSerializer<T> : AbstractSerializer<T>
    where T : unmanaged
{
    public static PrimitiveSerializer<T> Instance { get; } = new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Write(SerializationContext context, in T value)
    {
        return context.GetWriter().TryWrite(value);
    }
}

internal sealed class PrimitiveDeserializer<T> : AbstractDeserializer<T>
    where T : unmanaged
{
    public static PrimitiveDeserializer<T> Instance { get; } = new();

    public override bool CreateInstance(DeserializationContext context, ref Box<T> boxedInstance)
    {
        return CreateAndFillInstance(context, ref boxedInstance.Unbox());
    }

    public override bool FillInstance(DeserializationContext context, Box<T> boxedInstance) => true;

    public override bool CreateAndFillInstance(DeserializationContext context, ref T value)
    {
        return context.GetReader().TryRead(out value);
    }

    public override async ValueTask<Box<T>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        return await CreateAndFillInstanceAsync(context, cancellationToken);
    }

    public override ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<T> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        return default;
    }

    public override async ValueTask<T> CreateAndFillInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        return await context.GetReader().ReadAsync<T>(cancellationToken: cancellationToken);
    }
}
