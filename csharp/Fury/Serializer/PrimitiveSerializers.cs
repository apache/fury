using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Fury.Serializer;

internal sealed class PrimitiveSerializer<T> : AbstractSerializer<T>
    where T : unmanaged
{
    public static PrimitiveSerializer<T> Instance { get; } = new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Write(SerializationContext context, in T value)
    {
        context.Writer.Write(value);
    }
}

internal sealed class PrimitiveDeserializer<T> : AbstractDeserializer<T>
    where T : unmanaged
{
    public static PrimitiveDeserializer<T> Instance { get; } = new();

    private static DeserializationProgress<PrimitiveDeserializer<T>> InstanceNotCreated { get; } = new(Instance);

    public override void CreateInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref Box<T> boxedInstance
    )
    {
        CreateAndFillInstance(context, ref progress, ref boxedInstance.Unbox());
    }

    public override void FillInstance(
        DeserializationContext context,
        DeserializationProgress progress,
        Box<T> boxedInstance
    ) { }

    public override void CreateAndFillInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref T value
    )
    {
        if (!context.Reader.TryRead(out value))
        {
            progress = InstanceNotCreated;
        }

        progress = DeserializationProgress.Completed;
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
        return await context.Reader.ReadAsync<T>(cancellationToken: cancellationToken);
    }
}
