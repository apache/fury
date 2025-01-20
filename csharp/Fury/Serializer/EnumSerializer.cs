using System;
using System.Threading;
using System.Threading.Tasks;

namespace Fury.Serializer;

internal sealed class EnumSerializer<TEnum> : AbstractSerializer<TEnum>
    where TEnum : struct
{
    public override void Write(SerializationContext context, in TEnum value)
    {
        // TODO: Serialize by name

        var v = Convert.ToUInt32(value);
        context.Writer.Write7BitEncodedUint(v);
    }
}

internal sealed class EnumDeserializer<TEnum> : AbstractDeserializer<TEnum>
    where TEnum : struct
{
    private static readonly EnumDeserializer<TEnum> Instance = new();

    private static readonly DeserializationProgress<EnumDeserializer<TEnum>> InstanceNotCreated = new(Instance){Status = DeserializationStatus.InstanceNotCreated};

    public override void CreateInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref Box<TEnum> boxedInstance
    )
    {
        CreateAndFillInstance(context, ref progress, ref boxedInstance.Unbox());
    }

    public override void FillInstance(
        DeserializationContext context,
        DeserializationProgress progress,
        Box<TEnum> boxedInstance
    ) { }

    public override void CreateAndFillInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref TEnum instance
    )
    {
        if (!context.Reader.TryRead7BitEncodedUint(out var e))
        {
            progress = InstanceNotCreated;
        }

        progress = DeserializationProgress.Completed;
        instance = (TEnum)Enum.ToObject(typeof(TEnum), e);
    }

    public override async ValueTask<Box<TEnum>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        return await CreateAndFillInstanceAsync(context, cancellationToken);
    }

    public override ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TEnum> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        return default;
    }

    public override async ValueTask<TEnum> CreateAndFillInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        var e = await context.Reader.Read7BitEncodedUintAsync(cancellationToken);
        return (TEnum)Enum.ToObject(typeof(TEnum), e);
    }
}
