using System;
using System.Threading;
using System.Threading.Tasks;
using Fury.Context;

namespace Fury.Serialization;

internal sealed class EnumSerializer<TEnum> : AbstractSerializer<TEnum>
    where TEnum : struct
{
    public override bool Write(SerializationContext context, in TEnum value)
    {
        // TODO: Serialize by name

        var v = Convert.ToUInt32(value);
        return context.GetWriter().TryWrite7BitEncodedUint(v);
    }
}

internal sealed class EnumDeserializer<TEnum> : AbstractDeserializer<TEnum>
    where TEnum : struct
{
    public override bool CreateInstance(DeserializationContext context, ref Box<TEnum> boxedInstance)
    {
        return CreateAndFillInstance(context, ref boxedInstance.Unbox());
    }

    public override bool FillInstance(DeserializationContext context, Box<TEnum> boxedInstance) => true;

    public override bool CreateAndFillInstance(DeserializationContext context, ref TEnum instance)
    {
        if (!context.GetReader().TryRead7BitEncodedUint(out var e))
        {
            return false;
        }

        instance = (TEnum)Enum.ToObject(typeof(TEnum), e);
        return true;
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
        var e = await context.GetReader().Read7BitEncodedUintAsync(cancellationToken);
        return (TEnum)Enum.ToObject(typeof(TEnum), e);
    }
}
