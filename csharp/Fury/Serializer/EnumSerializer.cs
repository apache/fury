using System;
using System.Threading;
using System.Threading.Tasks;

namespace Fury.Serializer;

internal sealed class EnumSerializer<TEnum> : AbstractSerializer<TEnum>
    where TEnum : Enum
{
    public override void Write(SerializationContext context, in TEnum value)
    {
        // TODO: Serialize by name

        var v = Convert.ToInt32(value);
        context.Writer.Write7BitEncodedInt(v);
    }
}

internal sealed class EnumDeserializer<TEnum> : AbstractDeserializer<TEnum>
    where TEnum : Enum
{
    public override ValueTask<Box<TEnum>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        return new ValueTask<Box<TEnum>>(new Box<TEnum>(default!));
    }

    public override async ValueTask ReadAndFillAsync(
        DeserializationContext context,
        Box<TEnum> instance,
        CancellationToken cancellationToken = default
    )
    {
        var v = await context.Reader.Read7BitEncodedIntAsync(cancellationToken);
        instance.Value = (TEnum)Enum.ToObject(typeof(TEnum), v);
    }
}
