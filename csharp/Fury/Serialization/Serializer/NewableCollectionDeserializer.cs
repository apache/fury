using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Fury.Context;

namespace Fury.Serialization;

public sealed class NewableCollectionDeserializer<TElement, TCollection>(TypeRegistration? elementRegistration)
    : CollectionDeserializer<TElement, TCollection>(elementRegistration)
    where TElement : notnull
    where TCollection : class, ICollection<TElement>, new()
{
    public override bool CreateInstance(DeserializationContext context, ref Box<TCollection> boxedInstance)
    {
        if (Count is null)
        {
            if (!context.GetReader().TryReadCount(out var count))
            {
                return false;
            }
            Count = count;
        }

        boxedInstance = new TCollection();
        return true;
    }

    public override async ValueTask<Box<TCollection>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        Count ??= await context.GetReader().ReadCountAsync(cancellationToken);
        return new TCollection();
    }
}

public sealed class NullableNewableCollectionDeserializer<TElement, TCollection>(TypeRegistration? elementRegistration)
    : NullableCollectionDeserializer<TElement, TCollection>(elementRegistration)
    where TElement : struct
    where TCollection : class, ICollection<TElement?>, new()
{
    public override bool CreateInstance(DeserializationContext context, ref Box<TCollection> boxedInstance)
    {
        if (Count is null)
        {
            if (!context.GetReader().TryReadCount(out var count))
            {
                return false;
            }
            Count = count;
        }

        boxedInstance = new TCollection();
        return true;
    }

    public override async ValueTask<Box<TCollection>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        Count ??= await context.GetReader().ReadCountAsync(cancellationToken);
        return new TCollection();
    }
}
