using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Fury.Context;

namespace Fury.Serialization;

// List can be created with an initial capacity, so we use a specific deserializer for it.

internal sealed class ListDeserializer<TElement>(TypeRegistration? elementRegistration)
    : CollectionDeserializer<TElement, List<TElement>>(elementRegistration)
    where TElement : notnull
{
    public override bool CreateInstance(DeserializationContext context, ref Box<List<TElement>> boxedInstance)
    {
        if (Count is null)
        {
            if (!context.GetReader().TryReadCount(out var count))
            {
                return false;
            }
            Count = count;
        }

        boxedInstance = new List<TElement>(Count.Value);
        return true;
    }

    public override async ValueTask<Box<List<TElement>>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        Count ??= await context.GetReader().ReadCountAsync(cancellationToken);
        return new List<TElement>(Count.Value);
    }
}

internal sealed class NullableListDeserializer<TElement>(TypeRegistration? elementRegistration)
    : NullableCollectionDeserializer<TElement, List<TElement?>>(elementRegistration)
    where TElement : struct
{
    public override bool CreateInstance(DeserializationContext context, ref Box<List<TElement?>> boxedInstance)
    {
        if (Count is null)
        {
            if (!context.GetReader().TryReadCount(out var count))
            {
                return false;
            }
            Count = count;
        }

        boxedInstance = new List<TElement?>(Count.Value);
        return true;
    }

    public override async ValueTask<Box<List<TElement?>>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        Count ??= await context.GetReader().ReadCountAsync(cancellationToken);
        return new List<TElement?>(Count.Value);
    }
}
