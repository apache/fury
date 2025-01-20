using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Fury.Serializer;

public class CollectionDeserializerProgress : DeserializationProgress
{
    public int NotDeserializedCount;
}

public abstract class CollectionDeserializer<TElement, TCollection>(IDeserializer<TElement>? elementSerializer)
    : AbstractDeserializer<TCollection>
    where TElement : notnull
    where TCollection : class, ICollection<TElement?>
{
    private IDeserializer<TElement>? _elementDeserializer = elementSerializer;

    public override void FillInstance(
        DeserializationContext context,
        DeserializationProgress progress,
        Box<TCollection> boxedInstance
    )
    {
        var typedProgress = (CollectionDeserializerProgress)progress;
        var count = typedProgress.NotDeserializedCount;
        var instance = boxedInstance.Value!;

        var typedElementSerializer = _elementDeserializer;
        if (typedElementSerializer is null)
        {
            if (TypeHelper<TElement>.IsSealed)
            {
                typedElementSerializer = (IDeserializer<TElement>)context.GetDeserializer<TElement>();
                _elementDeserializer = typedElementSerializer;
            }
        }

        for (var i = 0; i < instance.Count; i++)
        {
            var status = context.Read(typedElementSerializer, out var element);
            if (status is DeserializationStatus.Completed)
            {
                instance.Add(element);
            }
            else
            {
                typedProgress.NotDeserializedCount = count - i;
                return;
            }
        }

        typedProgress.Status = DeserializationStatus.Completed;
    }

    public override async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TCollection> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        var instance = boxedInstance.Value!;
        var typedElementSerializer = _elementDeserializer;
        if (typedElementSerializer is null)
        {
            if (TypeHelper<TElement>.IsSealed)
            {
                typedElementSerializer = (IDeserializer<TElement>)context.GetDeserializer<TElement>();
                _elementDeserializer = typedElementSerializer;
            }
        }

        for (var i = 0; i < instance.Count; i++)
        {
            var item = await context.ReadAsync<TElement>(typedElementSerializer, cancellationToken);
            instance.Add(item);
        }
    }
}

public abstract class NullableCollectionDeserializer<TElement, TEnumerable>(IDeserializer<TElement>? elementSerializer)
    : AbstractDeserializer<TEnumerable>
    where TElement : struct
    where TEnumerable : class, ICollection<TElement?>
{
    private IDeserializer<TElement>? _elementDeserializer = elementSerializer;

    public override async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TEnumerable> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        var instance = boxedInstance.Value!;
        var typedElementSerializer = _elementDeserializer;
        if (typedElementSerializer is null)
        {
            if (TypeHelper<TElement>.IsSealed)
            {
                typedElementSerializer = (IDeserializer<TElement>)context.GetDeserializer<TElement>();
                _elementDeserializer = typedElementSerializer;
            }
        }

        for (var i = 0; i < instance.Count; i++)
        {
            var item = await context.ReadNullableAsync<TElement>(typedElementSerializer, cancellationToken);
            instance.Add(item);
        }
    }
}
