using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Fury.Collections;
using Fury.Context;

namespace Fury.Serialization;

public abstract class CollectionDeserializer<TElement, TCollection>(TypeRegistration? elementRegistration)
    : AbstractDeserializer<TCollection>
    where TElement : notnull
    where TCollection : class, ICollection<TElement>
{
    private TypeRegistration? _elementRegistration = elementRegistration;

    protected int? Count;
    private int _index;

    public override bool FillInstance(DeserializationContext context, Box<TCollection> boxedInstance)
    {
        var instance = boxedInstance.Value!;

        if (_elementRegistration is null && TypeHelper<TElement>.IsSealed)
        {
            _elementRegistration = context.Fury.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        if (instance.TryGetSpan(out var elements))
        {
            for (; _index < Count; _index++)
            {
                if (context.Read<TElement>(_elementRegistration, out var element))
                {
                    elements[_index] = element!;
                }
                else
                {
                    return false;
                }
            }
        }
        else
        {
            for (; _index < Count; _index++)
            {
                if (context.Read<TElement>(_elementRegistration, out var element))
                {
                    instance.Add(element!);
                }
                else
                {
                    return false;
                }
            }
        }

        Reset();
        return true;
    }

    public override async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TCollection> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        if (FillInstance(context, boxedInstance))
        {
            return;
        }

        var instance = boxedInstance.Value!;

        if (_elementRegistration is null && TypeHelper<TElement>.IsSealed)
        {
            _elementRegistration = context.Fury.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        for (; _index < Count; _index++)
        {
            var item = await context.ReadAsync<TElement>(cancellationToken);
            instance.Add(item!);
        }

        Reset();
    }

    public override void Reset()
    {
        base.Reset();
        Count = null;
        _index = 0;
    }
}

public abstract class NullableCollectionDeserializer<TElement, TCollection>(TypeRegistration? elementRegistration)
    : AbstractDeserializer<TCollection>
    where TElement : struct
    where TCollection : class, ICollection<TElement?>
{
    private TypeRegistration? _elementRegistration = elementRegistration;

    protected int? Count;
    private int _index;

    public override bool FillInstance(DeserializationContext context, Box<TCollection> boxedInstance)
    {
        var instance = boxedInstance.Value!;

        if (_elementRegistration is null && TypeHelper<TElement>.IsSealed)
        {
            _elementRegistration = context.Fury.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        if (instance.TryGetSpan(out var elements))
        {
            for (; _index < Count; _index++)
            {
                if (context.ReadNullable<TElement>(_elementRegistration, out var element))
                {
                    elements[_index] = element;
                }
                else
                {
                    return false;
                }
            }
        }
        else
        {
            for (; _index < Count; _index++)
            {
                if (context.ReadNullable<TElement>(_elementRegistration, out var element))
                {
                    instance.Add(element);
                }
                else
                {
                    return false;
                }
            }
        }

        Reset();
        return true;
    }

    public override async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TCollection> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        if (FillInstance(context, boxedInstance))
        {
            return;
        }

        var instance = boxedInstance.Value!;

        if (_elementRegistration is null && TypeHelper<TElement>.IsSealed)
        {
            _elementRegistration = context.Fury.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        for (; _index < instance.Count; _index++)
        {
            var item = await context.ReadNullableAsync<TElement>(cancellationToken);
            instance.Add(item);
        }

        Reset();
    }

    public override void Reset()
    {
        base.Reset();
        Count = null;
        _index = 0;
    }
}
