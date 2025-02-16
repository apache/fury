using System;
using System.Threading;
using System.Threading.Tasks;
using Fury.Context;

namespace Fury.Serialization;

public abstract class AbstractSerializer<TTarget> : ISerializer<TTarget>
    where TTarget : notnull
{
    public abstract bool Write(SerializationContext context, in TTarget value);

    public virtual bool Write(SerializationContext context, object value)
    {
        var typedValue = (TTarget)value;
        return Write(context, in typedValue);
    }

    public virtual void Reset() { }

    public virtual void Dispose() { }
}

public abstract class AbstractDeserializer<TTarget> : IDeserializer<TTarget>
    where TTarget : notnull
{
    private bool _instanceCreated;

    public abstract bool CreateInstance(DeserializationContext context, ref Box<TTarget> boxedInstance);

    public abstract bool FillInstance(DeserializationContext context, Box<TTarget> boxedInstance);

    public virtual bool CreateAndFillInstance(DeserializationContext context, ref TTarget? instance)
    {
        var boxedInstance = Box<TTarget>.Empty;
        var justCreated = false;
        if (!_instanceCreated)
        {
            _instanceCreated = CreateInstance(context, ref boxedInstance);
            justCreated = true;
        }

        if (!_instanceCreated)
        {
            return false;
        }

        var completed = false;
        try
        {
            completed = FillInstance(context, boxedInstance);
            if (completed)
            {
                _instanceCreated = false;
            }
            instance = boxedInstance.Value;
        }
        catch (Exception)
        {
            if (justCreated)
            {
                instance = boxedInstance.Value;
            }
        }

        return completed;
    }

    public virtual async ValueTask<Box<TTarget>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        var instance = Box<TTarget>.Empty;
        while (!CreateInstance(context, ref instance))
        {
            await context.GetReader().ReadAsync(cancellationToken); // ensure there is new data to read
        }
        return instance;
    }

    public virtual async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TTarget> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        while (!FillInstance(context, boxedInstance))
        {
            await context.GetReader().ReadAsync(cancellationToken); // ensure there is new data to read
        }
    }

    public virtual async ValueTask<TTarget> CreateAndFillInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        var typedInstance = await CreateInstanceAsync(context, cancellationToken);
        await FillInstanceAsync(context, typedInstance, cancellationToken);
        return typedInstance.Value!;
    }

    bool IDeserializer.CreateInstance(DeserializationContext context, ref Box boxedInstance)
    {
        var typedInstance = boxedInstance.AsTyped<TTarget>();
        var completed = CreateInstance(context, ref typedInstance);
        boxedInstance = typedInstance.AsUntyped();
        return completed;
    }

    bool IDeserializer.FillInstance(DeserializationContext context, Box boxedInstance)
    {
        var typedInstance = boxedInstance.AsTyped<TTarget>();
        return FillInstance(context, typedInstance);
    }

    async ValueTask<Box> IDeserializer.CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken
    )
    {
        var typedInstance = await CreateInstanceAsync(context, cancellationToken);
        return typedInstance.AsUntyped();
    }

    async ValueTask IDeserializer.FillInstanceAsync(
        DeserializationContext context,
        Box instance,
        CancellationToken cancellationToken
    )
    {
        var typedInstance = instance.AsTyped<TTarget>();
        await FillInstanceAsync(context, typedInstance, cancellationToken);
    }

    public virtual void Reset()
    {
        _instanceCreated = false;
    }

    public virtual void Dispose() { }
}
