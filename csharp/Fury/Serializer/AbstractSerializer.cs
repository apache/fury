using System.Threading;
using System.Threading.Tasks;

namespace Fury.Serializer;

public abstract class AbstractSerializer<TValue> : ISerializer<TValue>
    where TValue : notnull
{
    public abstract void Write(SerializationContext context, in TValue value);

    public virtual void Write(SerializationContext context, object value)
    {
        var typedValue = (TValue)value;
        Write(context, in typedValue);
    }
}

public abstract class AbstractDeserializer<TValue> : IDeserializer<TValue>
    where TValue : notnull
{
    public abstract void CreateInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref Box<TValue> boxedInstance
    );

    public abstract void FillInstance(
        DeserializationContext context,
        DeserializationProgress progress,
        Box<TValue> boxedInstance
    );

    public virtual void CreateAndFillInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref TValue? instance
    )
    {
        var boxedInstance = Box<TValue>.Empty;
        if (progress is null or {Status: DeserializationStatus.InstanceNotCreated})
        {
            CreateInstance(context, ref progress, ref boxedInstance);
            if (progress is null or {Status: DeserializationStatus.InstanceNotCreated})
            {
                return;
            }
        }

        if (progress is {Status: DeserializationStatus.InstanceCreated})
        {
            FillInstance(context, progress, boxedInstance);
        }
    }

    public virtual async ValueTask<Box<TValue>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        DeserializationProgress? progress = default;
        var instance = Box<TValue>.Empty;
        while (progress is not { Status: DeserializationStatus.InstanceCreated or DeserializationStatus.Completed })
        {
            await context.Reader.ReadAsync(cancellationToken); // ensure there is new data to read
            CreateInstance(context, ref progress, ref instance);
        }
        return instance;
    }

    public virtual async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TValue> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        var progress = context.CurrentProgress;

        while (progress is not { Status: DeserializationStatus.Completed })
        {
            await context.Reader.ReadAsync(cancellationToken); // ensure there is new data to read
            FillInstance(context, progress, boxedInstance);
        }
    }

    public virtual async ValueTask<TValue> CreateAndFillInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        var instance = await CreateInstanceAsync(context, cancellationToken);
        await FillInstanceAsync(context, instance, cancellationToken);
        return instance.Value!;
    }

    void IDeserializer.CreateInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref Box boxedInstance
    )
    {
        var typedInstance = boxedInstance.AsTyped<TValue>();
        CreateInstance(context, ref progress, ref typedInstance);
        boxedInstance = typedInstance.AsUntyped();
    }

    void IDeserializer.FillInstance(DeserializationContext context, DeserializationProgress progress, Box boxedInstance)
    {
        var typedInstance = boxedInstance.AsTyped<TValue>();
        FillInstance(context, progress, typedInstance);
    }

    async ValueTask<Box> IDeserializer.CreateInstanceAsync(DeserializationContext context,
        CancellationToken cancellationToken)
    {
        var instance = await CreateInstanceAsync(context, cancellationToken);
        return instance.AsUntyped();
    }

    async ValueTask IDeserializer.FillInstanceAsync(DeserializationContext context,
        Box instance,
        CancellationToken cancellationToken)
    {
        var typedInstance = instance.AsTyped<TValue>();
        await FillInstanceAsync(context, typedInstance, cancellationToken);
    }
}
