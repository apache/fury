using System;
using System.Threading;
using System.Threading.Tasks;
using Fury.Buffers;
using JetBrains.Annotations;

namespace Fury.Serializer;

internal class ArraySerializer<TElement>(ISerializer<TElement>? elementSerializer) : AbstractSerializer<TElement?[]>
    where TElement : notnull
{
    [UsedImplicitly]
    public ArraySerializer()
        : this(null) { }

    public override void Write(SerializationContext context, in TElement?[] value)
    {
        context.Writer.WriteCount(value.Length);
        foreach (var element in value)
        {
            context.Write(element, elementSerializer);
        }
    }
}

internal class NullableArraySerializer<TElement>(ISerializer<TElement>? elementSerializer)
    : AbstractSerializer<TElement?[]>
    where TElement : struct
{
    [UsedImplicitly]
    public NullableArraySerializer()
        : this(null) { }

    public override void Write(SerializationContext context, in TElement?[] value)
    {
        context.Writer.WriteCount(value.Length);
        foreach (var element in value)
        {
            context.Write(element, elementSerializer);
        }
    }
}

internal class ArrayDeserializationProgress<TDeserializer>(
    TDeserializer deserializer,
    ConcurrentObjectPool<ArrayDeserializationProgress<TDeserializer>> pool
) : DeserializationProgress<TDeserializer>(deserializer)
    where TDeserializer : IDeserializer
{
    private ConcurrentObjectPool<ArrayDeserializationProgress<TDeserializer>> Pool { get; } = pool;
    public int CurrentIndex;

    private void Reset()
    {
        CurrentIndex = 0;
        Status = DeserializationStatus.InstanceNotCreated;
    }

    public override void Dispose()
    {
        Reset();
        Pool.Return(this);
    }
}

internal class ArrayDeserializer<TElement> : AbstractDeserializer<TElement?[]>
    where TElement : notnull
{
    private readonly IDeserializer<TElement>? _elementDeserializer;
    private readonly ConcurrentObjectPool<ArrayDeserializationProgress<ArrayDeserializer<TElement>>> _progressPool;

    [UsedImplicitly]
    public ArrayDeserializer()
        : this(null) { }

    public ArrayDeserializer(IDeserializer<TElement>? elementDeserializer)
    {
        _elementDeserializer = elementDeserializer;
        _progressPool = new ConcurrentObjectPool<ArrayDeserializationProgress<ArrayDeserializer<TElement>>>(
            pool => new ArrayDeserializationProgress<ArrayDeserializer<TElement>>(this, pool)
        );
    }

    public override void CreateInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref Box<TElement?[]> boxedInstance
    )
    {
        if (progress is not ArrayDeserializationProgress<ArrayDeserializer<TElement>>)
        {
            progress = _progressPool.Rent();
        }
        if (context.Reader.TryReadCount(out var length))
        {
            boxedInstance = new TElement?[length];
            progress.Status = DeserializationStatus.InstanceCreated;
        }
        else
        {
            boxedInstance = Box<TElement?[]>.Empty;
        }
    }

    public override void FillInstance(
        DeserializationContext context,
        DeserializationProgress progress,
        Box<TElement?[]> boxedInstance
    )
    {
        var typedProgress = (ArrayDeserializationProgress<ArrayDeserializer<TElement>>)progress;
        var instance = boxedInstance.Value!;
        var current = typedProgress.CurrentIndex;
        for (; current < instance.Length; current++)
        {
            var status = context.Read(_elementDeserializer, out instance[current]);
            if (status is not DeserializationStatus.Completed)
            {
                break;
            }
        }

        if (current == instance.Length)
        {
            typedProgress.Status = DeserializationStatus.Completed;
        }
    }

    public override async ValueTask<Box<TElement?[]>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        var length = await context.Reader.ReadCountAsync(cancellationToken);
        return new TElement?[length];
    }

    public override async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TElement?[]> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        var instance = boxedInstance.Value!;
        for (var i = 0; i < instance.Length; i++)
        {
            instance[i] = await context.ReadAsync<TElement>(_elementDeserializer, cancellationToken);
        }
    }
}

internal class NullableArrayDeserializer<TElement> : AbstractDeserializer<TElement?[]>
    where TElement : struct
{
    private readonly IDeserializer<TElement>? _elementDeserializer;
    private readonly ConcurrentObjectPool<
        ArrayDeserializationProgress<NullableArrayDeserializer<TElement>>
    > _progressPool;

    [UsedImplicitly]
    public NullableArrayDeserializer()
        : this(null) { }

    public NullableArrayDeserializer(IDeserializer<TElement>? elementDeserializer)
    {
        _elementDeserializer = elementDeserializer;
        _progressPool = new ConcurrentObjectPool<ArrayDeserializationProgress<NullableArrayDeserializer<TElement>>>(
            pool => new ArrayDeserializationProgress<NullableArrayDeserializer<TElement>>(this, pool)
        );
    }

    public override void CreateInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref Box<TElement?[]> boxedInstance
    )
    {
        if (progress is not ArrayDeserializationProgress<NullableArrayDeserializer<TElement>>)
        {
            progress = _progressPool.Rent();
        }
        if (context.Reader.TryReadCount(out var length))
        {
            boxedInstance = new TElement?[length];
            progress.Status = DeserializationStatus.InstanceCreated;
        }
        else
        {
            boxedInstance = Box<TElement?[]>.Empty;
        }
    }

    public override void FillInstance(
        DeserializationContext context,
        DeserializationProgress progress,
        Box<TElement?[]> boxedInstance
    )
    {
        var typedProgress = (ArrayDeserializationProgress<NullableArrayDeserializer<TElement>>)progress;
        var instance = boxedInstance.Value!;
        var current = typedProgress.CurrentIndex;
        for (; current < instance.Length; current++)
        {
            var status = context.ReadNullable(_elementDeserializer, out instance[current]);
            if (status is not DeserializationStatus.Completed)
            {
                break;
            }
        }

        if (current == instance.Length)
        {
            typedProgress.Status = DeserializationStatus.Completed;
        }
    }

    public override async ValueTask<Box<TElement?[]>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        var length = await context.Reader.ReadCountAsync(cancellationToken);
        return new TElement?[length];
    }

    public override async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TElement?[]> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        var instance = boxedInstance.Value!;
        for (var i = 0; i < instance.Length; i++)
        {
            instance[i] = await context.ReadNullableAsync<TElement>(_elementDeserializer, cancellationToken);
        }
    }
}

internal sealed class PrimitiveArraySerializer<TElement> : AbstractSerializer<TElement[]>
    where TElement : unmanaged
{
    public static PrimitiveArraySerializer<TElement> Instance { get; } = new();

    public override void Write(SerializationContext context, in TElement[] value)
    {
        context.Writer.WriteCount(value.Length);
        context.Writer.Write<TElement>(value);
    }
}

internal sealed class PrimitiveArrayDeserializer<TElement> : ArrayDeserializer<TElement>
    where TElement : unmanaged
{
    public static PrimitiveArrayDeserializer<TElement> Instance { get; } = new();

    public override void FillInstance(
        DeserializationContext context,
        DeserializationProgress progress,
        Box<TElement[]> boxedInstance
    )
    {
        var typedProgress = (ArrayDeserializationProgress<ArrayDeserializer<TElement>>)progress;
        var instance = boxedInstance.Value!;
        var current = typedProgress.CurrentIndex;

        var readCount = context.Reader.ReadMemory(instance.AsSpan(current));

        if (readCount + current == instance.Length)
        {
            typedProgress.Status = DeserializationStatus.Completed;
        }
        else
        {
            typedProgress.CurrentIndex = readCount + current;
        }
    }

    public override async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TElement[]> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        var instance = boxedInstance.Value!;
        await context.Reader.ReadMemoryAsync<TElement>(instance, cancellationToken);
    }
}
