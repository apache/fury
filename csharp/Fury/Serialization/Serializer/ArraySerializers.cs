using System;
using System.Threading;
using System.Threading.Tasks;
using Fury.Context;
using JetBrains.Annotations;

namespace Fury.Serialization;

internal class ArraySerializer<TElement>(TypeRegistration? elementRegistration) : AbstractSerializer<TElement?[]>
    where TElement : notnull
{
    private TypeRegistration? _elementRegistration = elementRegistration;
    private bool _hasWrittenCount;
    private int _index;

    [UsedImplicitly]
    public ArraySerializer()
        : this(null) { }

    [Macro]
    public override bool Write(SerializationContext context, in TElement?[] value)
    {
        if (_elementRegistration is null && TypeHelper<TElement>.IsSealed)
        {
            _elementRegistration = context.Fury.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        if (!_hasWrittenCount)
        {
            _hasWrittenCount = context.GetWriter().TryWriteCount(value.Length);
            if (!_hasWrittenCount)
            {
                return false;
            }
        }

        for (; _index < value.Length; _index++)
        {
            if (!context.Write(in value[_index], _elementRegistration))
            {
                return false;
            }
        }

        return true;
    }

    public override void Reset()
    {
        base.Reset();
        _index = 0;
    }
}

internal class NullableArraySerializer<TElement>(TypeRegistration? elementRegistration)
    : AbstractSerializer<TElement?[]>
    where TElement : struct
{
    private TypeRegistration? _elementRegistration = elementRegistration;
    private bool _hasWrittenCount;
    private int _index;

    [UsedImplicitly]
    public NullableArraySerializer()
        : this(null) { }

    public override bool Write(SerializationContext context, in TElement?[] value)
    {
        if (_elementRegistration is null && TypeHelper<TElement>.IsSealed)
        {
            _elementRegistration = context.Fury.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        if (!_hasWrittenCount)
        {
            _hasWrittenCount = context.GetWriter().TryWriteCount(value.Length);
            if (!_hasWrittenCount)
            {
                return false;
            }
        }

        context.GetWriter().TryWriteCount(value.Length);
        for (; _index < value.Length; _index++)
        {
            if (!context.Write(in value[_index], _elementRegistration))
            {
                return false;
            }
        }

        return true;
    }

    public override void Reset()
    {
        base.Reset();
        _index = 0;
    }
}

internal class ArrayDeserializer<TElement>(TypeRegistration? elementRegistration) : AbstractDeserializer<TElement?[]>
    where TElement : notnull
{
    private TypeRegistration? _elementRegistration = elementRegistration;

    private int _index;

    [UsedImplicitly]
    public ArrayDeserializer()
        : this(null) { }

    public override bool CreateInstance(DeserializationContext context, ref Box<TElement?[]> boxedInstance)
    {
        if (!context.GetReader().TryReadCount(out var length))
        {
            boxedInstance = Box<TElement?[]>.Empty;
            return false;
        }
        boxedInstance = new TElement[length];
        return true;
    }

    public override bool FillInstance(DeserializationContext context, Box<TElement?[]> boxedInstance)
    {
        if (_elementRegistration is null && TypeHelper<TElement>.IsSealed)
        {
            _elementRegistration = context.Fury.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        var instance = boxedInstance.Value!;
        for (; _index < instance.Length; _index++)
        {
            if (!context.Read(_elementRegistration, out instance[_index]))
            {
                return false;
            }
        }

        Reset();
        return true;
    }

    public override async ValueTask<Box<TElement?[]>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        var length = await context.GetReader().ReadCountAsync(cancellationToken);
        return new TElement?[length];
    }

    public override async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TElement?[]> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        var instance = boxedInstance.Value!;
        for (; _index < instance.Length; _index++)
        {
            instance[_index] = await context.ReadAsync<TElement>(cancellationToken);
        }

        Reset();
    }

    public override void Reset()
    {
        base.Reset();
        _index = 0;
    }
}

internal class NullableArrayDeserializer<TElement>(TypeRegistration? elementRegistration)
    : AbstractDeserializer<TElement?[]>
    where TElement : struct
{
    private TypeRegistration? _elementRegistration = elementRegistration;

    private int _index;

    [UsedImplicitly]
    public NullableArrayDeserializer()
        : this(null) { }

    public override bool CreateInstance(DeserializationContext context, ref Box<TElement?[]> boxedInstance)
    {
        if (!context.GetReader().TryReadCount(out var length))
        {
            boxedInstance = Box<TElement?[]>.Empty;
            return false;
        }
        boxedInstance = new TElement?[length];
        return true;
    }

    public override bool FillInstance(DeserializationContext context, Box<TElement?[]> boxedInstance)
    {
        if (_elementRegistration is null && TypeHelper<TElement>.IsSealed)
        {
            _elementRegistration = context.Fury.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        var instance = boxedInstance.Value!;
        for (; _index < instance.Length; _index++)
        {
            if (!context.Read(_elementRegistration, out instance[_index]))
            {
                return false;
            }
        }

        Reset();
        return true;
    }

    public override async ValueTask<Box<TElement?[]>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        var length = await context.GetReader().ReadCountAsync(cancellationToken);

        return new TElement?[length];
    }

    public override async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TElement?[]> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        var instance = boxedInstance.Value!;
        for (; _index < instance.Length; _index++)
        {
            instance[_index] = await context.ReadAsync<TElement>(cancellationToken);
        }

        Reset();
    }

    public override void Reset()
    {
        base.Reset();
        _index = 0;
    }
}

internal sealed class PrimitiveArraySerializer<TElement> : AbstractSerializer<TElement[]>
    where TElement : unmanaged
{
    private bool _hasWrittenCount;

    public override bool Write(SerializationContext context, in TElement[] value)
    {
        if (!_hasWrittenCount)
        {
            _hasWrittenCount = context.GetWriter().TryWriteCount(value.Length);
            if (!_hasWrittenCount)
            {
                return false;
            }
        }

        return context.GetWriter().TryWrite<TElement>(value);
    }
}

internal sealed class PrimitiveArrayDeserializer<TElement> : ArrayDeserializer<TElement>
    where TElement : unmanaged
{
    private int _index;

    public override bool FillInstance(DeserializationContext context, Box<TElement[]> boxedInstance)
    {
        var instance = boxedInstance.Value!;

        var readCount = context.GetReader().ReadMemory(instance.AsSpan(_index));

        if (readCount + _index <= instance.Length)
        {
            return false;
        }
        Reset();
        return true;
    }

    public override async ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<TElement[]> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        var instance = boxedInstance.Value!;
        await context.GetReader().ReadMemoryAsync<TElement>(instance, cancellationToken);
    }

    public override void Reset()
    {
        base.Reset();
        _index = 0;
    }
}
