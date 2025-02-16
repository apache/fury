using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Fury.Collections;
using Fury.Context;
using JetBrains.Annotations;

namespace Fury.Serialization;

[PublicAPI]
public class EnumerableSerializer<TElement, TEnumerable>(TypeRegistration? elementRegistration)
    : AbstractSerializer<TEnumerable>
    where TElement : notnull
    where TEnumerable : IEnumerable<TElement>
{
    private TypeRegistration? _elementRegistration = elementRegistration;

    private int _index;
    private IEnumerator<TElement>? _enumerator;

    private bool _hasWrittenCount;
    private bool _hasFilledElements;
    private readonly PooledList<TElement> _elements = [];

    [UsedImplicitly]
    public EnumerableSerializer()
        : this(null) { }

    public override bool Write(SerializationContext context, in TEnumerable value)
    {
        if (_elementRegistration is null && TypeHelper<TElement>.IsSealed)
        {
            _elementRegistration = context.Fury.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        // fast path
        var written = WriteFast(context, in value, out var completed);
        if (!written)
        {
            if (value.TryGetNonEnumeratedCount(out var count))
            {
                // slow path
                // use the enumerator of input value to write elements
                WriteSlow(context, count, in value, out completed);
            }
            else
            {
                // slow path
                // copy elements to a list and use the list to write elements
                // to avoid multiple enumerations of the input value
                if (!_hasFilledElements)
                {
                    _elements.AddRange(value);
                    _hasFilledElements = true;
                }

                written = WriteFast(context, in _elements, out completed);

                Debug.Assert(written);
            }
        }

        if (completed)
        {
            Reset();
        }
        return completed;
    }

    private bool TryWriteCount(BatchWriter writer, int count)
    {
        if (!_hasWrittenCount)
        {
            if (!writer.TryWriteCount(count))
            {
                return false;
            }

            _hasWrittenCount = true;
        }

        return true;
    }

    private bool WriteFast<T>(SerializationContext context, in T value, out bool writeCompleted)
        where T : IEnumerable<TElement>
    {
        if (TryGetSpan(value, out var span))
        {
            if (!TryWriteCount(context.GetWriter(), span.Length))
            {
                writeCompleted = false;
                return true;
            }

            if (span.Length == 0)
            {
                writeCompleted = true;
                return true;
            }

            for (; _index < span.Length; _index++)
            {
                if (!context.Write(in span[_index], _elementRegistration))
                {
                    writeCompleted = false;
                    return true;
                }
            }

            writeCompleted = true;
            return true;
        }

        writeCompleted = false;
        return false;
    }

    private void WriteSlow(SerializationContext context, int count, in TEnumerable value, out bool writeCompleted)
    {
        if (!TryWriteCount(context.GetWriter(), count))
        {
            writeCompleted = false;
            return;
        }

        if (count == 0)
        {
            writeCompleted = true;
            return;
        }

        writeCompleted = true;
        var noElements = false;
        if (_enumerator is null)
        {
            _enumerator = value.GetEnumerator();
            if (!_enumerator.MoveNext())
            {
                noElements = true;
            }
        }

        if (!noElements)
        {
            do
            {
                if (!context.Write(_enumerator.Current, _elementRegistration))
                {
                    writeCompleted = false;
                    break;
                }
            } while (_enumerator.MoveNext());
        }

        _enumerator.Dispose();
        _enumerator = null;
    }

    protected virtual bool TryGetSpan<T>(in T value, out ReadOnlySpan<TElement> span)
        where T : IEnumerable<TElement>
    {
        var success = value.TryGetSpan(out var elements);
        span = elements;
        return success;
    }

    public override void Reset()
    {
        base.Reset();
        _index = 0;
        _enumerator?.Dispose();
        _enumerator = null;
        _hasWrittenCount = false;
        _hasFilledElements = false;
        _elements.Clear();
    }
}

[PublicAPI]
public class NullableEnumerableSerializer<TElement, TEnumerable>(TypeRegistration? elementRegistration)
    : AbstractSerializer<TEnumerable>
    where TElement : struct
    where TEnumerable : IEnumerable<TElement?>
{
    private TypeRegistration? _elementRegistration = elementRegistration;

    private int _index;
    private IEnumerator<TElement?>? _enumerator;

    private bool _hasWrittenCount;
    private bool _hasFilledElements;
    private readonly PooledList<TElement?> _elements = [];

    [UsedImplicitly]
    public NullableEnumerableSerializer()
        : this(null) { }

    public override bool Write(SerializationContext context, in TEnumerable value)
    {
        if (_elementRegistration is null && TypeHelper<TElement>.IsSealed)
        {
            _elementRegistration = context.Fury.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        // fast path
        var written = WriteFast(context, in value, out var completed);
        if (!written)
        {
            if (value.TryGetNonEnumeratedCount(out var count))
            {
                // slow path
                // use the enumerator of input value to write elements
                WriteSlow(context, count, in value, out completed);
            }
            else
            {
                // slow path
                // copy elements to a list and use the list to write elements
                // to avoid multiple enumerations of the input value
                if (!_hasFilledElements)
                {
                    _elements.AddRange(value);
                    _hasFilledElements = true;
                }

                written = WriteFast(context, in _elements, out completed);

                Debug.Assert(written);
            }
        }

        if (completed)
        {
            Reset();
        }
        return completed;
    }

    private bool TryWriteCount(BatchWriter writer, int count)
    {
        if (!_hasWrittenCount)
        {
            if (!writer.TryWriteCount(count))
            {
                return false;
            }

            _hasWrittenCount = true;
        }

        return true;
    }

    private bool WriteFast<T>(SerializationContext context, in T value, out bool writeCompleted)
        where T : IEnumerable<TElement?>
    {
        if (TryGetSpan(value, out var span))
        {
            if (!TryWriteCount(context.GetWriter(), span.Length))
            {
                writeCompleted = false;
                return true;
            }

            if (span.Length == 0)
            {
                writeCompleted = true;
                return true;
            }

            for (; _index < span.Length; _index++)
            {
                if (!context.Write(in span[_index], _elementRegistration))
                {
                    writeCompleted = false;
                    return true;
                }
            }

            writeCompleted = true;
            return true;
        }

        writeCompleted = false;
        return false;
    }

    private void WriteSlow(SerializationContext context, int count, in TEnumerable value, out bool writeCompleted)
    {
        if (!TryWriteCount(context.GetWriter(), count))
        {
            writeCompleted = false;
            return;
        }

        if (count == 0)
        {
            writeCompleted = true;
            return;
        }

        writeCompleted = true;
        var noElements = false;
        if (_enumerator is null)
        {
            _enumerator = value.GetEnumerator();
            if (!_enumerator.MoveNext())
            {
                noElements = true;
            }
        }

        if (!noElements)
        {
            do
            {
                if (!context.Write(_enumerator.Current, _elementRegistration))
                {
                    writeCompleted = false;
                    break;
                }
            } while (_enumerator.MoveNext());
        }

        _enumerator.Dispose();
        _enumerator = null;
    }

    protected virtual bool TryGetSpan<T>(in T value, out ReadOnlySpan<TElement?> span)
        where T : IEnumerable<TElement?>
    {
        var success = value.TryGetSpan(out var elements);
        span = elements;
        return success;
    }

    public override void Reset()
    {
        base.Reset();
        _index = 0;
        _enumerator?.Dispose();
        _enumerator = null;
        _hasWrittenCount = false;
        _hasFilledElements = false;
        _elements.Clear();
    }
}
