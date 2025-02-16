#if !NET8_0_OR_GREATER
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

// Copied and modified from System.Buffers.SequenceReader<T> in dotnet/runtime

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Fury;

// ReSharper disable InconsistentNaming

// ReSharper disable once CheckNamespace
namespace System.Buffers;

public ref struct SequenceReader<T>
    where T : unmanaged, IEquatable<T>
{
    private bool _moreData;
    private readonly long _length;

    private ReadOnlySequence<T>.Enumerator _enumerator;

    /// <summary>
    /// Create a <see cref="SequenceReader{T}"/> over the given <see cref="ReadOnlySequence{T}"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SequenceReader(ReadOnlySequence<T> sequence)
    {
        CurrentSpanIndex = 0;
        Consumed = 0;
        Sequence = sequence;
        _length = -1;

        var first = sequence.First;
        CurrentSpan = first.Span;
        _moreData = first.Length > 0;

        _enumerator = sequence.GetEnumerator();

        if (!_moreData && !sequence.IsSingleSegment)
        {
            _moreData = true;
            GetNextSpan();
        }
    }

    /// <summary>
    /// True when there is no more data in the <see cref="Sequence"/>.
    /// </summary>
    public readonly bool End => !_moreData;

    /// <summary>
    /// The underlying <see cref="ReadOnlySequence{T}"/> for the reader.
    /// </summary>
    public ReadOnlySequence<T> Sequence { get; }

    /// <summary>
    /// The current segment in the <see cref="Sequence"/> as a span.
    /// </summary>
    public ReadOnlySpan<T> CurrentSpan { get; private set; }

    /// <summary>
    /// The index in the <see cref="CurrentSpan"/>.
    /// </summary>
    public int CurrentSpanIndex { get; private set; }

    /// <summary>
    /// The unread portion of the <see cref="CurrentSpan"/>.
    /// </summary>
    public readonly ReadOnlySpan<T> UnreadSpan
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => CurrentSpan.Slice(CurrentSpanIndex);
    }

    /// <summary>
    /// The total number of <typeparamref name="T"/>'s processed by the reader.
    /// </summary>
    public long Consumed { get; private set; }

    /// <summary>
    /// Remaining <typeparamref name="T"/>'s in the reader's <see cref="Sequence"/>.
    /// </summary>
    public readonly long Remaining => Length - Consumed;

    /// <summary>
    /// Count of <typeparamref name="T"/> in the reader's <see cref="Sequence"/>.
    /// </summary>
    public readonly long Length
    {
        get
        {
            if (_length < 0)
            {
                // Cast-away readonly to initialize lazy field
                Unsafe.AsRef(in _length) = Sequence.Length;
            }
            return _length;
        }
    }

    /// <summary>
    /// Get the next segment with available data, if any.
    /// </summary>
    private void GetNextSpan()
    {
        if (!Sequence.IsSingleSegment)
        {
            while (_enumerator.MoveNext())
            {
                var memory = _enumerator.Current;
                if (memory.Length > 0)
                {
                    CurrentSpan = memory.Span;
                    CurrentSpanIndex = 0;
                    return;
                }

                CurrentSpan = default;
                CurrentSpanIndex = 0;
            }
        }
        _moreData = false;
    }

    /// <summary>
    /// Move the reader ahead the specified number of items.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(long count)
    {
        const long TooBigOrNegative = unchecked((long)0xFFFFFFFF80000000);
        if ((count & TooBigOrNegative) == 0 && CurrentSpan.Length - CurrentSpanIndex > (int)count)
        {
            CurrentSpanIndex += (int)count;
            Consumed += count;
        }
        else
        {
            // Can't satisfy from the current span
            AdvanceToNextSpan(count);
        }
    }

    private void AdvanceToNextSpan(long count)
    {
        if (count < 0)
        {
            ThrowHelper.ThrowArgumentOutOfRangeException(nameof(count));
        }

        Consumed += count;
        while (_moreData)
        {
            int remaining = CurrentSpan.Length - CurrentSpanIndex;

            if (remaining > count)
            {
                CurrentSpanIndex += (int)count;
                count = 0;
                break;
            }

            // As there may not be any further segments we need to
            // push the current index to the end of the span.
            CurrentSpanIndex += remaining;
            count -= remaining;
            Debug.Assert(count >= 0);

            GetNextSpan();

            if (count == 0)
            {
                break;
            }
        }

        if (count != 0)
        {
            // Not enough data left- adjust for where we actually ended and throw
            Consumed -= count;
            ThrowHelper.ThrowArgumentOutOfRangeException(nameof(count));
        }
    }
}
#endif
