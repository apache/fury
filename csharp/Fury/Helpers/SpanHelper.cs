using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace Fury;

internal static class SpanHelper
{
    public static Span<T> CreateSpan<T>(ref T reference, int length)
        where T : unmanaged
    {
#if NETSTANDARD2_0
        unsafe
        {
            fixed (T* p = &reference)
            {
                return new Span<T>(p, length);
            }
        }
#else
        return MemoryMarshal.CreateSpan(ref reference, length);
#endif
    }

    public static int CopyUpTo<T>(this ReadOnlySpan<T> source, Span<T> destination)
    {
        if (source.Length > destination.Length)
        {
            source = source.Slice(0, destination.Length);
        }

        source.CopyTo(destination);
        return source.Length;
    }

    public static int CopyUpTo<T>(this ReadOnlySequence<T> source, Span<T> destination)
    {
        var sourceLength = (int)source.Length;
        if (sourceLength > destination.Length)
        {
            source = source.Slice(0, destination.Length);
            sourceLength = destination.Length;
        }

        source.CopyTo(destination);
        return sourceLength;
    }
}
