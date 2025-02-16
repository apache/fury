using System;
using System.Collections;
using System.Collections.Generic;
using JetBrains.Annotations;
#if NET8_0_OR_GREATER
using System.Collections.Immutable;
using System.Runtime.InteropServices;
#endif

namespace Fury.Collections;

internal static class EnumerableExtensions
{
    public static bool TryGetSpan<T>([NoEnumeration] this IEnumerable<T> enumerable, out Span<T> span)
    {
        switch (enumerable)
        {
            case T[] elements:
                span = elements;
                return true;
#if NET8_0_OR_GREATER
            case List<T> elements:
                span = CollectionsMarshal.AsSpan(elements);
                return true;
            case ImmutableArray<T> elements:
                span = ImmutableCollectionsMarshal.AsArray(elements);
                return true;
#endif
            case PooledList<T> elements:
                span = elements.AsSpan();
                return true;
            default:
                span = Span<T>.Empty;
                return false;
        }
    }
}
