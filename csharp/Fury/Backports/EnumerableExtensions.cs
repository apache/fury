#if !NET8_0_OR_GREATER
using System.Collections;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace Fury;

internal static class EnumerableExtensions
{
    public static bool TryGetNonEnumeratedCount<T>([NoEnumeration] this IEnumerable<T> enumerable, out int count)
    {
        switch (enumerable)
        {
            case ICollection<T> typedCollection:
                count = typedCollection.Count;
                return true;
            case ICollection collection:
                count = collection.Count;
                return true;
            case IReadOnlyCollection<T> readOnlyCollection:
                count = readOnlyCollection.Count;
                return true;
            default:
                count = 0;
                return false;
        }
    }
}
#endif
