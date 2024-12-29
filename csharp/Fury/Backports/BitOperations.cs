#if !NET8_0_OR_GREATER
using System.Runtime.CompilerServices;

// ReSharper disable once CheckNamespace
namespace System.Numerics;

internal static class BitOperations
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong RotateLeft(ulong value, int offset) => (value << offset) | (value >> (64 - offset));
}
#endif
