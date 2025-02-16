using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;

namespace Fury;

internal static class BitHelper
{
    [Pure]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetBitMask32(int bitsCount) => (1 << bitsCount) - 1;

    [Pure]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long GetBitMask64(int bitsCount) => (1L << bitsCount) - 1;

    [Pure]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte ClearLowBits(byte value, int lowBitsCount) => (byte)(value & ~GetBitMask32(lowBitsCount));

    [Pure]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ClearLowBits(long value, int lowBitsCount) => value & ~GetBitMask64(lowBitsCount);

    [Pure]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong ClearLowBits(ulong value, int lowBitsCount) => value & ~(ulong)GetBitMask64(lowBitsCount);

    [Pure]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte ClearHighBits(byte value, int highBitsCount) => (byte)(value & GetBitMask32(8 - highBitsCount));

    [Pure]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte KeepLowBits(byte value, int lowBitsCount) => (byte)(value & GetBitMask32(lowBitsCount));

    [Pure]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong KeepLowBits(ulong value, int lowBitsCount) => value & (ulong)GetBitMask64(lowBitsCount);

    [Pure]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte KeepHighBits(byte value, int highBitsCount) => (byte)(value & ~GetBitMask32(8 - highBitsCount));

    [Pure]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte ReadBits(byte b1, int bitOffset, int bitCount)
    {
        return (byte)((b1 >>> (8 - bitCount - bitOffset)) & GetBitMask32(bitCount));
    }

    [Pure]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte ReadBits(byte b1, byte b2, int bitOffset, int bitCount)
    {
        var byteFromB1 = b1 << (bitOffset + bitCount - 8);
        var byteFromB2 = b2 >>> (8 * 2 - bitCount - bitOffset);
        return (byte)((byteFromB1 | byteFromB2) & GetBitMask32(bitCount));
    }
}
