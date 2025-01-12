using System.Runtime.CompilerServices;

namespace Fury;

internal static class BitUtility
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetBitMask(int bitsCount) => (1 << bitsCount) - 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte ClearLowBits(byte value, int lowBitsCount) => (byte)(value & ~GetBitMask(lowBitsCount));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte ClearHighBits(byte value, int highBitsCount) => (byte)(value & GetBitMask(8 - highBitsCount));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte KeepLowBits(byte value, int lowBitsCount) => (byte)(value & GetBitMask(lowBitsCount));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte KeepHighBits(byte value, int highBitsCount) => (byte)(value & ~GetBitMask(8 - highBitsCount));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte ReadBits(byte b1, int bitOffset, int bitCount)
    {
        return (byte)((b1 >>> (8 - bitCount - bitOffset)) & BitUtility.GetBitMask(bitCount));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte ReadBits(byte b1, byte b2, int bitOffset, int bitCount)
    {
        var byteFromB1 = b1 << (bitOffset + bitCount - 8);
        var byteFromB2 = b2 >>> (8 * 2 - bitCount - bitOffset);
        return (byte)((byteFromB1 | byteFromB2) & BitUtility.GetBitMask(bitCount));
    }
}
