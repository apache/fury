using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Fury;

public static class BatchWriterExtensions
{
    public static void Write<T>(ref this BatchWriter writer, T value)
        where T : unmanaged
    {
        var size = Unsafe.SizeOf<T>();
        var buffer = writer.GetSpan(size);
#if NET8_0_OR_GREATER
        MemoryMarshal.Write(buffer, in value);
#else
        MemoryMarshal.Write(buffer, ref value);
#endif
        writer.Advance(size);
    }

    public static void Write(ref this BatchWriter writer, Span<byte> values)
    {
        var buffer = writer.GetSpan(values.Length);
        values.CopyTo(buffer);
        writer.Advance(values.Length);
    }

    public static void Write<TElement>(ref this BatchWriter writer, Span<TElement> values)
        where TElement : unmanaged
    {
        writer.Write(MemoryMarshal.AsBytes(values));
    }

    public static unsafe void Write(
        ref this BatchWriter writer,
        ReadOnlySpan<char> value,
        Encoding encoding,
        int byteCountHint
    )
    {
        var buffer = writer.GetSpan(byteCountHint);
        int actualByteCount;

        fixed (char* pChars = value)
        fixed (byte* pBytes = buffer)
        {
            actualByteCount = encoding.GetBytes(pChars, value.Length, pBytes, buffer.Length);
        }

        writer.Advance(actualByteCount);
    }

    public static unsafe void Write(ref this BatchWriter writer, ReadOnlySpan<char> value, Encoding encoding)
    {
        const int fastPathBufferSize = 128;

        var possibleMaxByteCount = encoding.GetMaxByteCount(value.Length);
        int bufferLength;
        if (possibleMaxByteCount <= fastPathBufferSize)
        {
            bufferLength = possibleMaxByteCount;
        }
        else
        {
            fixed (char* pChars = value)
            {
                bufferLength = encoding.GetByteCount(pChars, value.Length);
            }
        }

        writer.Write(value, encoding, bufferLength);
    }

    public static void Write7BitEncodedInt(ref this BatchWriter writer, int value)
    {
        var zigzag = (uint)((value << 1) ^ (value >> 31));
        writer.Write7BitEncodedUint(zigzag);
    }

    public static void Write7BitEncodedUint(ref this BatchWriter writer, uint value)
    {
        switch (value)
        {
            case < 1u << 7:
                writer.Write((byte)value);
                return;
            case < 1u << 14:
            {
                var buffer = writer.GetSpan(2);
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)(value >> 7);
                writer.Advance(2);
                break;
            }
            case < 1u << 21:
            {
                var buffer = writer.GetSpan(3);
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >> 7) | ~0x7Fu);
                buffer[2] = (byte)(value >> 14);
                writer.Advance(3);
                break;
            }
            case < 1u << 28:
            {
                var buffer = writer.GetSpan(4);
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >> 14) | ~0x7Fu);
                buffer[3] = (byte)(value >> 21);
                writer.Advance(4);
                break;
            }
            default:
                var buffer2 = writer.GetSpan(5);
                buffer2[0] = (byte)(value | ~0x7Fu);
                buffer2[1] = (byte)((value >> 7) | ~0x7Fu);
                buffer2[2] = (byte)((value >> 14) | ~0x7Fu);
                buffer2[3] = (byte)((value >> 21) | ~0x7Fu);
                buffer2[4] = (byte)(value >> 28);
                writer.Advance(5);
                break;
        }
    }

    public static void Write7BitEncodedLong(ref this BatchWriter writer, long value)
    {
        var zigzag = (ulong)((value << 1) ^ (value >> 63));
        writer.Write7BitEncodedUlong(zigzag);
    }

    public static void Write7BitEncodedUlong(ref this BatchWriter writer, ulong value)
    {
        switch (value)
        {
            case < 1ul << 7:
                writer.Write((byte)value);
                return;
            case < 1ul << 14:
            {
                var buffer = writer.GetSpan(2);
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)(value >> 7);
                writer.Advance(2);
                break;
            }
            case < 1ul << 21:
            {
                var buffer = writer.GetSpan(3);
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >> 7) | ~0x7Fu);
                buffer[2] = (byte)(value >> 14);
                writer.Advance(3);
                break;
            }
            case < 1ul << 28:
            {
                var buffer = writer.GetSpan(4);
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >> 14) | ~0x7Fu);
                buffer[3] = (byte)(value >> 21);
                writer.Advance(4);
                break;
            }
            case < 1ul << 35:
            {
                var buffer = writer.GetSpan(5);
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >> 14) | ~0x7Fu);
                buffer[3] = (byte)((value >> 21) | ~0x7Fu);
                buffer[4] = (byte)(value >> 28);
                writer.Advance(5);
                break;
            }
            case < 1ul << 42:
            {
                var buffer = writer.GetSpan(6);
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >> 14) | ~0x7Fu);
                buffer[3] = (byte)((value >> 21) | ~0x7Fu);
                buffer[4] = (byte)((value >> 28) | ~0x7Fu);
                buffer[5] = (byte)(value >> 35);
                writer.Advance(6);
                break;
            }
            case < 1ul << 49:
            {
                var buffer = writer.GetSpan(7);
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >> 14) | ~0x7Fu);
                buffer[3] = (byte)((value >> 21) | ~0x7Fu);
                buffer[4] = (byte)((value >> 28) | ~0x7Fu);
                buffer[5] = (byte)((value >> 35) | ~0x7Fu);
                buffer[6] = (byte)(value >> 42);
                writer.Advance(7);
                break;
            }
            case < 1ul << 56:
            {
                var buffer = writer.GetSpan(8);
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >> 14) | ~0x7Fu);
                buffer[3] = (byte)((value >> 21) | ~0x7Fu);
                buffer[4] = (byte)((value >> 28) | ~0x7Fu);
                buffer[5] = (byte)((value >> 35) | ~0x7Fu);
                buffer[6] = (byte)((value >> 42) | ~0x7Fu);
                buffer[7] = (byte)(value >> 49);
                writer.Advance(8);
                break;
            }
            case < 1ul << 63:
            {
                var buffer = writer.GetSpan(9);
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >> 14) | ~0x7Fu);
                buffer[3] = (byte)((value >> 21) | ~0x7Fu);
                buffer[4] = (byte)((value >> 28) | ~0x7Fu);
                buffer[5] = (byte)((value >> 35) | ~0x7Fu);
                buffer[6] = (byte)((value >> 42) | ~0x7Fu);
                buffer[7] = (byte)((value >> 49) | ~0x7Fu);
                buffer[8] = (byte)(value >> 56);
                writer.Advance(9);
                break;
            }
        }
    }

    public static void WriteCount(ref this BatchWriter writer, int length)
    {
        writer.Write7BitEncodedUint((uint)length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Write(ref this BatchWriter writer, ReferenceFlag flag)
    {
        writer.Write((sbyte)flag);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Write(ref this BatchWriter writer, RefId refId)
    {
        writer.Write7BitEncodedUint((uint)refId.Value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Write(ref this BatchWriter writer, TypeId typeId)
    {
        writer.Write7BitEncodedUint((uint)typeId.Value);
    }
}
