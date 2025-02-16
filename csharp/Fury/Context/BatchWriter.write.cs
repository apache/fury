using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Fury.Context;

public ref partial struct BatchWriter
{
    public bool TryWrite<T>(T value)
        where T : unmanaged
    {
        var size = Unsafe.SizeOf<T>();
        var buffer = GetSpan(size);
        if (buffer.Length < size)
        {
            return false;
        }
#if NET8_0_OR_GREATER
        MemoryMarshal.Write(buffer, in value);
#else
        MemoryMarshal.Write(buffer, ref value);
#endif
        Advance(size);

        return true;
    }

    internal bool TryWrite<T>(T value, ref bool hasWritten)
        where T : unmanaged
    {
        if (!hasWritten)
        {
            hasWritten = TryWrite(value);
        }

        return hasWritten;
    }

    public bool TryWrite(ReadOnlySpan<byte> values)
    {
        var buffer = GetSpan(values.Length);
        if (buffer.Length < values.Length)
        {
            return false;
        }
        values.CopyTo(buffer);
        Advance(values.Length);
        return true;
    }

    internal bool TryWrite(ReadOnlySpan<byte> values, ref bool hasWritten)
    {
        if (!hasWritten)
        {
            hasWritten = TryWrite(values);
        }

        return hasWritten;
    }

    public bool TryWrite<TElement>(ReadOnlySpan<TElement> values)
        where TElement : unmanaged
    {
        return TryWrite(MemoryMarshal.AsBytes(values));
    }

    public unsafe void Write(ReadOnlySpan<char> value, Encoding encoding, int byteCountHint)
    {
        var buffer = GetSpan(byteCountHint);
        int actualByteCount;

        fixed (char* pChars = value)
        fixed (byte* pBytes = buffer)
        {
            actualByteCount = encoding.GetBytes(pChars, value.Length, pBytes, buffer.Length);
        }

        Advance(actualByteCount);
    }

    public unsafe void Write(ReadOnlySpan<char> value, Encoding encoding)
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

        Write(value, encoding, bufferLength);
    }

    public bool TryWrite7BitEncodedInt(int value)
    {
        var zigzag = BitOperations.RotateLeft((uint)value, 1);
        return TryWrite7BitEncodedUint(zigzag);
    }

    public bool TryWrite7BitEncodedUint(uint value)
    {
        Span<byte> buffer;
        switch (value)
        {
            case < 1u << 7:
                return TryWrite((byte)value);
            case < 1u << 14:
            {
                if (!TryGetSpan(2, out buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)(value >>> 7);
                Advance(2);
                return true;
            }
            case < 1u << 21:
            {
                if (!TryGetSpan(3, out buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >>> 7) | ~0x7Fu);
                buffer[2] = (byte)(value >>> 14);
                Advance(3);
                return true;
            }
            case < 1u << 28:
            {
                if (!TryGetSpan(4, out buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >>> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >>> 14) | ~0x7Fu);
                buffer[3] = (byte)(value >>> 21);
                Advance(4);
                return true;
            }
            default:
                if (!TryGetSpan(5, out buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >>> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >>> 14) | ~0x7Fu);
                buffer[3] = (byte)((value >>> 21) | ~0x7Fu);
                buffer[4] = (byte)(value >>> 28);
                Advance(5);
                return true;
        }
    }

    internal bool TryWrite7BitEncodedInt(int value, ref bool hasWritten)
    {
        if (!hasWritten)
        {
            hasWritten = TryWrite7BitEncodedInt(value);
        }

        return hasWritten;
    }

    internal bool TryWrite7BitEncodedUint(uint value, ref bool hasWritten)
    {
        if (!hasWritten)
        {
            hasWritten = TryWrite7BitEncodedUint(value);
        }

        return hasWritten;
    }

    public bool TryWrite7BitEncodedLong(long value)
    {
        var zigzag = BitOperations.RotateLeft((ulong)value, 1);
        return TryWrite7BitEncodedUlong(zigzag);
    }

    public bool TryWrite7BitEncodedUlong(ulong value)
    {
        switch (value)
        {
            case < 1ul << 7:
                return TryWrite((byte)value);
            case < 1ul << 14:
            {
                if (!TryGetSpan(2, out var buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)(value >>> 7);
                Advance(2);
                return true;
            }
            case < 1ul << 21:
            {
                if (!TryGetSpan(3, out var buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >>> 7) | ~0x7Fu);
                buffer[2] = (byte)(value >>> 14);
                Advance(3);
                return true;
            }
            case < 1ul << 28:
            {
                if (!TryGetSpan(4, out var buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >>> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >>> 14) | ~0x7Fu);
                buffer[3] = (byte)(value >>> 21);
                Advance(4);
                return true;
            }
            case < 1ul << 35:
            {
                if (!TryGetSpan(5, out var buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >>> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >>> 14) | ~0x7Fu);
                buffer[3] = (byte)((value >>> 21) | ~0x7Fu);
                buffer[4] = (byte)(value >>> 28);
                Advance(5);
                return true;
            }
            case < 1ul << 42:
            {
                if (!TryGetSpan(6, out var buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >>> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >>> 14) | ~0x7Fu);
                buffer[3] = (byte)((value >>> 21) | ~0x7Fu);
                buffer[4] = (byte)((value >>> 28) | ~0x7Fu);
                buffer[5] = (byte)(value >>> 35);
                Advance(6);
                return true;
            }
            case < 1ul << 49:
            {
                if (!TryGetSpan(7, out var buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >>> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >>> 14) | ~0x7Fu);
                buffer[3] = (byte)((value >>> 21) | ~0x7Fu);
                buffer[4] = (byte)((value >>> 28) | ~0x7Fu);
                buffer[5] = (byte)((value >>> 35) | ~0x7Fu);
                buffer[6] = (byte)(value >>> 42);
                Advance(7);
                return true;
            }
            case < 1ul << 56:
            {
                if (!TryGetSpan(8, out var buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >>> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >>> 14) | ~0x7Fu);
                buffer[3] = (byte)((value >>> 21) | ~0x7Fu);
                buffer[4] = (byte)((value >>> 28) | ~0x7Fu);
                buffer[5] = (byte)((value >>> 35) | ~0x7Fu);
                buffer[6] = (byte)((value >>> 42) | ~0x7Fu);
                buffer[7] = (byte)(value >>> 49);
                Advance(8);
                return true;
            }
            case < 1ul << 63:
            {
                if (!TryGetSpan(9, out var buffer))
                {
                    return false;
                }
                buffer[0] = (byte)(value | ~0x7Fu);
                buffer[1] = (byte)((value >>> 7) | ~0x7Fu);
                buffer[2] = (byte)((value >>> 14) | ~0x7Fu);
                buffer[3] = (byte)((value >>> 21) | ~0x7Fu);
                buffer[4] = (byte)((value >>> 28) | ~0x7Fu);
                buffer[5] = (byte)((value >>> 35) | ~0x7Fu);
                buffer[6] = (byte)((value >>> 42) | ~0x7Fu);
                buffer[7] = (byte)((value >>> 49) | ~0x7Fu);
                buffer[8] = (byte)(value >>> 56);
                Advance(9);
                return true;
            }
            default:
                ThrowHelper.ThrowUnreachableException();
                return false;
        }
    }

    internal bool TryWrite7BitEncodedLong(long value, ref bool hasWritten)
    {
        if (!hasWritten)
        {
            hasWritten = TryWrite7BitEncodedLong(value);
        }

        return hasWritten;
    }

    internal bool TryWrite7BitEncodedUlong(ulong value, ref bool hasWritten)
    {
        if (!hasWritten)
        {
            hasWritten = TryWrite7BitEncodedUlong(value);
        }

        return hasWritten;
    }

    // Specialized write methods

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryWriteCount(int length)
    {
        return TryWrite7BitEncodedUint((uint)length);
    }
}
