using System;
using System.Buffers;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Fury;

public static class BatchReaderExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe TValue ReadFixedSized<TValue>(ReadOnlySequence<byte> buffer)
        where TValue : unmanaged
    {
        var size = Unsafe.SizeOf<TValue>();
        TValue result;
        buffer.Slice(0, size).CopyTo(new Span<byte>(&result, size));
        return result;
    }

    public static async ValueTask<T> ReadAsync<T>(
        this BatchReader reader,
        CancellationToken cancellationToken = default
    )
        where T : unmanaged
    {
        var requiredSize = Unsafe.SizeOf<T>();
        var result = await reader.ReadAtLeastAsync(requiredSize, cancellationToken);
        var buffer = result.Buffer;
        if (buffer.Length < requiredSize)
        {
            ThrowHelper.ThrowBadSerializationDataException(ExceptionMessages.InsufficientData());
        }

        var value = ReadFixedSized<T>(buffer);
        reader.AdvanceTo(requiredSize);
        return value;
    }

    public static async ValueTask ReadMemoryAsync<TElement>(
        this BatchReader reader,
        Memory<TElement> destination,
        CancellationToken cancellationToken = default
    )
        where TElement : unmanaged
    {
        var requiredSize = destination.Length;
        var result = await reader.ReadAtLeastAsync(requiredSize, cancellationToken);
        var buffer = result.Buffer;
        if (result.IsCompleted && buffer.Length < requiredSize)
        {
            ThrowHelper.ThrowBadSerializationDataException(ExceptionMessages.InsufficientData());
        }

        buffer.Slice(0, requiredSize).CopyTo(MemoryMarshal.AsBytes(destination.Span));
        reader.AdvanceTo(requiredSize);
    }

    public static async ValueTask<string> ReadStringAsync(
        this BatchReader reader,
        int byteCount,
        Encoding encoding,
        CancellationToken cancellationToken = default
    )
    {
        var result = await reader.ReadAtLeastAsync(byteCount, cancellationToken);
        var buffer = result.Buffer;
        if (result.IsCompleted && buffer.Length < byteCount)
        {
            ThrowHelper.ThrowBadSerializationDataException(ExceptionMessages.InsufficientData());
        }

        var value = DoReadString(byteCount, buffer, encoding);
        reader.AdvanceTo(byteCount);
        return value;
    }

    private static unsafe string DoReadString(int byteCount, ReadOnlySequence<byte> byteSequence, Encoding encoding)
    {
        const int maxStackBufferSize = StaticConfigs.StackAllocLimit / sizeof(char);
        var decoder = encoding.GetDecoder();
        int writtenChars;
        string result;
        if (byteCount < maxStackBufferSize)
        {
            // Fast path
            Span<char> stringBuffer = stackalloc char[byteCount];
            writtenChars = ReadStringCommon(decoder, byteSequence, stringBuffer);
            fixed (char* pChars = stringBuffer)
            {
                result = new string(pChars, 0, writtenChars);
            }
        }
        else
        {
            var rentedBuffer = ArrayPool<char>.Shared.Rent(byteCount);
            writtenChars = ReadStringCommon(decoder, byteSequence, rentedBuffer);
            result = new string(rentedBuffer, 0, writtenChars);
            ArrayPool<char>.Shared.Return(rentedBuffer);
        }

        return result;
    }

    private static unsafe int ReadStringCommon(
        Decoder decoder,
        ReadOnlySequence<byte> byteSequence,
        Span<char> unwrittenBuffer
    )
    {
        var writtenChars = 0;
        foreach (var byteMemory in byteSequence)
        {
            int charsUsed;
            var byteSpan = byteMemory.Span;
            fixed (char* pUnWrittenBuffer = unwrittenBuffer)
            fixed (byte* pBytes = byteMemory.Span)
            {
                decoder.Convert(
                    pBytes,
                    byteSpan.Length,
                    pUnWrittenBuffer,
                    unwrittenBuffer.Length,
                    false,
                    out _,
                    out charsUsed,
                    out _
                );
            }

            unwrittenBuffer = unwrittenBuffer.Slice(charsUsed);
            writtenChars += charsUsed;
        }

        return writtenChars;
    }

    public static async ValueTask<int> Read7BitEncodedIntAsync(
        this BatchReader reader,
        CancellationToken cancellationToken = default
    )
    {
        var result = await Read7BitEncodedUintAsync(reader, cancellationToken);
        return (int)((result >> 1) | (result << 31));
    }

    public static async ValueTask<uint> Read7BitEncodedUintAsync(
        this BatchReader reader,
        CancellationToken cancellationToken = default
    )
    {
        var result = await reader.ReadAtLeastAsync(MaxBytesOfVarInt32WithoutOverflow + 1, cancellationToken);
        var buffer = result.Buffer;

        // Fast path
        var value = DoRead7BitEncodedUintFast(buffer.First.Span, out var consumed);
        if (consumed == 0)
        {
            // Slow path
            value = DoRead7BitEncodedUintSlow(buffer, out consumed);
        }

        reader.AdvanceTo(consumed);

        return value;
    }

    private const int MaxBytesOfVarInt32WithoutOverflow = 4;

    private static uint DoRead7BitEncodedUintFast(ReadOnlySpan<byte> buffer, out int consumed)
    {
        if (buffer.Length <= MaxBytesOfVarInt32WithoutOverflow)
        {
            consumed = 0;
            return 0;
        }
        uint result = 0;
        consumed = 0;
        uint readByte;
        for (var i = 0; i < MaxBytesOfVarInt32WithoutOverflow; i++)
        {
            readByte = buffer[i];
            result |= (readByte & 0x7F) << (i * 7);
            if ((readByte & 0x80) == 0)
            {
                consumed = i + 1;
                return result;
            }
        }

        readByte = buffer[MaxBytesOfVarInt32WithoutOverflow];
        if (readByte > 0b_1111u)
        {
            ThrowHelper.ThrowBadSerializationDataException(ExceptionMessages.VarInt32Overflow());
        }

        result |= readByte << (MaxBytesOfVarInt32WithoutOverflow * 7);
        consumed = MaxBytesOfVarInt32WithoutOverflow + 1;
        return result;
    }

    private static uint DoRead7BitEncodedUintSlow(ReadOnlySequence<byte> buffer, out int consumed)
    {
        uint result = 0;
        var consumedBytes = 0;
        foreach (var memory in buffer)
        {
            var span = memory.Span;
            foreach (uint readByte in span)
            {
                if (consumedBytes < MaxBytesOfVarInt32WithoutOverflow)
                {
                    result |= (readByte & 0x7F) << (7 * consumedBytes);
                    ++consumedBytes;
                    if ((readByte & 0x80) == 0)
                    {
                        consumed = consumedBytes;
                        return result;
                    }
                }
                else
                {
                    if (readByte > 0b_1111u)
                    {
                        ThrowHelper.ThrowBadSerializationDataException(ExceptionMessages.VarInt32Overflow());
                    }
                    result |= readByte << (7 * MaxBytesOfVarInt32WithoutOverflow);
                    consumed = consumedBytes + 1;
                    return result;
                }
            }
        }
        consumed = 0;
        return result;
    }

    public static async ValueTask<long> Read7BitEncodedLongAsync(
        this BatchReader reader,
        CancellationToken cancellationToken = default
    )
    {
        var result = await Read7BitEncodedUlongAsync(reader, cancellationToken);
        return (long)((result >> 1) | (result << 63));
    }

    public static async ValueTask<ulong> Read7BitEncodedUlongAsync(
        this BatchReader reader,
        CancellationToken cancellationToken = default
    )
    {
        var result = await reader.ReadAtLeastAsync(MaxBytesOfVarInt64WithoutOverflow + 1, cancellationToken);
        var buffer = result.Buffer;

        // Fast path
        var value = DoRead7BitEncodedUlongFast(buffer.First.Span, out var consumed);
        if (consumed == 0)
        {
            // Slow path
            value = DoRead7BitEncodedUlongSlow(buffer, out consumed);
        }

        reader.AdvanceTo(consumed);

        return value;
    }

    private const int MaxBytesOfVarInt64WithoutOverflow = 8;

    private static ulong DoRead7BitEncodedUlongFast(ReadOnlySpan<byte> buffer, out int consumed)
    {
        if (buffer.Length <= MaxBytesOfVarInt64WithoutOverflow)
        {
            consumed = 0;
            return 0;
        }
        ulong result = 0;
        consumed = 0;
        ulong readByte;
        for (var i = 0; i < MaxBytesOfVarInt64WithoutOverflow; i++)
        {
            readByte = buffer[i];
            result |= (readByte & 0x7F) << (i * 7);
            if ((readByte & 0x80) == 0)
            {
                consumed = i + 1;
                return result;
            }
        }

        readByte = buffer[MaxBytesOfVarInt64WithoutOverflow];
        result |= readByte << (MaxBytesOfVarInt64WithoutOverflow * 7);
        return result;
    }

    private static ulong DoRead7BitEncodedUlongSlow(ReadOnlySequence<byte> buffer, out int consumed)
    {
        ulong result = 0;
        var consumedBytes = 0;
        foreach (var memory in buffer)
        {
            var span = memory.Span;
            foreach (ulong readByte in span)
            {
                if (consumedBytes < MaxBytesOfVarInt64WithoutOverflow)
                {
                    result |= (readByte & 0x7F) << (7 * consumedBytes);
                    ++consumedBytes;
                    if ((readByte & 0x80) == 0)
                    {
                        consumed = consumedBytes;
                        return result;
                    }
                }
                else
                {
                    result |= readByte << (7 * MaxBytesOfVarInt64WithoutOverflow);
                    consumed = consumedBytes + 1;
                    return result;
                }
            }
        }
        consumed = 0;
        return result;
    }

    public static async ValueTask<int> ReadCountAsync(this BatchReader reader, CancellationToken cancellationToken)
    {
        return (int)await reader.Read7BitEncodedUintAsync(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static async ValueTask<ReferenceFlag> ReadReferenceFlagAsync(
        this BatchReader reader,
        CancellationToken cancellationToken = default
    )
    {
        return (ReferenceFlag)await reader.ReadAsync<sbyte>(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static async ValueTask<TypeId> ReadTypeIdAsync(
        this BatchReader reader,
        CancellationToken cancellationToken = default
    )
    {
        return new TypeId((int)await reader.Read7BitEncodedUintAsync(cancellationToken));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static async ValueTask<RefId> ReadRefIdAsync(
        this BatchReader reader,
        CancellationToken cancellationToken = default
    )
    {
        return new RefId((int)await reader.Read7BitEncodedUintAsync(cancellationToken));
    }
}
