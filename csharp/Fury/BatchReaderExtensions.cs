using System;
using System.Buffers;
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
        var result = await reader.ReadAtLeastAsync(5, cancellationToken);
        var buffer = result.Buffer;

        // Fast path
        var consumed = Read7BitEncodedIntFast(buffer.First.Span, out var value);
        if (consumed == 0)
        {
            // Slow path
            consumed = Read7BitEncodedIntSlow(buffer, out value);
        }

        reader.AdvanceTo(consumed);

        return (int)value;
    }

    private const int MaxBytesOfVarInt32WithoutOverflow = 4;
    private const int MaxBytesOfVarInt32 = MaxBytesOfVarInt32WithoutOverflow + 1;

    private static int Read7BitEncodedIntFast(ReadOnlySpan<byte> bytes, out uint result)
    {
        if (bytes.Length <= MaxBytesOfVarInt32WithoutOverflow)
        {
            result = 0;
            return 0;
        }

        uint value = 0;
        var consumedByteCount = 0;
        byte byteValue;
        while (consumedByteCount < MaxBytesOfVarInt32WithoutOverflow)
        {
            byteValue = bytes[consumedByteCount];
            value |= (byteValue & 0x7Fu) << (consumedByteCount * 7);
            consumedByteCount++;
            if (byteValue <= 0x7Fu)
            {
                result = value;
                return consumedByteCount; // early exit
            }
        }

        byteValue = bytes[MaxBytesOfVarInt32WithoutOverflow];
        if (byteValue > 0b_1111u)
        {
            ThrowHelper.ThrowBadSerializationDataException(ExceptionMessages.VarInt32Overflow());
        }

        value |= (uint)byteValue << (MaxBytesOfVarInt32WithoutOverflow * 7);
        result = value;
        return MaxBytesOfVarInt32;
    }

    private static int Read7BitEncodedIntSlow(ReadOnlySequence<byte> buffer, out uint result)
    {
        uint value = 0;
        var consumedByteCount = 0;
        foreach (var memory in buffer)
        {
            var bytes = memory.Span;
            foreach (var byteValue in bytes)
            {
                if (consumedByteCount < MaxBytesOfVarInt32WithoutOverflow)
                {
                    value |= (byteValue & 0x7Fu) << (consumedByteCount * 7);
                    consumedByteCount++;
                    if (byteValue <= 0x7Fu)
                    {
                        result = value;
                        return consumedByteCount; // early exit
                    }
                }
                else if (byteValue <= 0b_1111u)
                {
                    value |= (uint)byteValue << (MaxBytesOfVarInt32WithoutOverflow * 7);
                    result = value;
                    return MaxBytesOfVarInt32; // early exit
                }
                else
                {
                    ThrowHelper.ThrowBadSerializationDataException(ExceptionMessages.VarInt32Overflow());
                }
            }
        }

        ThrowHelper.ThrowBadSerializationDataException(ExceptionMessages.VarInt32Truncated());
        result = 0;
        return 0;
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
        return new TypeId(await reader.Read7BitEncodedIntAsync(cancellationToken));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static async ValueTask<RefId> ReadRefIdAsync(
        this BatchReader reader,
        CancellationToken cancellationToken = default
    )
    {
        return new RefId(await reader.Read7BitEncodedIntAsync(cancellationToken));
    }
}
