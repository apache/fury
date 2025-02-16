using System;
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Fury.Meta;
using JetBrains.Annotations;

namespace Fury.Context;

public sealed partial class BatchReader
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe TTarget ReadFixedSized<TTarget>(ReadOnlySequence<byte> buffer, int size)
        where TTarget : unmanaged
    {
        TTarget result = default;
        buffer.Slice(0, size).CopyTo(new Span<byte>(&result, size));
        return result;
    }

    public async ValueTask<T> ReadAsync<T>(CancellationToken cancellationToken = default)
        where T : unmanaged
    {
        var requiredSize = TypeHelper<T>.Size;
        var result = await ReadAtLeastOrThrowIfLessAsync(requiredSize, cancellationToken);
        var buffer = result.Buffer;
        var value = ReadFixedSized<T>(buffer, requiredSize);
        Advance(requiredSize);
        return value;
    }

    public bool TryRead<T>(out T value)
        where T : unmanaged
    {
        var requiredSize = TypeHelper<T>.Size;
        if (!TryRead(out var result))
        {
            value = default;
            return false;
        }
        var buffer = result.Buffer;
        if (buffer.Length < requiredSize)
        {
            value = default;
            AdvanceTo(buffer.Start, buffer.End);
            return false;
        }

        value = ReadFixedSized<T>(buffer, requiredSize);
        Advance(requiredSize);
        return true;
    }

    internal bool TryRead<T>(ref T? value)
        where T : unmanaged
    {
        if (value is null)
        {
            if (!TryRead(out T notNullValue))
            {
                return false;
            }
            value = notNullValue;
        }
        return true;
    }

    public async ValueTask<T> ReadAsAsync<T>(int size, CancellationToken cancellationToken = default)
        where T : unmanaged
    {
        var result = await ReadAtLeastOrThrowIfLessAsync(size, cancellationToken);
        var buffer = result.Buffer;
        var value = ReadFixedSized<T>(buffer, size);
        Advance(size);
        return value;
    }

    public bool TryReadAs<T>(int size, out T value)
        where T : unmanaged
    {
        if (!TryRead(out var result))
        {
            value = default;
            return false;
        }
        var buffer = result.Buffer;
        if (buffer.Length < size)
        {
            value = default;
            AdvanceTo(buffer.Start, buffer.End);
            return false;
        }

        value = ReadFixedSized<T>(buffer, size);
        Advance(size);
        return true;
    }

    internal bool TryReadAs<T>(int size, ref T? value)
        where T : unmanaged
    {
        if (value is null)
        {
            if (!TryReadAs(size, out T notNullValue))
            {
                return false;
            }
            value = notNullValue;
        }
        return true;
    }

    public async ValueTask ReadMemoryAsync<TElement>(
        Memory<TElement> destination,
        CancellationToken cancellationToken = default
    )
        where TElement : unmanaged
    {
        var requiredSize = destination.Length * Unsafe.SizeOf<TElement>();
        var result = await ReadAtLeastOrThrowIfLessAsync(requiredSize, cancellationToken);
        var buffer = result.Buffer;
        if (buffer.Length > requiredSize)
        {
            buffer = buffer.Slice(0, requiredSize);
        }

        buffer.CopyTo(MemoryMarshal.AsBytes(destination.Span));
        AdvanceTo(buffer.End);
    }

    public int ReadMemory<TElement>(Span<TElement> destination)
        where TElement : unmanaged
    {
        var bytesDestination = MemoryMarshal.AsBytes(destination);
        var elementSize = Unsafe.SizeOf<TElement>();
        var requiredSize = bytesDestination.Length;
        if (!TryReadAtLeastOrThrowIfNoFurtherData(requiredSize, out var result))
        {
            return 0;
        }
        var buffer = result.Buffer;
        var examinedPosition = buffer.End;
        var bufferLength = (int)buffer.Length;
        if (bufferLength > requiredSize)
        {
            buffer = buffer.Slice(0, requiredSize);
            examinedPosition = buffer.End;
        }
        else if (bufferLength % elementSize != 0)
        {
            bufferLength -= bufferLength % elementSize;
            buffer = buffer.Slice(0, bufferLength);
        }

        buffer.CopyTo(bytesDestination);
        AdvanceTo(buffer.End, examinedPosition);
        return bufferLength;
    }

    public async ValueTask<string> ReadStringAsync(
        int byteCount,
        Encoding encoding,
        CancellationToken cancellationToken = default
    )
    {
        var result = await ReadAtLeastOrThrowIfLessAsync(byteCount, cancellationToken);
        var buffer = result.Buffer;
        var value = DoReadString(byteCount, buffer, encoding);
        Advance(byteCount);
        return value;
    }

    public async ValueTask<(int charsUsed, int bytesUsed)> ReadStringAsync(
        int byteCount,
        Decoder decoder,
        Memory<char> output,
        CancellationToken cancellationToken = default
    )
    {
        var charsUsed = 0;
        var bytesUsed = 0;
        while (bytesUsed < byteCount)
        {
            var result = await ReadAsync(cancellationToken);
            ReadStringCommon(
                result,
                byteCount - bytesUsed,
                decoder,
                output.Span.Slice(charsUsed),
                out var currentCharsUsed,
                out var currentBytesUsed
            );
            charsUsed += currentCharsUsed;
            bytesUsed += currentBytesUsed;
        }

        return (charsUsed, bytesUsed);
    }

    public void ReadString(int byteCount, Decoder decoder, Span<char> output, out int charsUsed, out int bytesUsed)
    {
        if (!TryRead(out var result))
        {
            charsUsed = 0;
            bytesUsed = 0;
            return;
        }

        ReadStringCommon(result, byteCount, decoder, output, out charsUsed, out bytesUsed);
    }

    private unsafe void ReadStringCommon(
        ReadResult result,
        int byteCount,
        Decoder decoder,
        Span<char> output,
        out int charsUsed,
        out int bytesUsed
    )
    {
        var buffer = result.Buffer;
        var availableByteCount = buffer.Length;
        if (availableByteCount > byteCount)
        {
            buffer = buffer.Slice(0, byteCount);
        }

        var flush = availableByteCount >= byteCount;
        charsUsed = 0;
        bytesUsed = 0;
        var currentOutput = output;
        var bytesEnumerator = buffer.GetEnumerator();
        var hasNext = bytesEnumerator.MoveNext();
        Debug.Assert(hasNext);
        while (hasNext)
        {
            var byteMemory = bytesEnumerator.Current;
            hasNext = bytesEnumerator.MoveNext();
            var currentBytes = byteMemory.Span;
            fixed (char* pOutput = currentOutput)
            fixed (byte* pBytes = byteMemory.Span)
            {
                decoder.Convert(
                    pBytes,
                    currentBytes.Length,
                    pOutput,
                    currentOutput.Length,
                    flush && !hasNext,
                    out var currentBytesUsed,
                    out var currentCharsUsed,
                    out _
                );

                charsUsed += currentCharsUsed;
                bytesUsed += currentBytesUsed;
                currentOutput = currentOutput.Slice(currentCharsUsed);
                if (charsUsed == output.Length)
                {
                    break;
                }
            }
        }
        AdvanceTo(buffer.GetPosition(bytesUsed));
    }

    private static unsafe string DoReadString(int byteCount, ReadOnlySequence<byte> bytes, Encoding encoding)
    {
        var decoder = encoding.GetDecoder();
        int writtenChars;
        string result;
        if (byteCount < StaticConfigs.CharStackAllocLimit)
        {
            // Fast path
            Span<char> stringBuffer = stackalloc char[byteCount];
            writtenChars = ReadStringCommon(decoder, bytes, stringBuffer);
            result = stringBuffer.Slice(0, writtenChars).ToString();
        }
        else
        {
            var rentedBuffer = ArrayPool<char>.Shared.Rent(byteCount);
            writtenChars = ReadStringCommon(decoder, bytes, rentedBuffer);
            result = new string(rentedBuffer, 0, writtenChars);
            ArrayPool<char>.Shared.Return(rentedBuffer);
        }

        return result;
    }

    private static unsafe int ReadStringCommon(Decoder decoder, ReadOnlySequence<byte> bytes, Span<char> output)
    {
        var writtenChars = 0;
        foreach (var byteMemory in bytes)
        {
            int charsUsed;
            var byteSpan = byteMemory.Span;
            fixed (char* pUnWrittenBuffer = output)
            fixed (byte* pBytes = byteMemory.Span)
            {
                decoder.Convert(
                    pBytes,
                    byteSpan.Length,
                    pUnWrittenBuffer,
                    output.Length,
                    false,
                    out _,
                    out charsUsed,
                    out _
                );
            }

            output = output.Slice(charsUsed);
            writtenChars += charsUsed;
        }

        return writtenChars;
    }

    public async ValueTask<int> Read7BitEncodedIntAsync(CancellationToken cancellationToken = default)
    {
        var result = await Read7BitEncodedUintAsync(cancellationToken);
        return (int)BitOperations.RotateRight(result, 1);
    }

    public bool TryRead7BitEncodedInt(out int value)
    {
        if (!TryRead7BitEncodedUint(out var result))
        {
            value = 0;
            return false;
        }
        value = (int)BitOperations.RotateRight(result, 1);
        return true;
    }

    public async ValueTask<uint> Read7BitEncodedUintAsync(CancellationToken cancellationToken = default)
    {
        var result = await ReadAtLeastAsync(MaxBytesOfVarInt32WithoutOverflow + 1, cancellationToken);
        var buffer = result.Buffer;

        // Fast path
        var value = DoRead7BitEncodedUintFast(buffer.First.Span, out var consumed);
        if (consumed == 0)
        {
            // Slow path
            value = DoRead7BitEncodedUintSlow(buffer, out consumed);
        }

        Advance(consumed);

        return value;
    }

    public bool TryRead7BitEncodedUint(out uint value)
    {
        if (!TryRead(out var result))
        {
            value = 0;
            return false;
        }
        var buffer = result.Buffer;

        // Fast path
        value = DoRead7BitEncodedUintFast(buffer.First.Span, out var consumed);
        if (consumed == 0)
        {
            // Slow path
            value = DoRead7BitEncodedUintSlow(buffer, out consumed);
        }

        if (consumed == 0)
        {
            value = 0;
            return false;
        }

        Advance(consumed);
        return true;
    }

    public bool TryRead7BitEncodedUint(ref uint? value)
    {
        if (value is null)
        {
            if (!TryRead7BitEncodedUint(out var notNullValue))
            {
                return false;
            }
            value = notNullValue;
        }
        return true;
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
            ThrowHelper.ThrowBadDeserializationInputException_VarInt32Overflow();
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
                        ThrowHelper.ThrowBadDeserializationInputException_VarInt32Overflow();
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

    public async ValueTask<long> Read7BitEncodedLongAsync(CancellationToken cancellationToken = default)
    {
        var result = await Read7BitEncodedUlongAsync(cancellationToken);
        return (long)BitOperations.RotateRight(result, 1);
    }

    public bool TryRead7BitEncodedLong(out long value)
    {
        if (!TryRead7BitEncodedUlong(out var result))
        {
            value = 0;
            return false;
        }
        value = (long)BitOperations.RotateRight(result, 1);
        return true;
    }

    public async ValueTask<ulong> Read7BitEncodedUlongAsync(CancellationToken cancellationToken = default)
    {
        var result = await ReadAtLeastAsync(MaxBytesOfVarInt64WithoutOverflow + 1, cancellationToken);
        var buffer = result.Buffer;

        // Fast path
        var value = DoRead7BitEncodedUlongFast(buffer.First.Span, out var consumed);
        if (consumed == 0)
        {
            // Slow path
            value = DoRead7BitEncodedUlongSlow(buffer, out consumed);
        }

        Advance(consumed);

        return value;
    }

    public bool TryRead7BitEncodedUlong(out ulong value)
    {
        if (!TryRead(out var result))
        {
            value = 0;
            return false;
        }
        var buffer = result.Buffer;

        // Fast path
        value = DoRead7BitEncodedUlongFast(buffer.First.Span, out var consumed);
        if (consumed == 0)
        {
            // Slow path
            value = DoRead7BitEncodedUlongSlow(buffer, out consumed);
        }

        if (consumed == 0)
        {
            value = 0;
            return false;
        }

        Advance(consumed);
        return true;
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async ValueTask<int> ReadCountAsync(CancellationToken cancellationToken)
    {
        return (int)await Read7BitEncodedUintAsync(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadCount(out int value)
    {
        if (!TryRead7BitEncodedUint(out var result))
        {
            value = 0;
            return false;
        }
        value = (int)result;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal async ValueTask<ReferenceFlag> ReadReferenceFlagAsync(CancellationToken cancellationToken = default)
    {
        return (ReferenceFlag)await ReadAsync<sbyte>(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryReadReferenceFlag(out ReferenceFlag value)
    {
        if (!TryRead(out sbyte result))
        {
            value = default;
            return false;
        }
        value = (ReferenceFlag)result;
        return true;
    }

    [Conditional("DEBUG")]
    private static void CheckTypeKind(byte kind)
    {
        try
        {
            _ = Enum.GetName(typeof(InternalTypeKind), kind);
        }
        catch (ArgumentException e)
        {
            ThrowHelper.ThrowBadDeserializationInputException_UnrecognizedTypeKind(kind, e);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal async ValueTask<InternalTypeKind> ReadTypeKindAsync(CancellationToken cancellationToken = default)
    {
        var kind = (byte)await Read7BitEncodedUintAsync(cancellationToken);
        BatchReader.CheckTypeKind(kind);
        return (InternalTypeKind)kind;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryReadTypeKind(out InternalTypeKind value)
    {
        if (!TryRead7BitEncodedUint(out var kind))
        {
            value = default;
            return false;
        }
        BatchReader.CheckTypeKind((byte)kind);
        value = (InternalTypeKind)kind;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal async ValueTask<RefId> ReadRefIdAsync(CancellationToken cancellationToken = default)
    {
        return new RefId((int)await Read7BitEncodedUintAsync(cancellationToken));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryReadRefId(out RefId value)
    {
        if (!TryRead7BitEncodedUint(out var result))
        {
            value = default;
            return false;
        }
        value = new RefId((int)result);
        return true;
    }
}
