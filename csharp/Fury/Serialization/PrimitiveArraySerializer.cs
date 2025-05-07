using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Fury.Context;

namespace Fury.Serialization;

// For primitive arrays, the length is the byte size of the array rather than the number of elements.

internal sealed class PrimitiveArraySerializer<TElement> : AbstractSerializer<TElement[]>
    where TElement : unmanaged
{
    private bool _hasWrittenLength;
    private int _writtenByteCount;

    public override void Reset()
    {
        _hasWrittenLength = false;
        _writtenByteCount = 0;
    }

    public override bool Serialize(SerializationWriter writer, in TElement[] value)
    {
        var writerRef = writer.ByrefWriter;
        var bytes = MemoryMarshal.AsBytes(value.AsSpan()).Slice(_writtenByteCount);
        var byteCount = bytes.Length;
        if (_hasWrittenLength)
        {
            _hasWrittenLength = writerRef.Write7BitEncodedUint((uint)byteCount);
            if (_hasWrittenLength)
            {
                return false;
            }
        }

        var buffer = writerRef.GetSpan(byteCount);
        var consumed = bytes.CopyUpTo(buffer);
        _writtenByteCount += consumed;
        writerRef.Advance(consumed);
        Debug.Assert(_writtenByteCount <= byteCount);
        return _writtenByteCount == byteCount;
    }
}

internal sealed class PrimitiveArrayDeserializer<TElement> : AbstractDeserializer<TElement[]>
    where TElement : unmanaged
{
    private static readonly int ElementSize = Unsafe.SizeOf<TElement>();

    private TElement[]? _array;
    private int _readByteCount;

    public override void Reset()
    {
        _array = null;
        _readByteCount = 0;
    }

    public override object ReferenceableObject => ThrowInvalidOperationException_AcyclicType();

    public override ReadValueResult<TElement[]> Deserialize(DeserializationReader reader)
    {
        var task = Deserialize(reader, false, CancellationToken.None);
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    public override ValueTask<ReadValueResult<TElement[]>> DeserializeAsync(
        DeserializationReader reader,
        CancellationToken cancellationToken = default
    )
    {
        return Deserialize(reader, true, cancellationToken);
    }

    private async ValueTask<ReadValueResult<TElement[]>> Deserialize(
        DeserializationReader reader,
        bool isAsync,
        CancellationToken cancellationToken
    )
    {
        if (_array is null)
        {
            var byteCountResult = await reader.Read7BitEncodedUint(isAsync, cancellationToken);
            if (!byteCountResult.IsSuccess)
            {
                return ReadValueResult<TElement[]>.Failed;
            }

            var byteCount = (int)byteCountResult.Value;
            if (byteCount % ElementSize != 0)
            {
                ThrowBadDeserializationInputException_InvalidByteCount(byteCount);
                return ReadValueResult<TElement[]>.Failed;
            }

            _array = new TElement[byteCount / ElementSize];
        }

        var totalByteCount = _array.Length * ElementSize;
        var unreadByteCount = totalByteCount - _readByteCount;
        var readResult = await reader.Read(unreadByteCount, isAsync, cancellationToken);
        var buffer = readResult.Buffer;
        var destination = MemoryMarshal.AsBytes(_array.AsSpan()).Slice(_readByteCount);
        var consumed = buffer.CopyUpTo(destination);
        _readByteCount += consumed;
        reader.AdvanceTo(buffer.GetPosition(consumed));
        Debug.Assert(_readByteCount <= totalByteCount);

        if (_readByteCount != totalByteCount)
        {
            return ReadValueResult<TElement[]>.Failed;
        }

        return ReadValueResult<TElement[]>.FromValue(_array);
    }

    [DoesNotReturn]
    private static void ThrowBadDeserializationInputException_InvalidByteCount(int byteCount)
    {
        throw new BadDeserializationInputException(
            $"Invalid byte count: {byteCount}. Expected a multiple of {ElementSize}."
        );
    }
}
