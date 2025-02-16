using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Fury.Context;

public sealed partial class BatchReader(PipeReader reader)
{
    private ReadOnlySequence<byte> _cachedBuffer;
    private SequencePosition _examinedPosition;
    private bool _isCanceled;
    private bool _isCompleted;

    private bool AllExamined => _examinedPosition.Equals(_cachedBuffer.End);

    public async ValueTask<ReadResult> ReadAtLeastAsync(int minimumSize, CancellationToken cancellationToken = default)
    {
        if (_cachedBuffer.Length < minimumSize)
        {
            reader.AdvanceTo(_cachedBuffer.Start);
            var result = await reader.ReadAtLeastAsync(minimumSize, cancellationToken);
            PopulateNewData(in result);
        }

        return new ReadResult(_cachedBuffer, _isCanceled, _isCompleted);
    }

    public async ValueTask<ReadResult> ReadAtLeastOrThrowIfLessAsync(
        int minimumSize,
        CancellationToken cancellationToken = default
    )
    {
        var result = await ReadAtLeastAsync(minimumSize, cancellationToken);
        if (result.Buffer.Length < minimumSize)
        {
            ThrowHelper.ThrowBadDeserializationInputException_InsufficientData();
        }

        return result;
    }

    public void Advance(int consumed)
    {
        _cachedBuffer = _cachedBuffer.Slice(consumed);
    }

    public void AdvanceTo(SequencePosition consumed)
    {
        _cachedBuffer = _cachedBuffer.Slice(consumed);
    }

    public void AdvanceTo(SequencePosition consumed, SequencePosition examined)
    {
        AdvanceTo(consumed);
        _examinedPosition = examined;
    }

    public async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
    {
        if (AllExamined)
        {
            reader.AdvanceTo(_cachedBuffer.Start, _cachedBuffer.End);
            var result = await reader.ReadAsync(cancellationToken);
            PopulateNewData(in result);
        }

        return new ReadResult(_cachedBuffer, _isCanceled, _isCompleted);
    }

    public bool TryRead(out ReadResult result)
    {
        if (AllExamined)
        {
            reader.AdvanceTo(_cachedBuffer.Start, _cachedBuffer.End);
            var success = reader.TryRead(out result);
            if (success)
            {
                PopulateNewData(result);
            }
            return success;
        }

        result = new ReadResult(_cachedBuffer, _isCanceled, _isCompleted);
        return true;
    }

    public bool TryReadAtLeast(int minimumSize, out ReadResult result)
    {
        if (!TryRead(out result))
        {
            return false;
        }
        var buffer = result.Buffer;
        if (buffer.Length < minimumSize)
        {
            AdvanceTo(buffer.Start, buffer.End);
            if (!TryRead(out result))
            {
                return false;
            }
        }
        return result.Buffer.Length >= minimumSize;
    }

    public bool TryReadAtLeastOrThrowIfNoFurtherData(int minimumSize, out ReadResult result)
    {
        var success = TryReadAtLeast(minimumSize, out result);
        if (!success && result is not {IsCompleted: false, IsCanceled: false})
        {
            ThrowHelper.ThrowBadDeserializationInputException_InsufficientData();
        }
        return success;
    }

    private void PopulateNewData(in ReadResult result)
    {
        _cachedBuffer = result.Buffer;
        _isCanceled = result.IsCanceled;
        _isCompleted = result.IsCompleted;
    }

    public void Complete()
    {
        reader.AdvanceTo(_cachedBuffer.Start);
        _cachedBuffer = default;
        _isCompleted = true;
        reader.Complete();
    }
}
