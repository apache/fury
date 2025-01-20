using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Fury;

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

    public void AdvanceTo(int consumed)
    {
        _cachedBuffer = _cachedBuffer.Slice(consumed);
    }

    public void AdvanceTo(SequencePosition consumed)
    {
        _cachedBuffer = _cachedBuffer.Slice(consumed);
    }

    public void AdvanceTo(int consumed, int examined)
    {
        AdvanceTo(consumed);
        _examinedPosition = _cachedBuffer.GetPosition(examined);
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
            reader.AdvanceTo(_cachedBuffer.Start, _examinedPosition);
            var result = await reader.ReadAsync(cancellationToken);
            PopulateNewData(in result);
        }

        return new ReadResult(_cachedBuffer, _isCanceled, _isCompleted);
    }

    public bool TryRead(out ReadResult result)
    {
        if (AllExamined)
        {
            result = default;
            return false;
        }

        result = new ReadResult(_cachedBuffer, _isCanceled, _isCompleted);
        return true;
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
