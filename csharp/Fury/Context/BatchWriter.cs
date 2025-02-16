using System;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fury.Context;

// This is used to reduce the virtual call overhead of the PipeWriter

[StructLayout(LayoutKind.Auto)]
public ref partial struct BatchWriter
{
    private readonly Context _context;

    private Span<byte> _cachedBuffer = Span<byte>.Empty;
    private int _version;

    internal BatchWriter(Context context)
    {
        _context = context;
        UpdateVersion();
    }

    public void Advance(int count)
    {
        EnsureVersion();
        if (count > _cachedBuffer.Length)
        {
            ThrowHelper.ThrowArgumentOutOfRangeException_AttemptedToAdvanceFurtherThanBufferLength(
                nameof(count),
                _cachedBuffer.Length,
                count
            );
        }
        _context.Consume(count);
        _version = _context.Version;
        _cachedBuffer = _cachedBuffer.Slice(count);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureVersion();
        if (_cachedBuffer.Length < sizeHint)
        {
            _context.AdvanceConsumed();
            UpdateVersion(sizeHint);
        }

        return _cachedBuffer;
    }

    public void Flush()
    {
        _context.AdvanceConsumed();
        EnsureVersion();
    }

    public bool TryGetSpan(int sizeHint, out Span<byte> span)
    {
        EnsureVersion();
        span = GetSpan();
        return span.Length >= sizeHint;
    }

    private void EnsureVersion()
    {
        if (_context.Version != _version)
        {
            UpdateVersion();
        }
    }

    private void UpdateVersion(int sizeHint = 0)
    {
        _version = _context.Version;
        _cachedBuffer = _context.Writer.GetSpan(sizeHint);
    }

    internal sealed class Context
    {
        public PipeWriter Writer;
        public int Consumed { get; private set; }
        public int Version { get; private set; }

        public void Initialize(PipeWriter writer)
        {
            Writer = writer;
            Consumed = 0;
            Version = 0;
        }

        public void AdvanceConsumed()
        {
            Writer.Advance(Consumed);
            Consumed = 0;
            PumpVersion();
        }

        public void Consume(int count)
        {
            Consumed += count;
            PumpVersion();
        }

        private void PumpVersion()
        {
            Version++;
        }

        public void Reset()
        {
            Consumed = 0;
            Version = 0;
        }
    }
}
