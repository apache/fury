using System;
using System.Buffers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Fury.Buffers;

namespace Fury.Serializer;

public enum StringEncoding : byte
{
    Latin1 = 0,

    // ReSharper disable once InconsistentNaming
    UTF16 = 1,

    // ReSharper disable once InconsistentNaming
    UTF8 = 2,
}

internal static class StringEncodingExtensions
{
    internal const int BitCount = 2;
    internal const int Mask = (1 << BitCount) - 1;

    internal static readonly Encoding Latin1 = Encoding.GetEncoding(
        "ISO-8859-1",
        EncoderFallback.ExceptionFallback,
        DecoderFallback.ExceptionFallback
    );

    public static Encoding GetEncoding(this StringEncoding encoding)
    {
        return encoding switch
        {
            StringEncoding.Latin1 => Latin1,
            StringEncoding.UTF16 => Encoding.Unicode,
            _ => Encoding.UTF8
        };
    }
}

internal sealed class StringSerializer : AbstractSerializer<string>
{
    public static StringSerializer Instance { get; } = new();

    public override void Write(SerializationContext context, in string value)
    {
        // TODO: optimize for big strings

        var preferredEncodings = context.Fury.Config.StringSerializationConfig.PreferredEncodings;
        foreach (var preferredEncoding in preferredEncodings)
        {
            var encoding = preferredEncoding.GetEncoding();
            int byteCount;
            try
            {
                byteCount = encoding.GetByteCount(value);
            }
            catch (EncoderFallbackException)
            {
                continue;
            }
            var header = (uint)((byteCount << StringEncodingExtensions.BitCount) | (byte)preferredEncoding);
            context.Writer.Write7BitEncodedUint(header);
            context.Writer.Write(value.AsSpan(), encoding, byteCount);
        }
    }
}

internal sealed class StringDeserializer : AbstractDeserializer<string>
{
    private readonly ConcurrentObjectPool<Progress> _progressPool;
    private readonly ArrayPool<char> _charPool = ArrayPool<char>.Shared;

    private readonly ConcurrentObjectPool<Decoder> _latin1DecoderPool =
        new(_ => StringEncodingExtensions.Latin1.GetDecoder());
    private readonly ConcurrentObjectPool<Decoder> _utf16DecoderPool = new(_ => Encoding.Unicode.GetDecoder());
    private readonly ConcurrentObjectPool<Decoder> _utf8DecoderPool = new(_ => Encoding.UTF8.GetDecoder());

    public StringDeserializer()
    {
        _progressPool = new ConcurrentObjectPool<Progress>(_ => new Progress(this));
    }

    public override void CreateInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref Box<string> boxedInstance
    )
    {
        var str = string.Empty;
        CreateAndFillInstance(context, ref progress, ref str);
        boxedInstance.Value = str;
    }

    public override void FillInstance(
        DeserializationContext context,
        DeserializationProgress progress,
        Box<string> boxedInstance
    ) { }

    public override void CreateAndFillInstance(
        DeserializationContext context,
        ref DeserializationProgress? progress,
        ref string? instance
    )
    {
        ThrowHelper.ThrowUnreachableExceptionIf_DebugOnly(progress is not (null or Progress));
        var typedProgress = progress as Progress ?? _progressPool.Rent();
        if (progress is not Progress)
        {
            typedProgress = _progressPool.Rent();
        }
        if (typedProgress.StringEncoding is null)
        {
            if (!context.Reader.TryRead7BitEncodedUint(out var header))
            {
                typedProgress.Status = DeserializationStatus.InstanceNotCreated;
                typedProgress.StringEncoding = null;
                typedProgress.ByteCount = Progress.NoByteCount;
                progress = typedProgress;
                return;
            }
            typedProgress.StringEncoding = (StringEncoding)(header & StringEncodingExtensions.Mask);
            typedProgress.ByteCount = (int)(header >> StringEncodingExtensions.BitCount);
        }

        var byteCount = typedProgress.ByteCount;
        var maxCharCount = typedProgress.Encoding!.GetMaxCharCount(byteCount);
        var decoder = typedProgress.Decoder!;

        var canBeDoneInOneGo = false;
        if (!context.Reader.TryRead(out var readResult))
        {
            canBeDoneInOneGo = readResult.Buffer.Length >= byteCount;
        }

        if (canBeDoneInOneGo && maxCharCount <= StaticConfigs.CharsStackAllocLimit)
        {
            // fast path

            Span<char> charBuffer = stackalloc char[maxCharCount];
            context.Reader.ReadString(byteCount, decoder, charBuffer, out var charsUsed, out var bytesUsed);
            ThrowHelper.ThrowUnreachableExceptionIf_DebugOnly(bytesUsed < byteCount);
            instance = charBuffer.Slice(0, charsUsed).ToString();
            typedProgress.Reset();
            _progressPool.Return(typedProgress);
            progress = DeserializationProgress.Completed;
        }
        else
        {
            typedProgress.EnsureCharBufferCapacity(maxCharCount);
            var charBuffer = typedProgress.CharBuffer.AsSpan().Slice(typedProgress.CharsUsed);

            var bytesUnused = byteCount - typedProgress.BytesUsed;
            context.Reader.ReadString(bytesUnused, decoder, charBuffer, out var charsUsed, out var bytesUsed);
            typedProgress.CharsUsed += charsUsed;
            typedProgress.BytesUsed += bytesUsed;

            ThrowHelper.ThrowUnreachableExceptionIf_DebugOnly(bytesUsed > bytesUnused);
            if (bytesUsed == bytesUnused)
            {
                instance = typedProgress.CharBuffer.AsSpan().Slice(0, typedProgress.CharsUsed).ToString();
                typedProgress.Reset();
                _progressPool.Return(typedProgress);
                progress = DeserializationProgress.Completed;
            }
            else
            {
                progress = typedProgress;
            }
        }
    }

    public override async ValueTask<Box<string>> CreateInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        return await CreateAndFillInstanceAsync(context, cancellationToken);
    }

    public override ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box<string> boxedInstance,
        CancellationToken cancellationToken = default
    )
    {
        return default;
    }

    public override async ValueTask<string> CreateAndFillInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    )
    {
        var header = await context.Reader.Read7BitEncodedUintAsync(cancellationToken);
        var stringEncoding = (StringEncoding)(header & StringEncodingExtensions.Mask);
        var byteCount = (int)(header >> StringEncodingExtensions.BitCount);

        var encoding = stringEncoding.GetEncoding();
        var maxCharCount = encoding.GetMaxCharCount(byteCount);
        var charBuffer = _charPool.Rent(maxCharCount);
        var decoderPool = GetDecoderPool(stringEncoding);
        var decoder = decoderPool.Rent();
        var (charsUsed, bytesUsed) = await context
            .Reader
            .ReadStringAsync(byteCount, decoder, charBuffer, cancellationToken);
        ThrowHelper.ThrowUnreachableExceptionIf_DebugOnly(bytesUsed < byteCount);
        var str = charBuffer.AsSpan().Slice(0, charsUsed).ToString();
        decoder.Reset();
        decoderPool.Return(decoder);
        _charPool.Return(charBuffer);

        return str;
    }

    private ConcurrentObjectPool<Decoder> GetDecoderPool(StringEncoding encoding)
    {
        return encoding switch
        {
            StringEncoding.Latin1 => _latin1DecoderPool,
            StringEncoding.UTF16 => _utf16DecoderPool,
            _ => _utf8DecoderPool
        };
    }

    private sealed class Progress(StringDeserializer deserializer)
        : DeserializationProgress<StringDeserializer>(deserializer)
    {
        public const int NoByteCount = -1;

        public int ByteCount = NoByteCount;

        public StringEncoding? StringEncoding { get; set; }

        public Encoding? Encoding => StringEncoding?.GetEncoding();

        // cache decoders to avoid creating them every time

        private Decoder? _decoder;

        public Decoder? Decoder
        {
            get
            {
                if (_decoder is not null)
                {
                    return _decoder;
                }

                if (StringEncoding is not { } stringEncoding)
                {
                    return null;
                }

                _decoder = Deserializer!.GetDecoderPool(stringEncoding).Rent();
                return _decoder;
            }
        }

        public char[] CharBuffer { get; private set; } = [];
        public int CharsUsed;
        public int BytesUsed;

        public void EnsureCharBufferCapacity(int length)
        {
            if (CharBuffer.Length >= length)
            {
                return;
            }

            var pool = Deserializer!._charPool;
            var newBuffer = pool.Rent(length);
            if (CharBuffer.Length != 0)
            {
                if (CharsUsed > 0)
                {
                    ThrowHelper.ThrowUnreachableException_DebugOnly();

                    Array.Copy(CharBuffer, newBuffer, CharsUsed);
                }
                pool.Return(CharBuffer);
            }

            CharBuffer = newBuffer;
        }

        public void Reset()
        {
            if (StringEncoding is { } stringEncoding)
            {
                if (_decoder is not null)
                {
                    _decoder.Reset();
                    Deserializer!.GetDecoderPool(stringEncoding).Return(_decoder);
                    _decoder = null;
                }
                StringEncoding = null;
            }

            ByteCount = 0;
            CharsUsed = 0;
            BytesUsed = 0;
            Status = DeserializationStatus.InstanceNotCreated;
        }
    }
}
