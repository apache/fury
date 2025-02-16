using System;
using System.Buffers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Fury.Collections;
using Fury.Context;

namespace Fury.Serialization;

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
}

internal sealed class StringSerializer : AbstractSerializer<string>
{
    private readonly Encoder _latin1Encoder = StringEncodingExtensions.Latin1.GetEncoder();
    private readonly Encoder _utf16Encoder = Encoding.Unicode.GetEncoder();
    private readonly Encoder _utf8Encoder = Encoding.UTF8.GetEncoder();

    private Encoder? _selectedEncoder;
    private StringEncoding _selectedEncoding;
    private bool _hasWrittenHeader;
    private bool _hasWrittenUtf16ByteCount;
    private int _charsUsed;
    private int _byteCount;

    public override unsafe bool Write(SerializationContext context, in string value)
    {
        // TODO: optimize for big strings

        var config = context.Fury.Config.StringSerializationConfig;
        if (_selectedEncoder is null)
        {
            foreach (var preferredEncoding in config.PreferredEncodings)
            {
                var encoder = preferredEncoding switch
                {
                    StringEncoding.Latin1 => _latin1Encoder,
                    StringEncoding.UTF16 => _utf16Encoder,
                    _ => _utf8Encoder
                };
                try
                {
                    fixed (char* pChar = value.AsSpan())
                    {
                        _byteCount = encoder.GetByteCount(pChar, value.Length, true);
                        _selectedEncoding = preferredEncoding;
                        _selectedEncoder = encoder;
                    }
                }
                catch (EncoderFallbackException) { }
            }
        }

        if (_selectedEncoder is null)
        {
            // fallback to UTF8
            _selectedEncoder = _utf8Encoder;
            _selectedEncoding = StringEncoding.UTF8;
        }

        var writer = context.GetWriter();
        if (!_hasWrittenHeader)
        {
            var header = (uint)((_byteCount << StringEncodingExtensions.BitCount) | (byte)_selectedEncoding);
            _hasWrittenHeader = writer.TryWrite7BitEncodedUint(header);
            if (!_hasWrittenHeader)
            {
                return false;
            }
        }

        if (
            _selectedEncoding == StringEncoding.UTF8
            && config.WriteNumUtf16BytesForUtf8Encoding
            && !_hasWrittenUtf16ByteCount
        )
        {
            var utf16ByteCount = Encoding.Unicode.GetByteCount(value);
            _hasWrittenUtf16ByteCount = writer.TryWrite7BitEncodedUint((uint)utf16ByteCount);
            if (!_hasWrittenUtf16ByteCount)
            {
                return false;
            }
        }

        while (_charsUsed < value.Length)
        {
            var charSpan = value.AsSpan().Slice(_charsUsed);
            var buffer = writer.GetSpan();
            if (buffer.Length == 0)
            {
                return false;
            }

            fixed (char* pChar = charSpan)
            fixed (byte* pBuffer = buffer)
            {
                _selectedEncoder.Convert(
                    pChar,
                    value.Length - _charsUsed,
                    pBuffer,
                    buffer.Length,
                    true,
                    out var currentCharsUsed,
                    out var currentBytesUsed,
                    out _
                );

                _charsUsed += currentCharsUsed;
                writer.Advance(currentBytesUsed);
            }
        }

        Reset();
        return true;
    }

    public override void Reset()
    {
        base.Reset();
        _hasWrittenHeader = false;
        _charsUsed = 0;
        _byteCount = 0;
        _selectedEncoder = null;
        _latin1Encoder.Reset();
        _utf16Encoder.Reset();
        _utf8Encoder.Reset();
    }
}

internal sealed class StringDeserializer : AbstractDeserializer<string>
{
    private readonly Decoder _latin1Decoder = StringEncodingExtensions.Latin1.GetDecoder();
    private readonly Decoder _utf16Decoder = Encoding.Unicode.GetDecoder();
    private readonly Decoder _utf8Decoder = Encoding.UTF8.GetDecoder();

    private StringEncoding _encoding;
    private Decoder? _decoder;
    private int _byteCount;
    private bool _hasReadCharCount;
    private int _bytesUsed;
    private PooledArrayBufferWriter<char> _charBuffer = new();

    public override void Reset()
    {
        base.Reset();
        _encoding = default;
        _decoder = null;
        _byteCount = 0;
        _hasReadCharCount = false;
        _bytesUsed = 0;
    }

    public override void Dispose()
    {
        base.Dispose();
        _charBuffer.Dispose();
    }

    private Decoder SelectDecoder(StringEncoding encoding)
    {
        return encoding switch
        {
            StringEncoding.Latin1 => _latin1Decoder,
            StringEncoding.UTF16 => _utf16Decoder,
            _ => _utf8Decoder
        };
    }

    private void CreateAndFillCommon(ReadOnlySequence<byte> buffer)
    {
        if (buffer.Length > _byteCount - _bytesUsed)
        {
            buffer = buffer.Slice(0, _byteCount - _bytesUsed);
        }

        var bufferReader = new SequenceReader<byte>(buffer);
        var convertCompleted = true;
        while (!bufferReader.End || !convertCompleted)
        {
            var unwrittenChars = _charBuffer.GetSpan();
            var unreadBytes = bufferReader.UnreadSpan;
            var flush = bufferReader.Remaining == unreadBytes.Length;
            _decoder!.Convert(
                unreadBytes,
                unwrittenChars,
                flush,
                out var bytesUsed,
                out var charsUsed,
                out convertCompleted
            );
            _bytesUsed += bytesUsed;
            _charBuffer.Advance(charsUsed);
            bufferReader.Advance(bytesUsed);
        }
    }

#if NET8_0_OR_GREATER
    private static bool TryCreateStringFast(
        ReadOnlySequence<byte> buffer,
        int charCount,
        Decoder decoder,
        out string str
    )
    {
        var success = true;
        str = string.Create(
            charCount,
            (buffer, decoder),
            (chars, userdata) =>
            {
                // The last char is preserved for null-terminator
                var unwrittenChars = chars[..^1];

                var bufferReader = new SequenceReader<byte>(userdata.buffer);
                var convertCompleted = true;
                while (!bufferReader.End || !convertCompleted)
                {
                    var unreadBytes = bufferReader.UnreadSpan;
                    var flush = bufferReader.Remaining == unreadBytes.Length;
                    userdata
                        .decoder
                        .Convert(
                            unreadBytes,
                            unwrittenChars,
                            flush,
                            out var bytesUsed,
                            out var charsUsed,
                            out convertCompleted
                        );
                    unwrittenChars = unwrittenChars[charsUsed..];
                    bufferReader.Advance(bytesUsed);

                    if (flush && !bufferReader.End)
                    {
                        // encoded char count is too small
                        success = false;
                        return;
                    }
                }

                if (unwrittenChars.Length > 0)
                {
                    // encoded char count is too big
                    success = false;
                }
            }
        );

        return success;
    }
#endif

    public override bool CreateInstance(DeserializationContext context, ref Box<string> boxedInstance)
    {
        var str = string.Empty;
        var success = CreateAndFillInstance(context, ref str);
        boxedInstance.Value = str;
        return success;
    }

    public override bool FillInstance(DeserializationContext context, Box<string> boxedInstance) => true;

    public override bool CreateAndFillInstance(DeserializationContext context, ref string? instance)
    {
        var reader = context.GetReader();
        if (_decoder is null)
        {
            if (!reader.TryRead7BitEncodedUint(out var header))
            {
                return false;
            }

            _encoding = (StringEncoding)(header & StringEncodingExtensions.Mask);
            _decoder = SelectDecoder(_encoding);
            _byteCount = (int)(header >> StringEncodingExtensions.BitCount);
        }

        var config = context.Fury.Config.StringSerializationConfig;
        if (config.WriteNumUtf16BytesForUtf8Encoding && !_hasReadCharCount)
        {
            if (!reader.TryRead(out int charCount))
            {
                return false;
            }

            _hasReadCharCount = true;

#if NET8_0_OR_GREATER
            if (reader.TryRead(out var readResult))
            {
                var buffer = readResult.Buffer;
                if (buffer.Length > _byteCount)
                {
                    buffer = buffer.Slice(0, _byteCount);
                }

                if (buffer.Length == _byteCount)
                {
                    if (TryCreateStringFast(buffer, charCount, _decoder, out instance))
                    {
                        return true;
                    }
                }
            }
#endif
            _charBuffer.EnsureFreeCapacity(charCount);
        }

        while (_bytesUsed < _byteCount)
        {
            if (!reader.TryRead(out var readResult))
            {
                return false;
            }

            var buffer = readResult.Buffer;
            if (buffer.Length == 0)
            {
                return false;
            }

            CreateAndFillCommon(buffer);
        }

        instance = _charBuffer.WrittenSpan.ToString();

        Reset();
        return true;
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
        var reader = context.GetReader();
        if (_decoder is null)
        {
            var header = await reader.Read7BitEncodedUintAsync(cancellationToken);
            _encoding = (StringEncoding)(header & StringEncodingExtensions.Mask);
            _byteCount = (int)(header >> StringEncodingExtensions.BitCount);
            _decoder = SelectDecoder(_encoding);
        }

        var config = context.Fury.Config.StringSerializationConfig;
        if (config.WriteNumUtf16BytesForUtf8Encoding && !_hasReadCharCount)
        {
            var charCount = await reader.ReadAsync<int>(cancellationToken);
            _hasReadCharCount = true;

#if NET8_0_OR_GREATER
            if (charCount <= config.FastPathStringLengthThreshold)
            {
                var readResult = await reader.ReadAtLeastOrThrowIfLessAsync(_byteCount, cancellationToken);
                var buffer = readResult.Buffer;
                if (buffer.Length > _byteCount)
                {
                    buffer = buffer.Slice(0, _byteCount);
                }

                if (buffer.Length == _byteCount)
                {
                    if (TryCreateStringFast(buffer, charCount, _decoder, out var result))
                    {
                        return result;
                    }
                }
            }
#endif
            _charBuffer.EnsureFreeCapacity(charCount);
        }

        while (_bytesUsed < _byteCount)
        {
            var readResult = await reader.ReadAsync(cancellationToken);
            var buffer = readResult.Buffer;
            if (buffer.Length == 0)
            {
                ThrowHelper.ThrowBadDeserializationInputException_InsufficientData();
            }

            CreateAndFillCommon(buffer);
        }

        var str = _charBuffer.WrittenSpan.ToString();
        Reset();
        return str;
    }
}
