using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Fury.Collections;
using Fury.Context;
using Fury.Meta;

namespace Fury.Serialization.Meta;

internal struct MetaStringSerializer(AutoIncrementIdDictionary<MetaString> sharedMetaStringContext)
{
    private int? _lastUncompletedId;
    private bool _shouldWriteId;
    private bool _hasWrittenHeader;
    private bool _hasWrittenHashCodeOrEncoding;
    private bool _hasWrittenBytes;

    public void Reset()
    {
        _lastUncompletedId = null;
        _shouldWriteId = false;
        _hasWrittenHeader = false;
        _hasWrittenHashCodeOrEncoding = false;
        _hasWrittenBytes = false;
    }

    public bool Write(ref BatchWriter writer, MetaString metaString)
    {
        _lastUncompletedId ??= sharedMetaStringContext.AddOrGet(metaString, out _shouldWriteId);
        var completed = true;
        if (_shouldWriteId)
        {
            var header = (uint)((_lastUncompletedId.Value + 1) << 1 | 1);
            completed = completed && writer.TryWrite7BitEncodedUint(header, ref _hasWrittenHeader);
        }
        else
        {
            var length = metaString.Bytes.Length;
            var header = (uint)(length << 1);
            completed = completed && writer.TryWrite7BitEncodedUint(header, ref _hasWrittenHeader);
            if (length > MetaString.SmallStringThreshold)
            {
                completed = completed && writer.TryWrite(metaString.HashCode, ref _hasWrittenHashCodeOrEncoding);
            }
            else
            {
                completed =
                    completed && writer.TryWrite((byte)metaString.MetaEncoding, ref _hasWrittenHashCodeOrEncoding);
            }

            completed = completed && writer.TryWrite(metaString.Bytes, ref _hasWrittenBytes);
        }

        return completed;
    }
}

internal struct MetaStringDeserializer(
    AutoIncrementIdDictionary<MetaString> sharedMetaStringContext,
    MetaStringStorage sharedMetaStringStorage,
    MetaStringStorage.EncodingPolicy encodingPolicy
)
{
    private uint? _header;
    private ulong? _hashCode;
    private MetaString.Encoding? _metaEncoding;
    private ulong? _v1;
    private ulong? _v2;

    private MetaStringStorage.CreateFromBytesDelegateCache? _cache;

    public void Reset()
    {
        _header = null;
        _hashCode = null;
        _metaEncoding = null;
        _v1 = null;
        _v2 = null;
    }

    public bool Read(BatchReader reader, [NotNullWhen(true)] ref MetaString? metaString)
    {
        if (metaString is not null)
        {
            return true;
        }
        var completed = reader.TryRead7BitEncodedUint(ref _header);
        if (_header is null || !completed)
        {
            return false;
        }

        var isId = (_header.Value & 1) == 1;
        if (isId)
        {
            metaString = GetMetaStringById();
            completed = true;
        }
        else
        {
            completed = GetMetaStringByHashCodeAndBytes(reader, out metaString);
        }

        Debug.Assert(completed);
        return completed;
    }

    public async ValueTask<MetaString> ReadAsync(BatchReader reader, CancellationToken cancellationToken = default)
    {
        _header = await reader.Read7BitEncodedUintAsync(cancellationToken);
        var isId = (_header.Value & 1) == 1;
        if (isId)
        {
            return GetMetaStringById();
        }

        return await GetMetaStringByHashCodeAndBytesAsync(reader, cancellationToken);
    }

    private MetaString GetMetaStringById()
    {
        Debug.Assert(_header is not null);
        var id = (int)(_header!.Value >> 1) - 1;
        if (!sharedMetaStringContext.TryGetValue(id, out var metaString))
        {
            ThrowHelper.ThrowBadDeserializationInputException_UnknownMetaStringId(id);
        }

        return metaString;
    }

    private bool GetMetaStringByHashCodeAndBytes(BatchReader reader, [NotNullWhen(true)] out MetaString? metaString)
    {
        Debug.Assert(_header is not null);
        metaString = null;
        var completed = true;
        var length = (int)(_header!.Value >> 1);
        if (length > MetaString.SmallStringThreshold)
        {
            // big meta string
            completed = completed && reader.TryRead(ref _hashCode);
            if (_hashCode is null)
            {
                return false;
            }

            if (!reader.TryReadAtLeast(length, out var readResult))
            {
                return false;
            }

            var buffer = readResult.Buffer;
            if (buffer.Length > length)
            {
                buffer = buffer.Slice(0, length);
            }

            metaString = sharedMetaStringStorage.GetBigMetaString(
                _hashCode.Value,
                in buffer,
                encodingPolicy,
                ref _cache
            );
        }
        else
        {
            // small meta string
            completed = completed && reader.TryRead(ref _metaEncoding);
            if (_metaEncoding is null)
            {
                return false;
            }

            Span<ulong> v = stackalloc ulong[2];
            if (length <= 8)
            {
                completed = completed && reader.TryReadAs(length, ref _v1);
                if (_v1 is null)
                {
                    return false;
                }

                _v2 = 0;
                v[0] = _v1.Value;
            }
            else
            {
                completed = completed && reader.TryReadAs(8, ref _v1);
                completed = completed && reader.TryReadAs(length - 8, ref _v2);
                if (_v1 is null || _v2 is null)
                {
                    return false;
                }
                v[0] = _v1.Value;
                v[1] = _v2.Value;
            }

            var bytes = MemoryMarshal.AsBytes(v).Slice(0, length);
            var hashCode = MetaString.GetHashCode(bytes, _metaEncoding.Value);
            metaString = sharedMetaStringStorage.GetSmallMetaString(
                hashCode,
                _v1.Value,
                _v2.Value,
                length,
                encodingPolicy,
                ref _cache
            );
        }

        return completed;
    }

    private async ValueTask<MetaString> GetMetaStringByHashCodeAndBytesAsync(
        BatchReader reader,
        CancellationToken cancellationToken = default
    )
    {
        Debug.Assert(_header is not null);
        MetaString metaString;
        var length = (int)(_header!.Value >> 1);
        if (length > MetaString.SmallStringThreshold)
        {
            // big meta string
            _hashCode = await reader.ReadAsync<ulong>(cancellationToken);
            var readResult = await reader.ReadAtLeastOrThrowIfLessAsync(length, cancellationToken);
            var buffer = readResult.Buffer;
            if (buffer.Length > length)
            {
                buffer = buffer.Slice(0, length);
            }

            metaString = sharedMetaStringStorage.GetBigMetaString(
                _hashCode.Value,
                in buffer,
                encodingPolicy,
                ref _cache
            );
        }
        else
        {
            // small meta string
            _metaEncoding = await reader.ReadAsync<MetaString.Encoding>(cancellationToken);
            if (length == 0)
            {
                metaString = MetaStringStorage.GetEmptyMetaString(encodingPolicy);
            }
            else
            {
                if (length <= 8)
                {
                    _v1 = await reader.ReadAsAsync<ulong>(length, cancellationToken);
                    _v2 = 0;
                }
                else
                {
                    _v1 = await reader.ReadAsAsync<ulong>(8, cancellationToken);
                    _v2 = await reader.ReadAsAsync<ulong>(length - 8, cancellationToken);
                }

                var hashCode = MetaString.GetHashCode(length, _v1.Value, _v2.Value, _metaEncoding.Value);
                metaString = sharedMetaStringStorage.GetSmallMetaString(
                    hashCode,
                    _v1.Value,
                    _v2.Value,
                    length,
                    encodingPolicy,
                    ref _cache
                );
            }
        }

        return metaString;
    }
}
