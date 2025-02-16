using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.InteropServices;
using Fury.Meta;

namespace Fury.Context;

internal sealed class MetaStringStorage
{
    private const char NamespaceSpecialChar1 = '.';
    private const char NamespaceSpecialChar2 = '_';
    private const char NameSpecialChar1 = '$';
    private const char NameSpecialChar2 = '_';
    private const char FieldSpecialChar1 = '$';
    private const char FieldSpecialChar2 = '_';

    public static MetaString EmptyNamespaceMetaString { get; } =
        new(string.Empty, MetaString.Encoding.Utf8, NamespaceSpecialChar1, NamespaceSpecialChar2, []);
    public static MetaString EmptyNameMetaString { get; } =
        new(string.Empty, MetaString.Encoding.Utf8, NameSpecialChar1, NameSpecialChar2, []);
    public static MetaString EmptyFieldMetaString { get; } =
        new(string.Empty, MetaString.Encoding.Utf8, FieldSpecialChar1, FieldSpecialChar2, []);

    private static readonly HybridMetaStringEncoding NamespaceEncoding = new(
        NamespaceSpecialChar1,
        NamespaceSpecialChar2
    );
    private static readonly HybridMetaStringEncoding NameEncoding = new(NameSpecialChar1, NameSpecialChar2);
    private static readonly HybridMetaStringEncoding FieldEncoding = new(FieldSpecialChar1, FieldSpecialChar2);

    private static readonly MetaString.Encoding[] CandidateNamespaceEncodings =
    [
        MetaString.Encoding.Utf8,
        MetaString.Encoding.AllToLowerSpecial,
        MetaString.Encoding.LowerUpperDigitSpecial,
    ];
    private static readonly MetaString.Encoding[] CandidateNameEncodings =
    [
        MetaString.Encoding.Utf8,
        MetaString.Encoding.LowerUpperDigitSpecial,
        MetaString.Encoding.FirstToLowerSpecial,
        MetaString.Encoding.AllToLowerSpecial,
    ];
    private static readonly MetaString.Encoding[] CandidateFieldEncodings =
    [
        MetaString.Encoding.Utf8,
        MetaString.Encoding.LowerUpperDigitSpecial,
        MetaString.Encoding.AllToLowerSpecial,
    ];

    private readonly ConcurrentDictionary<string, MetaString> _namespaceMetaStrings = new();
    private readonly ConcurrentDictionary<string, MetaString> _nameMetaStrings = new();
    private readonly ConcurrentDictionary<string, MetaString> _fieldMetaStrings = new();

    private readonly ConcurrentDictionary<ulong, MetaString> _hashCodeToNamespaceMetaString = new();
    private readonly ConcurrentDictionary<ulong, MetaString> _hashCodeToNameMetaString = new();
    private readonly ConcurrentDictionary<ulong, MetaString> _hashCodeToFieldMetaString = new();

    public (char SpecialChar1, char SpecialChar2) GetSpecialChars(EncodingPolicy policy)
    {
        return policy switch
        {
            EncodingPolicy.Namespace => (NamespaceSpecialChar1, NamespaceSpecialChar2),
            EncodingPolicy.Name => (NameSpecialChar1, NameSpecialChar2),
            EncodingPolicy.Field => (FieldSpecialChar1, FieldSpecialChar2),
            _ => ThrowHelper.ThrowUnreachableException<(char, char)>(),
        };
    }

    [Pure]
    public static MetaString GetEmptyMetaString(EncodingPolicy policy)
    {
        return policy switch
        {
            EncodingPolicy.Namespace => EmptyNamespaceMetaString,
            EncodingPolicy.Name => EmptyNameMetaString,
            EncodingPolicy.Field => EmptyFieldMetaString,
            _ => ThrowHelper.ThrowUnreachableException<MetaString>(),
        };
    }

    public MetaString GetMetaString(string? chars, EncodingPolicy policy)
    {
        if (chars is null)
        {
            return GetEmptyMetaString(policy);
        }
        var hybridEncoding = GetHybridEncoding(policy);
        var candidateEncodings = GetCandidateEncodings(policy);
        var metaStrings = GetMetaStrings(policy);
        var encoding = hybridEncoding.SelectEncoding(chars, candidateEncodings);
        var metaString = metaStrings.GetOrAdd(
            chars,
            str =>
            {
                var bytes = encoding.GetBytes(str);
                return new MetaString(
                    str,
                    encoding.Encoding,
                    hybridEncoding.SpecialChar1,
                    hybridEncoding.SpecialChar2,
                    bytes
                );
            }
        );
        return metaString;
    }

    public MetaString GetBigMetaString(
        ulong hashCode,
        in ReadOnlySequence<byte> bytesSequence,
        EncodingPolicy policy,
        ref CreateFromBytesDelegateCache? cache
    )
    {
        Debug.Assert(bytesSequence.Length > MetaString.SmallStringThreshold);
        cache ??= new CreateFromBytesDelegateCache();
        var metaStringFactory = cache.GetBigMetaStringFactory(in bytesSequence, policy);
        var hashCodeToMetaString = GetHashCodeToMetaString(policy);
        var metaString = hashCodeToMetaString.GetOrAdd(hashCode, metaStringFactory);
        if (metaString.HashCode != hashCode)
        {
            hashCodeToMetaString.TryRemove(hashCode, out _);
            ThrowHelper.ThrowBadDeserializationInputException_BadMetaStringHashCodeOrBytes();
        }

        return metaString;
    }

    public MetaString GetSmallMetaString(
        ulong hashCode,
        ulong v1,
        ulong v2,
        int length,
        EncodingPolicy policy,
        ref CreateFromBytesDelegateCache? cache
    )
    {
        if (length == 0)
        {
            return GetEmptyMetaString(policy);
        }
        Debug.Assert(length <= MetaString.SmallStringThreshold);
        cache ??= new CreateFromBytesDelegateCache();
        var metaStringFactory = cache.GetSmallMetaStringFactory(v1, v2, length, policy);
        var hashCodeToMetaString = GetHashCodeToMetaString(policy);
        var metaString = hashCodeToMetaString.GetOrAdd(hashCode, metaStringFactory);
        if (metaString.HashCode != hashCode)
        {
            hashCodeToMetaString.TryRemove(hashCode, out _);
            ThrowHelper.ThrowBadDeserializationInputException_BadMetaStringHashCodeOrBytes();
        }

        return metaString;
    }

    private static HybridMetaStringEncoding GetHybridEncoding(EncodingPolicy policy)
    {
        return policy switch
        {
            EncodingPolicy.Namespace => NamespaceEncoding,
            EncodingPolicy.Name => NameEncoding,
            EncodingPolicy.Field => FieldEncoding,
            _ => ThrowHelper.ThrowUnreachableException<HybridMetaStringEncoding>(),
        };
    }

    private static MetaString.Encoding[] GetCandidateEncodings(EncodingPolicy policy)
    {
        return policy switch
        {
            EncodingPolicy.Namespace => CandidateNamespaceEncodings,
            EncodingPolicy.Name => CandidateNameEncodings,
            EncodingPolicy.Field => CandidateFieldEncodings,
            _ => ThrowHelper.ThrowUnreachableException<MetaString.Encoding[]>(),
        };
    }

    private ConcurrentDictionary<string, MetaString> GetMetaStrings(EncodingPolicy policy)
    {
        return policy switch
        {
            EncodingPolicy.Namespace => _namespaceMetaStrings,
            EncodingPolicy.Name => _nameMetaStrings,
            EncodingPolicy.Field => _fieldMetaStrings,
            _ => ThrowHelper.ThrowUnreachableException<ConcurrentDictionary<string, MetaString>>(),
        };
    }

    private ConcurrentDictionary<ulong, MetaString> GetHashCodeToMetaString(EncodingPolicy policy)
    {
        return policy switch
        {
            EncodingPolicy.Namespace => _hashCodeToNamespaceMetaString,
            EncodingPolicy.Name => _hashCodeToNameMetaString,
            EncodingPolicy.Field => _hashCodeToFieldMetaString,
            _ => ThrowHelper.ThrowUnreachableException<ConcurrentDictionary<ulong, MetaString>>(),
        };
    }

    public enum EncodingPolicy
    {
        Namespace,
        Name,
        Field,
    }

    public sealed class CreateFromBytesDelegateCache
    {
        private ReadOnlySequence<byte> _bytes;
        private ulong _v1;
        private ulong _v2;
        private int _smallStringLength;
        private EncodingPolicy _policy;

        private readonly Func<ulong, MetaString> _cachedBigMetaStringFactory;
        private readonly Func<ulong, MetaString> _cachedSmallMetaStringFactory;

        public CreateFromBytesDelegateCache()
        {
            // Cache the factory delegate to avoid allocations on every call to ConcurrentDictionary.GetOrAdd
            _cachedBigMetaStringFactory = CreateBigMetaString;
            _cachedSmallMetaStringFactory = CreateSmallMetaString;
        }

        public Func<ulong, MetaString> GetBigMetaStringFactory(in ReadOnlySequence<byte> bytes, EncodingPolicy policy)
        {
            _bytes = bytes;
            _policy = policy;
            return _cachedBigMetaStringFactory;
        }

        public Func<ulong, MetaString> GetSmallMetaStringFactory(ulong v1, ulong v2, int length, EncodingPolicy policy)
        {
            _v1 = v1;
            _v2 = v2;
            _smallStringLength = length;
            _policy = policy;
            return _cachedSmallMetaStringFactory;
        }

        private MetaString CreateBigMetaString(ulong hashCode)
        {
            var bytes = _bytes.ToArray();
            return CreateMetaStringCore(hashCode, bytes);
        }

        private MetaString CreateSmallMetaString(ulong hashCode)
        {
            Span<byte> bytes = stackalloc byte[MetaString.SmallStringThreshold];
            var ulongSpan = MemoryMarshal.Cast<byte, ulong>(bytes);
            ulongSpan[0] = _v1;
            ulongSpan[1] = _v2;
            bytes = bytes.Slice(0, _smallStringLength);
            return CreateMetaStringCore(hashCode, bytes.ToArray());
        }

        private MetaString CreateMetaStringCore(ulong hashCode, byte[] bytes)
        {
            var metaEncoding = MetaString.GetEncodingFromHashCode(hashCode);
            var hybridEncoding = GetHybridEncoding(_policy);
            var encoding = hybridEncoding.GetEncoding(metaEncoding);
            var charCount = encoding.GetCharCount(bytes);
            var str = StringHelper.Create(charCount, (encoding, bytes), DecodeBytes);
            return new MetaString(str, metaEncoding, hybridEncoding.SpecialChar1, hybridEncoding.SpecialChar2, bytes);
        }

        private static void DecodeBytes(Span<char> chars, (MetaStringEncoding, byte[]) state)
        {
            var (encoding, bytes) = state;
            var charsWritten = encoding.GetChars(bytes, chars);
            Debug.Assert(charsWritten == chars.Length - 1); // -1 for null terminator
        }
    }
}
