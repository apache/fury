using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Fury.Buffers;
using Fury.Context;
using Fury.Serialization.Meta;

namespace Fury.Serialization;

[Flags]
internal enum CollectionHeaderFlags : byte
{
    TrackingRef = 0b1,
    HasNull = 0b10,
    NotDeclElementType = 0b100,
    NotSameType = 0b1000,
}

// IReadOnlyCollection<TElement> is not inherited from ICollection<TElement>, so we use IEnumerable<TElement> instead.

public abstract class CollectionSerializer<TElement, TCollection>(TypeRegistration? elementRegistration = null)
    : AbstractSerializer<TCollection>
    where TCollection : notnull
{
    private bool _hasWrittenHeader;
    private bool _hasInitializedTypeMetaSerializer;

    /// <summary>
    /// Only used when elements are same type but not declared type.
    /// </summary>
    private TypeRegistration? _cachedElementRegistration;
    private TypeMetaSerializer? _elementTypeMetaSerializer;
    private bool _hasWrittenCount;

    protected TypeRegistration? ElementRegistration { get; set; } = elementRegistration;

    public override void Reset()
    {
        _hasWrittenHeader = false;
        _hasWrittenCount = false;

        _hasInitializedTypeMetaSerializer = false;
        _cachedElementRegistration = null;
    }

    public sealed override bool Serialize(SerializationWriter writer, in TCollection value)
    {
        if (ElementRegistration is null && typeof(TElement).IsSealed)
        {
            ElementRegistration = writer.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        var writerRef = writer.ByrefWriter;
        WriteCount(ref writerRef, in value);
        if (!_hasWrittenCount)
        {
            return false;
        }

        return WriteElements(ref writerRef, in value);
    }

    // TODO: Implement this method
    private bool WriteElementsHeader(ref SerializationWriterRef writerRef, in TCollection collection)
    {
        // For value types:
        // 1. If TElement is nullable, we check if there is any null element and write nullability header.
        // 2. If TElement is not nullable, we write a header without default value.
        // For reference types:
        // 1. If TElement is sealed:
        //   1. If ReferenceTracking is enabled, we write a header with tracking reference flag.
        //   2. If ReferenceTracking is disabled, we write a header with nullability flag.
        // 2. If TElement is not sealed:
        //   1. If ReferenceTracking is enabled, we write a header with tracking reference flag.

        if (typeof(TElement).IsValueType)
        {
            if (NullableHelper.IsNullable(typeof(TElement)))
            {
                WriteNullabilityHeader(ref writerRef, in collection);
            }
            else
            {
                WriteHeader(ref writerRef, default);
            }
            return _hasWrittenHeader;
        }
        var config = writerRef.Config;
        if (typeof(TElement).IsSealed)
        {
            if (config.ReferenceTracking)
            {
                // RefFlag contains the nullability information, so we don't need to write HasNull flag here.
                WriteHeader(ref writerRef, CollectionHeaderFlags.TrackingRef);
            }
            else
            {
                WriteNullabilityHeader(ref writerRef, in collection);
            }
        }
        else
        {
            if (config.ReferenceTracking)
            {
                var flags = CollectionHeaderFlags.TrackingRef;
                var checkResult = CheckElementsState(in collection, CollectionCheckOptions.TypeConsistency);
                if (checkResult.ElementType is { } elementType)
                {
                    // TODO: Handle the case when all elements are null.
                    if (elementType == typeof(TElement))
                    {
                        WriteHeader(ref writerRef, flags);
                    }
                    else
                    {
                        flags |= CollectionHeaderFlags.NotDeclElementType;
                        WriteHeader(ref writerRef, flags);
                        if (!_hasWrittenHeader)
                        {
                            return false;
                        }

                        _elementTypeMetaSerializer ??= writerRef.InnerWriter.CreateTypeMetaSerializer();
                        if (!_hasInitializedTypeMetaSerializer)
                        {
                            _elementTypeMetaSerializer.Initialize(writerRef.InnerWriter.MetaStringContext);
                            _hasInitializedTypeMetaSerializer = true;
                        }

                        _cachedElementRegistration = writerRef.TypeRegistry.GetTypeRegistration(elementType);
                        _elementTypeMetaSerializer.Write(ref writerRef, _cachedElementRegistration);
                    }
                }
                else
                {
                    flags |= CollectionHeaderFlags.NotSameType | CollectionHeaderFlags.NotDeclElementType;
                    WriteHeader(ref writerRef, flags);
                }
            }
        }
        return _hasWrittenHeader;
    }

    private void WriteNullabilityHeader(ref SerializationWriterRef writerRef, in TCollection collection)
    {
        var checkResult = CheckElementsState(in collection, CollectionCheckOptions.Nullability);
        var flags = checkResult.HasNull ? CollectionHeaderFlags.HasNull : default;
        WriteHeader(ref writerRef, flags);
    }

    /// <summary>
    /// Depending on the <paramref name="options"/>, this method will check if the elements in the collection
    /// contain null values or if they are of the same type.
    /// </summary>
    /// <param name="collection">
    /// The collection to check.
    /// </param>
    /// <param name="options">
    /// Which checks to perform.
    /// </param>
    /// <returns>
    /// A <see cref="CollectionCheckResult"/> indicating the result of the checks.
    /// </returns>
    /// <seealso cref="CheckElementsState{T}"/>
    protected virtual CollectionCheckResult CheckElementsState(
        in TCollection collection,
        CollectionCheckOptions options
    )
    {
        if (collection is not IEnumerable<TElement> enumerable)
        {
            ThrowNotSupportedException_TCollectionNotSupported();
            return default;
        }

        return CheckElementsState(enumerable.GetEnumerator(), options);
    }

    /// <summary>
    /// A default implementation of <see cref="CheckElementsState"/>.
    /// </summary>
    /// <param name="enumerator">
    /// An enumerator for the collection to check.
    /// </param>
    /// <param name="options">
    /// Which checks to perform.
    /// </param>
    /// <returns>
    /// A <see cref="CollectionCheckResult"/> indicating the result of the checks.
    /// </returns>
    protected CollectionCheckResult CheckElementsState<TEnumerator>(
        in TEnumerator enumerator,
        CollectionCheckOptions options
    )
        where TEnumerator : IEnumerator<TElement>
    {
        // We create this separate method to avoid boxing the enumerator.

        if ((options & CollectionCheckOptions.Nullability) != 0)
        {
            if ((options & CollectionCheckOptions.TypeConsistency) != 0)
            {
                return CheckElementsNullabilityAndTypeConsistency(enumerator);
            }

            return CheckElementsNullability(enumerator);
        }

        if ((options & CollectionCheckOptions.TypeConsistency) != 0)
        {
            return CheckElementsTypeConsistency(enumerator);
        }

        Debug.Fail("Invalid options");
        return new CollectionCheckResult(true, null);
    }

    private static CollectionCheckResult CheckElementsNullability<TEnumerator>(TEnumerator enumerator)
        where TEnumerator : IEnumerator<TElement>
    {
        if (typeof(TElement).IsValueType && !NullableHelper.IsNullable(typeof(TElement)))
        {
            return CollectionCheckResult.FromNullability(false);
        }

        while (enumerator.MoveNext())
        {
            if (enumerator.Current is null)
            {
                return CollectionCheckResult.FromNullability(true);
            }
        }

        return CollectionCheckResult.FromNullability(false);
    }

    private static CollectionCheckResult CheckElementsTypeConsistency<TEnumerator>(TEnumerator enumerator)
        where TEnumerator : IEnumerator<TElement>
    {
        if (typeof(TElement).IsSealed)
        {
            return CollectionCheckResult.FromElementType(typeof(TElement));
        }
        Type? elementType = null;
        while (enumerator.MoveNext())
        {
            var element = enumerator.Current;
            if (element is null)
            {
                continue;
            }
            if (elementType is null)
            {
                elementType = element.GetType();
            }
            else if (elementType != element.GetType())
            {
                return CollectionCheckResult.FromElementType(null);
            }
        }

        return CollectionCheckResult.FromElementType(elementType ?? typeof(void));
    }

    private static CollectionCheckResult CheckElementsNullabilityAndTypeConsistency<TEnumerator>(TEnumerator enumerator)
        where TEnumerator : IEnumerator<TElement>
    {
        var hasNull = false;
        var hasDifferentType = false;
        if (typeof(TElement).IsSealed)
        {
            return CheckElementsNullability(enumerator);
        }

        Type? elementType = null;
        while (enumerator.MoveNext())
        {
            var element = enumerator.Current;
            if (element is null)
            {
                hasNull = true;
            }
            else
            {
                if (elementType is null)
                {
                    elementType = element.GetType();
                }
                else if (elementType != element.GetType())
                {
                    hasDifferentType = true;
                }
            }

            if (hasNull && hasDifferentType)
            {
                break;
            }
        }

        return new CollectionCheckResult(hasNull, hasDifferentType ? null : elementType);
    }

    private void WriteHeader(ref SerializationWriterRef writerRef, CollectionHeaderFlags flags)
    {
        if (_hasWrittenHeader)
        {
            return;
        }

        _hasWrittenHeader = writerRef.WriteUInt8((byte)flags);
    }

    private void WriteCount(ref SerializationWriterRef writerRef, in TCollection collection)
    {
        if (_hasWrittenCount)
        {
            return;
        }

        var count = GetCount(in collection);
        _hasWrittenCount = writerRef.Write7BitEncodedUInt32((uint)count);
    }

    protected abstract bool WriteElements(ref SerializationWriterRef writer, in TCollection collection);

    protected abstract int GetCount(in TCollection collection);

    private void ThrowNotSupportedException_TCollectionNotSupported([CallerMemberName] string methodName = "")
    {
        throw new NotSupportedException(
            $"The default implementation of {methodName} is not supported for {typeof(TCollection).Name}."
        );
    }

    [Flags]
    protected enum CollectionCheckOptions
    {
        Nullability = 0b1,
        TypeConsistency = 0b10,
    }

    protected readonly record struct CollectionCheckResult(bool HasNull, Type? ElementType)
    {
        public static CollectionCheckResult AllNullElements => new(true, null);

        public static CollectionCheckResult FromNullability(bool hasNull) => new(hasNull, null);

        public static CollectionCheckResult FromElementType(Type? elementType) => new(true, elementType);
    }
}

public abstract class CollectionDeserializer<TElement, TCollection>(TypeRegistration? elementRegistration = null)
    : AbstractDeserializer<TCollection>
    where TCollection : notnull
{
    private int? _count;
    private CollectionHeaderFlags? _headerFlags;

    protected TCollection? Collection;
    private TypeRegistration? _elementRegistration = elementRegistration;

    public override void Reset()
    {
        _count = null;
        _headerFlags = null;
        Collection = default;
    }

    public sealed override ReadValueResult<TCollection> Deserialize(DeserializationReader reader)
    {
        var task = Deserialize(reader, false, CancellationToken.None);
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    public sealed override ValueTask<ReadValueResult<TCollection>> DeserializeAsync(
        DeserializationReader reader,
        CancellationToken cancellationToken = default
    )
    {
        return Deserialize(reader, true, cancellationToken);
    }

    private async ValueTask<ReadValueResult<TCollection>> Deserialize(
        DeserializationReader reader,
        bool isAsync,
        CancellationToken cancellationToken = default
    )
    {
        if (_elementRegistration is null && typeof(TElement).IsSealed)
        {
            _elementRegistration = reader.TypeRegistry.GetTypeRegistration(typeof(TElement));
        }

        if (_count is null)
        {
            var countResult = await reader.Read7BitEncodedUint(isAsync, cancellationToken);
            if (!countResult.IsSuccess)
            {
                return ReadValueResult<TCollection>.Failed;
            }

            var count = (int)countResult.Value;
            _count = count;

            CreateCollection(count);
        }
        else
        {
            Debug.Assert(Collection is not null);
        }

        // TODO: Read header

        bool fillSuccess;
        if (isAsync)
        {
            fillSuccess = await ReadElementsAsync(reader, cancellationToken);
        }
        else
        {
            // ReSharper disable once MethodHasAsyncOverloadWithCancellation
            fillSuccess = ReadElements(reader);
        }

        if (!fillSuccess)
        {
            return ReadValueResult<TCollection>.Failed;
        }
        return ReadValueResult<TCollection>.FromValue(Collection);
    }

    protected ReadValueResult<TElement> ReadElement(DeserializationReader reader) { }

    protected ValueTask<ReadValueResult<TElement>> ReadElementAsync(DeserializationReader reader, CancellationToken cancellationToken = default) { }

    private protected async ValueTask<ReadValueResult<TElement?>> ReadElement(
        DeserializationReader reader,
        bool isAsync,
        CancellationToken cancellationToken
    )
    {
        if (_headerFlags is not { } headerFlags)
        {
            ThrowInvalidOperationException_HeaderNotRead();
            return ReadValueResult<TElement>.Failed;
        }

        var needReadRefMeta =
            (headerFlags & (CollectionHeaderFlags.TrackingRef | CollectionHeaderFlags.HasNull)) != 0;
        var needReadTypeMeta = (headerFlags & CollectionHeaderFlags.NotSameType) != 0;

        if (needReadRefMeta && needReadTypeMeta)
        {
            return await reader.Deserialize<TElement>(_elementRegistration, isAsync, cancellationToken);
        }
    }

    protected abstract bool ReadElements(DeserializationReader reader);

    protected abstract ValueTask<bool> ReadElementsAsync(
        DeserializationReader reader,
        CancellationToken cancellationToken
    );

    [MemberNotNull(nameof(Collection))]
    protected abstract void CreateCollection(int count);

    [DoesNotReturn]
    private void ThrowInvalidOperationException_HeaderNotRead()
    {
        throw new InvalidOperationException(
            $"Header not read yet. Call {nameof(ReadElement)} in {nameof(ReadElements)} " +
            $"or {nameof(ReadElementAsync)} in {nameof(ReadElementsAsync)}."
        );
    }
}

#region Built-in

internal sealed class ListSerializer<TElement>(TypeRegistration? elementRegistration)
    : CollectionSerializer<TElement, List<TElement>>(elementRegistration)
{
    private int _writtenCount;

    public override void Reset()
    {
        base.Reset();
        _writtenCount = 0;
    }

    protected override int GetCount(in List<TElement> list) => list.Count;

    protected override bool WriteElements(ref SerializationWriterRef writer, in List<TElement> collection)
    {
#if NET5_0_OR_GREATER
        var elementSpan = CollectionsMarshal.AsSpan(collection);
        for (; _writtenCount < elementSpan.Length; _writtenCount++)
        {
            if (!writer.Serialize(in elementSpan[_writtenCount]))
            {
                return false;
            }
        }

        return true;
#else
        foreach (var element in collection)
        {
            if (!writer.Serialize(in element))
            {
                return false;
            }
        }

        return true;
#endif
    }

    protected override CollectionCheckResult CheckElementsState(
        in List<TElement> collection,
        CollectionCheckOptions options
    )
    {
        return base.CheckElementsState(collection.GetEnumerator(), options);
    }
}

internal sealed class ListDeserializer<TElement>(TypeRegistration? elementRegistration)
    : CollectionDeserializer<TElement, List<TElement>>(elementRegistration)
{
    protected override void CreateCollection(int count) => new(count);

    protected override bool ReadElements(DeserializationReader reader) { }

    protected override ValueTask<bool> ReadElementsAsync(
        DeserializationReader reader,
        CancellationToken cancellationToken
    ) { }

    private async ValueTask<bool> ReadElements(
        DeserializationReader reader,
        bool isAsync,
        CancellationToken cancellationToken
    ) { }
}

internal sealed class ArraySerializer<TElement>(TypeRegistration? elementRegistration)
    : CollectionSerializer<TElement, TElement[]>(elementRegistration)
{
    protected override int GetCount(in TElement[] list) => list.Length;

    protected override bool TryGetSpan(in TElement[] list, out ReadOnlySpan<TElement> elementSpan)
    {
        elementSpan = list;
        return true;
    }
}

internal sealed class ArrayDeserializer<TElement>(TypeRegistration? elementRegistration)
    : CollectionDeserializer<TElement, TElement[]>(elementRegistration)
{
    protected override void CreateCollection(int count) => new TElement[count];

    protected override bool TryGetMemory(ref TElement[] list, out Memory<TElement> elementMemory)
    {
        elementMemory = list;
        return true;
    }
}

internal sealed class HashSetSerializer<TElement>(TypeRegistration? elementRegistration)
    : CollectionSerializer<TElement, HashSet<TElement>>(elementRegistration)
{
    protected override int GetCount(in HashSet<TElement> set)
    {
        return set.Count;
    }
}

internal sealed class HashSetDeserializer<TElement>(TypeRegistration? elementRegistration)
    : CollectionDeserializer<TElement, HashSet<TElement>>(elementRegistration)
{
    protected override void CreateCollection(int count)
    {
#if NETSTANDARD2_0
        return [];
#else
        return new HashSet<TElement>(count);
#endif
    }
}

#endregion
