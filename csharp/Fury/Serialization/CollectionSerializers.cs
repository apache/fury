using System;
using System.Buffers;
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
public enum CollectionHeaderFlags : byte
{
    TrackingRef = 0b1,
    HasNull = 0b10,
    NotDeclElementType = 0b100,
    NotSameType = 0b1000,
}

file static class CollectionSerializationHelper
{
    public static int GetSizeFactor<T>()
    {
        return typeof(T) switch
        {
            { } t when t == typeof(byte) => sizeof(byte),
            { } t when t == typeof(sbyte) => sizeof(sbyte),
            { } t when t == typeof(ushort) => sizeof(ushort),
            { } t when t == typeof(short) => sizeof(short),
            { } t when t == typeof(uint) => sizeof(uint),
            { } t when t == typeof(int) => sizeof(int),
            { } t when t == typeof(ulong) => sizeof(ulong),
            { } t when t == typeof(long) => sizeof(long),
#if NET5_0_OR_GREATER
            { } t when t == typeof(Half) => Unsafe.SizeOf<Half>(),
#endif
            { } t when t == typeof(float) => sizeof(float),
            { } t when t == typeof(double) => sizeof(double),
            { } t when t == typeof(bool) => sizeof(bool),
            _ => 1,
        };
    }

    public static bool TryGetByteSpan<TElement>(Span<TElement> elementSpan, out Span<byte> bytes)
    {
        switch (elementSpan)
        {
            case Span<byte> byteSpan:
                bytes = byteSpan;
                break;
            case Span<sbyte> sbyteSpan:
                bytes = MemoryMarshal.AsBytes(sbyteSpan);
                break;
            case Span<ushort> ushortSpan:
                bytes = MemoryMarshal.AsBytes(ushortSpan);
                break;
            case Span<short> shortSpan:
                bytes = MemoryMarshal.AsBytes(shortSpan);
                break;
            case Span<uint> uintSpan:
                bytes = MemoryMarshal.AsBytes(uintSpan);
                break;
            case Span<int> intSpan:
                bytes = MemoryMarshal.AsBytes(intSpan);
                break;
            case Span<ulong> ulongSpan:
                bytes = MemoryMarshal.AsBytes(ulongSpan);
                break;
            case Span<long> longSpan:
                bytes = MemoryMarshal.AsBytes(longSpan);
                break;

#if NET5_0_OR_GREATER
            case Span<Half> halfSpan:
                bytes = MemoryMarshal.AsBytes(halfSpan);
                break;
#endif
            case Span<float> floatSpan:
                bytes = MemoryMarshal.AsBytes(floatSpan);
                break;
            case Span<double> doubleSpan:
                bytes = MemoryMarshal.AsBytes(doubleSpan);
                break;
            case Span<bool> boolSpan:
                bytes = MemoryMarshal.AsBytes(boolSpan);
                break;
            default:
                bytes = Span<byte>.Empty;
                return false;
        }

        return true;
    }

    public static bool TryGetByteSpan<TElement>(ReadOnlySpan<TElement> elementSpan, out ReadOnlySpan<byte> bytes)
    {
        switch (elementSpan)
        {
            case ReadOnlySpan<byte> byteSpan:
                bytes = byteSpan;
                break;
            case ReadOnlySpan<sbyte> sbyteSpan:
                bytes = MemoryMarshal.AsBytes(sbyteSpan);
                break;
            case ReadOnlySpan<ushort> ushortSpan:
                bytes = MemoryMarshal.AsBytes(ushortSpan);
                break;
            case ReadOnlySpan<short> shortSpan:
                bytes = MemoryMarshal.AsBytes(shortSpan);
                break;
            case ReadOnlySpan<uint> uintSpan:
                bytes = MemoryMarshal.AsBytes(uintSpan);
                break;
            case ReadOnlySpan<int> intSpan:
                bytes = MemoryMarshal.AsBytes(intSpan);
                break;
            case ReadOnlySpan<ulong> ulongSpan:
                bytes = MemoryMarshal.AsBytes(ulongSpan);
                break;
            case ReadOnlySpan<long> longSpan:
                bytes = MemoryMarshal.AsBytes(longSpan);
                break;
#if NET5_0_OR_GREATER
            case ReadOnlySpan<Half> halfSpan:
                bytes = MemoryMarshal.AsBytes(halfSpan);
                break;
#endif
            case ReadOnlySpan<float> floatSpan:
                bytes = MemoryMarshal.AsBytes(floatSpan);
                break;
            case ReadOnlySpan<double> doubleSpan:
                bytes = MemoryMarshal.AsBytes(doubleSpan);
                break;
            case ReadOnlySpan<bool> boolSpan:
                bytes = MemoryMarshal.AsBytes(boolSpan);
                break;
            default:
                bytes = ReadOnlySpan<byte>.Empty;
                return false;
        }

        return true;
    }
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
    private int _currentIndex;

    protected TypeRegistration? ElementRegistration { get; set; } = elementRegistration;

    public override void Reset()
    {
        _hasWrittenHeader = false;
        _hasWrittenCount = false;
        _currentIndex = 0;

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

        if ((options & CollectionCheckOptions.Nullability) != 0)
        {
            if ((options & CollectionCheckOptions.TypeConsistency) != 0)
            {
                return CheckElementsNullabilityAndTypeConsistency(enumerable);
            }

            return CheckElementsNullability(enumerable);
        }

        if ((options & CollectionCheckOptions.TypeConsistency) != 0)
        {
            return CheckElementsTypeConsistency(enumerable);
        }

        Debug.Fail("Invalid options");
        return new CollectionCheckResult(true, null);
    }

    private CollectionCheckResult CheckElementsNullability(IEnumerable<TElement> enumerable)
    {
        if (typeof(TElement).IsValueType && !NullableHelper.IsNullable(typeof(TElement)))
        {
            return CollectionCheckResult.FromNullability(false);
        }

        foreach (var element in enumerable)
        {
            if (element is null)
            {
                return CollectionCheckResult.FromNullability(true);
            }
        }

        return CollectionCheckResult.FromNullability(false);
    }

    private CollectionCheckResult CheckElementsTypeConsistency(IEnumerable<TElement> enumerable)
    {
        if (typeof(TElement).IsSealed)
        {
            return CollectionCheckResult.FromElementType(typeof(TElement));
        }
        Type? elementType = null;
        foreach (var element in enumerable)
        {
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

    private CollectionCheckResult CheckElementsNullabilityAndTypeConsistency(IEnumerable<TElement> enumerable)
    {
        var hasNull = false;
        var hasDifferentType = false;
        if (typeof(TElement).IsSealed)
        {
            return CheckElementsNullability(enumerable);
        }

        Type? elementType = null;
        foreach (var element in enumerable)
        {
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

        _hasWrittenHeader = writerRef.Write((byte)flags);
    }

    private void WriteCount(ref SerializationWriterRef writerRef, in TCollection collection)
    {
        if (_hasWrittenCount)
        {
            return;
        }

        // Primitives have special handling in Fury.
        // The length of primitive collection should be serialized as the number of bytes.

        var sizeFactor = CollectionSerializationHelper.GetSizeFactor<TElement>();
        var count = GetCount(in collection) * sizeFactor;
        _hasWrittenCount = writerRef.Write7BitEncodedUint((uint)count);
    }

    private bool WriteElements(ref SerializationWriterRef writer, in TCollection collection)
    {
        if (TryGetSpan(in collection, out var elementSpan))
        {
            if (CollectionSerializationHelper.TryGetByteSpan(elementSpan, out var byteSpan))
            {
                _currentIndex += writer.Write(byteSpan);
                return _currentIndex == byteSpan.Length;
            }

            for (; _currentIndex < elementSpan.Length; _currentIndex++)
            {
                if (!writer.Serialize(in elementSpan[_currentIndex], ElementRegistration))
                {
                    return false;
                }
            }

            return true;
        }

        if (TryGetSequence(in collection, out var elementSequence))
        {
            var skipCount = 0;
            foreach (var elementMemory in elementSequence)
            {
                elementSpan = elementMemory.Span;
                if (CollectionSerializationHelper.TryGetByteSpan(elementSpan, out var byteSpan))
                {
                    if (byteSpan.Length <= _currentIndex - skipCount)
                    {
                        skipCount += byteSpan.Length;
                        continue;
                    }

                    byteSpan = byteSpan.Slice(_currentIndex - skipCount);
                    var writtenByteCount = writer.Write(byteSpan);
                    _currentIndex += writtenByteCount;
                    skipCount = _currentIndex;
                    if (writtenByteCount < byteSpan.Length)
                    {
                        return false;
                    }
                }
                else
                {
                    if (elementSpan.Length <= _currentIndex - skipCount)
                    {
                        skipCount += elementSpan.Length;
                        continue;
                    }

                    elementSpan = elementSpan.Slice(_currentIndex - skipCount);
                    for (var i = 0; i < elementSpan.Length; i++)
                    {
                        if (!writer.Serialize(in elementSpan[i], ElementRegistration))
                        {
                            return false;
                        }
                        _currentIndex++;
                        skipCount = _currentIndex;
                    }
                }
            }
        }

        if (collection is not IEnumerable<TElement> enumerableCollection)
        {
            ThrowNotSupportedException_TCollectionNotSupported();
            return false;
        }
        foreach (var element in enumerableCollection)
        {
            if (!writer.Serialize(in element, ElementRegistration))
            {
                return false;
            }
        }
        return true;
    }

    protected abstract int GetCount(in TCollection collection);

    protected virtual bool TryGetSpan(in TCollection collection, out ReadOnlySpan<TElement> elementSpan)
    {
        elementSpan = ReadOnlySpan<TElement>.Empty;
        return false;
    }

    protected virtual bool TryGetSequence(in TCollection collection, out ReadOnlySequence<TElement> elementSequence)
    {
        elementSequence = ReadOnlySequence<TElement>.Empty;
        return false;
    }

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
    private int _currentIndex;

    private object? _untypedCollection;
    private TCollection? _collection;
    private TypeRegistration? _elementRegistration = elementRegistration;

    public sealed override object ReferenceableObject
    {
        get
        {
            if (_collection is null)
            {
                ThrowInvalidOperationException_ObjectNotCreated();
            }

            _untypedCollection ??= _collection;
            return _untypedCollection;
        }
    }

    public override void Reset()
    {
        _count = null;
        _currentIndex = 0;
        _untypedCollection = null;
        _collection = default;
    }

    public override ReadValueResult<TCollection> Deserialize(DeserializationReader reader)
    {
        var task = Deserialize(reader, false, CancellationToken.None);
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    public override ValueTask<ReadValueResult<TCollection>> DeserializeAsync(
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
            var sizeFactor = CollectionSerializationHelper.GetSizeFactor<TElement>();
            if (count % sizeFactor != 0)
            {
                ThrowBadDeserializationInputException_InvalidByteCount(count);
            }

            _collection = CreateCollection(count / sizeFactor);
        }
        else
        {
            Debug.Assert(_collection is not null);
        }

        var fillSuccess = false;
        try
        {
            ref var collectionRef = ref Unsafe.NullRef<TCollection>();
            if (typeof(TCollection).IsValueType && _untypedCollection is null)
            {
                collectionRef = ref _collection!;
            }
            else
            {
                _untypedCollection ??= _collection;
                collectionRef = ref ReferenceHelper.UnboxOrGetInputRef<TCollection>(ref _untypedCollection);
            }
            if (TryGetMemory(ref collectionRef, out var elementMemory))
            {
                fillSuccess = await FillcollectionByMemory(reader, elementMemory, isAsync, cancellationToken);
            }
            else if (CanAddElement)
            {
                fillSuccess = await FillcollectionByAddElement(reader, isAsync, cancellationToken);
            }
            else if (_collection is ICollection<TElement>)
            {
                _untypedCollection ??= _collection;
                var collection = (ICollection<TElement>)_untypedCollection;
                fillSuccess = await FillcollectionByCollectionInterface(reader, collection, isAsync, cancellationToken);
            }
            else
            {
                ThrowNotSupportedException_TcollectionNotSupported();
            }
        }
        finally
        {
            // Copy the modified collection to the boxed collection if Tcollection is a value type.
            if (typeof(TCollection).IsValueType && _untypedCollection is not null)
            {
                ref var collectionRef = ref ReferenceHelper.UnboxOrGetNullRef<TCollection>(_untypedCollection);
                Debug.Assert(!Unsafe.IsNullRef(ref collectionRef));
                collectionRef = _collection;
            }
        }

        if (!fillSuccess)
        {
            return ReadValueResult<TCollection>.Failed;
        }
        return ReadValueResult<TCollection>.FromValue(_collection);
    }

    private async ValueTask<bool> FillcollectionByMemory(
        DeserializationReader reader,
        Memory<TElement> elementMemory,
        bool isAsync,
        CancellationToken cancellationToken
    )
    {
        var count = _count!.Value;
        var unreadCount = count - _currentIndex;
        var elementSpan = elementMemory.Span;
        if (CollectionSerializationHelper.TryGetByteSpan(elementSpan, out var byteSpan))
        {
            if (byteSpan.Length < count)
            {
                ThrowInvalidOperationException_SpanTooSmall(
                    elementSpan.Length,
                    count / CollectionSerializationHelper.GetSizeFactor<TElement>()
                );
            }

            var readResult = await reader.Read(count - _currentIndex, isAsync, cancellationToken);
            var buffer = readResult.Buffer;
            elementSpan = elementMemory.Span;
            CollectionSerializationHelper.TryGetByteSpan(elementSpan, out byteSpan);
            byteSpan = byteSpan.Slice(_currentIndex);
            var bufferLength = buffer.Length;
            if (unreadCount < bufferLength)
            {
                buffer = buffer.Slice(0, unreadCount);
                bufferLength = unreadCount;
            }
            buffer.CopyTo(byteSpan);
            reader.AdvanceTo(buffer.End);
            _currentIndex += (int)bufferLength;
            return _currentIndex == _count;
        }

        if (isAsync)
        {
            for (; _currentIndex < elementMemory.Length; _currentIndex++)
            {
                var readResult = await reader.DeserializeAsync<TElement>(_elementRegistration, cancellationToken);
                if (!readResult.IsSuccess)
                {
                    return false;
                }
                elementMemory.Span[_currentIndex] = readResult.Value!;
            }
        }
        else
        {
            for (; _currentIndex < elementMemory.Length; _currentIndex++)
            {
                var readResult = reader.Deserialize<TElement>(_elementRegistration);
                if (!readResult.IsSuccess)
                {
                    return false;
                }
                elementSpan[_currentIndex] = readResult.Value!;
            }
        }

        return true;
    }

    private async ValueTask<bool> FillcollectionByAddElement(
        DeserializationReader reader,
        bool isAsync,
        CancellationToken cancellationToken
    )
    {
        Debug.Assert(_collection is not null);
        var count = _count!.Value;
        if (typeof(TCollection).IsValueType && _untypedCollection is null)
        {
            for (; _currentIndex < count; _currentIndex++)
            {
                var readResult = await reader.Deserialize<TElement>(_elementRegistration, isAsync, cancellationToken);
                if (!readResult.IsSuccess)
                {
                    return false;
                }

                AddElement(ref _collection, readResult.Value!);
            }
        }
        else
        {
            _untypedCollection ??= _collection;
            for (; _currentIndex < count; _currentIndex++)
            {
                var readResult = await reader.Deserialize<TElement>(_elementRegistration, isAsync, cancellationToken);
                if (!readResult.IsSuccess)
                {
                    return false;
                }

                AddElement(_untypedCollection, readResult.Value!);
            }
        }

        return true;
    }

    private async ValueTask<bool> FillcollectionByCollectionInterface(
        DeserializationReader reader,
        ICollection<TElement> collection,
        bool isAsync,
        CancellationToken cancellationToken
    )
    {
        var count = _count!.Value;
        for (; _currentIndex < count; _currentIndex++)
        {
            var readResult = await reader.Deserialize<TElement>(_elementRegistration, isAsync, cancellationToken);
            if (!readResult.IsSuccess)
            {
                return false;
            }

            collection.Add(readResult.Value!);
        }
        return true;
    }

    protected abstract TCollection CreateCollection(int count);

    /// <summary>
    /// Try to get the <see cref="Memory{T}"/> which represents the elements in the collection.
    /// </summary>
    /// <param name="collection">
    /// The collection to which the element will be added.
    /// </param>
    /// <param name="elementMemory">
    /// The <see cref="Memory{T}"/> which represents the elements in the collection.
    /// </param>
    /// <returns>
    /// True if the <see cref="Memory{T}"/> was successfully obtained, false otherwise.
    /// </returns>
    /// <remarks>
    /// When this method returns false, the <see cref="CanAddElement"/> property will be checked.
    /// </remarks>
    protected virtual bool TryGetMemory(ref TCollection collection, out Memory<TElement> elementMemory)
    {
        elementMemory = Memory<TElement>.Empty;
        return false;
    }

    /// <summary>
    /// Indicates if the <see cref="AddElement(object, in TElement)"/> and <see cref="AddElement(ref TCollection,in TElement)"/>
    /// can be used to add elements to the collection.
    /// </summary>
    /// <remarks>
    /// If true, the <see cref="AddElement(object, in TElement)"/> and <see cref="AddElement(ref TCollection,in TElement)"/>
    /// must be overridden.
    /// </remarks>
    protected virtual bool CanAddElement => false;

    /// <inheritdoc cref="AddElement(object,in TElement)"/>
    /// <remarks>
    /// <para>
    /// This method will not be called if <see cref="CanAddElement"/> is false.
    /// </para>
    /// <para>
    /// This method is designed to avoid boxing when <typeparamref name="TCollection"/> is a value type.
    /// If the collection is boxed, the <see cref="AddElement(object, in TElement)"/> method will be called instead.
    /// </para>
    /// </remarks>
    protected virtual void AddElement(ref TCollection collection, in TElement element)
    {
        ThrowNotSupportedException_AddElementNotOverridden();
    }

    /// <summary>
    /// Adds an element to the collection.
    /// </summary>
    /// <param name="collection">
    /// The collection to which the element will be added.
    /// </param>
    /// <param name="element">
    /// The element to add.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Thrown if not overridden.
    /// </exception>
    /// <remarks>
    /// <para>
    /// This method will not be called if <see cref="CanAddElement"/> is false.
    /// </para>
    /// </remarks>
    protected virtual void AddElement(object collection, in TElement element)
    {
        // Unlike TryGetMemory, AddElement is called once for each element.
        // Since ref local variables can't be accessed across an await boundary,
        // if the collection is a value type and has already been boxed,
        // each call needs to obtain a ref to Tcollection from _untypedCollection via unsafe means,
        // which could result in a large number of virtual calls.
        // Therefore, we provide an overload that accepts an object type to optionally allow users to avoid this issue.

        ref var collectionRef = ref ReferenceHelper.UnboxOrGetInputRef<TCollection>(ref collection);
        AddElement(ref collectionRef, in element);
    }

    [DoesNotReturn]
    private static void ThrowBadDeserializationInputException_InvalidByteCount(int count)
    {
        throw new BadDeserializationInputException(
            $"{count} is not a valid byte count for {typeof(TCollection).Name}, "
                + $"it should be a multiple of {CollectionSerializationHelper.GetSizeFactor<TElement>()}."
        );
    }

    [DoesNotReturn]
    private static void ThrowInvalidOperationException_SpanTooSmall(int providedCount, int requiredCount)
    {
        throw new InvalidOperationException(
            $"The provided span is too small. " + $"Expected {requiredCount} elements, but got {providedCount}."
        );
    }

    [DoesNotReturn]
    private static void ThrowNotSupportedException_TcollectionNotSupported()
    {
        throw new NotSupportedException(
            $"{nameof(TryGetMemory)} or {nameof(CanAddElement)} is not overridden, "
                + $"or {typeof(TCollection).Name} does not implement {nameof(ICollection<TElement>)}"
        );
    }

    [DoesNotReturn]
    private static void ThrowNotSupportedException_AddElementNotOverridden()
    {
        throw new InvalidOperationException(
            $"Unable to add element. {nameof(AddElement)} must be overridden if {nameof(CanAddElement)} is true."
        );
    }
}

#region Built-in

internal sealed class ListSerializer<TElement>(TypeRegistration? elementRegistration)
    : CollectionSerializer<TElement, List<TElement>>(elementRegistration)
{
    protected override int GetCount(in List<TElement> list) => list.Count;

    protected override bool TryGetSpan(in List<TElement> list, out ReadOnlySpan<TElement> elementSpan)
    {
#if NET5_0_OR_GREATER
        elementSpan = CollectionsMarshal.AsSpan(list);
        return true;
#else
        elementSpan = ReadOnlySpan<TElement>.Empty;
        return false;
#endif
    }
}

internal sealed class ListDeserializer<TElement>(TypeRegistration? elementRegistration)
    : CollectionDeserializer<TElement, List<TElement>>(elementRegistration)
{
#if NET5_0_OR_GREATER
    private readonly ListMemoryManager<TElement> _listMemoryManager = new();

    protected override bool TryGetMemory(ref List<TElement> list, out Memory<TElement> elementMemory)
    {
        _listMemoryManager.List = _listMemoryManager.List;
        elementMemory = _listMemoryManager.Memory;
        return true;
    }
#endif

    protected override List<TElement> CreateCollection(int count) => new(count);
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
    protected override TElement[] CreateCollection(int count) => new TElement[count];

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
    protected override HashSet<TElement> CreateCollection(int count)
    {
#if NETSTANDARD2_0
        return [];
#else
        return new HashSet<TElement>(count);
#endif
    }
}

#endregion
