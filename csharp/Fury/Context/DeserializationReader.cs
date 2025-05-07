using System;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Fury.Collections;
using Fury.Meta;
using Fury.Serialization;
using Fury.Serialization.Meta;
using JetBrains.Annotations;

namespace Fury.Context;

public sealed class DeserializationReader
{
    private sealed class Frame
    {
        public object? Value;
        public TypeRegistration? Registration;
        public RefMetadata? RefMetadata;
        public IDeserializer? Deserializer;

        public void Reset()
        {
            Debug.Assert(Registration is not null || Deserializer is null);
            if (Registration is not null && Deserializer is not null)
            {
                Registration.ReturnDeserializer(Deserializer);
            }
            Value = null;
            Registration = null;
            RefMetadata = null;
            Deserializer = null;
        }
    }

    public TypeRegistry TypeRegistry { get; }
    private readonly MetaStringStorage _metaStringStorage;

    public DeserializationConfig Config { get; private set; } = DeserializationConfig.Default;
    private readonly BatchReader _innerReader = new();

    private readonly HeaderDeserializer _headerDeserializer = new();
    private readonly ReferenceMetaDeserializer _referenceMetaDeserializer = new();
    private readonly TypeMetaDeserializer _typeMetaDeserializer;

    internal AutoIncrementIdDictionary<MetaString> MetaStringContext { get; } = new();
    private readonly FrameStack<Frame> _frameStack = new();

    internal DeserializationReader(TypeRegistry registry, MetaStringStorage metaStringStorage)
    {
        TypeRegistry = registry;
        _metaStringStorage = metaStringStorage;
        _typeMetaDeserializer = CreateTypeMetaDeserializer();
        _typeMetaDeserializer.Initialize(MetaStringContext);
    }

    internal void Reset()
    {
        _innerReader.Reset();
        _headerDeserializer.Reset();
        _referenceMetaDeserializer.Reset();
        _typeMetaDeserializer.Reset();
        foreach (var frame in _frameStack.Frames)
        {
            frame.Reset();
        }
        _frameStack.Reset();
    }

    private void ResetCurrent()
    {
        _referenceMetaDeserializer.ResetCurrent();
        _typeMetaDeserializer.ResetCurrent();
    }

    internal void Initialize(PipeReader pipeReader, DeserializationConfig config)
    {
        Config = config;
        _innerReader.Initialize(pipeReader);
    }

    internal TypeMetaDeserializer CreateTypeMetaDeserializer() => new(TypeRegistry, _metaStringStorage);

    private void OnCurrentDeserializationCompleted(bool isSuccess)
    {
        if (isSuccess)
        {
            ResetCurrent();
            _frameStack.PopFrame().Reset();
        }
        else
        {
            _frameStack.MoveLast();
        }
    }

    internal ValueTask<ReadValueResult<bool>> ReadHeader(bool isAsync, CancellationToken cancellationToken)
    {
        return _headerDeserializer.Read(this, isAsync, cancellationToken);
    }

    // TODO: Fast path for primitive types and string

    [MustUseReturnValue]
    public ReadValueResult<TTarget?> Deserialize<TTarget>(TypeRegistration? registrationHint = null)
    {
        var task = Deserialize<TTarget>(registrationHint, false, CancellationToken.None);
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    [MustUseReturnValue]
    public ValueTask<ReadValueResult<TTarget?>> DeserializeAsync<TTarget>(
        TypeRegistration? registrationHint = null,
        CancellationToken cancellationToken = default
    )
    {
        return Deserialize<TTarget>(registrationHint, true, cancellationToken);
    }

    [MustUseReturnValue]
    internal async ValueTask<ReadValueResult<TTarget?>> Deserialize<TTarget>(
        TypeRegistration? registrationHint,
        bool isAsync,
        CancellationToken cancellationToken
    )
    {
        _frameStack.MoveNext();
        var isSuccess = false;
        try
        {
            isSuccess = await ReadRefMeta(isAsync, cancellationToken);
            if (!isSuccess)
            {
                return ReadValueResult<TTarget?>.Failed;
            }
            var valueResult = await ReadValue<TTarget>(registrationHint, isAsync, cancellationToken);
            isSuccess = valueResult.IsSuccess;
            return valueResult;
        }
        finally
        {
            OnCurrentDeserializationCompleted(isSuccess);
        }
    }

    [MustUseReturnValue]
    public ReadValueResult<TTarget?> DeserializeNullable<TTarget>(TypeRegistration? registrationHint = null)
        where TTarget : struct
    {
        var task = DeserializeNullable<TTarget>(registrationHint, false, CancellationToken.None);
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    [MustUseReturnValue]
    public async ValueTask<ReadValueResult<TTarget?>> DeserializeNullableAsync<TTarget>(
        TypeRegistration? registrationHint = null,
        CancellationToken cancellationToken = default
    )
        where TTarget : struct
    {
        return await DeserializeNullable<TTarget>(registrationHint, true, cancellationToken);
    }

    [MustUseReturnValue]
    internal async ValueTask<ReadValueResult<TTarget?>> DeserializeNullable<TTarget>(
        TypeRegistration? registrationHint,
        bool isAsync,
        CancellationToken cancellationToken
    )
        where TTarget : struct
    {
        _frameStack.MoveNext();
        var isSuccess = false;
        try
        {
            var refMetaResult = await ReadRefMeta(isAsync, cancellationToken);
            if (!refMetaResult)
            {
                return ReadValueResult<TTarget?>.Failed;
            }

            if (_frameStack.CurrentFrame.RefMetadata is { RefFlag: RefFlag.Null })
            {
                return ReadValueResult<TTarget?>.FromValue(null);
            }

            var valueResult = await ReadValue<TTarget>(registrationHint, isAsync, cancellationToken);
            isSuccess = valueResult.IsSuccess;
            if (!isSuccess)
            {
                return ReadValueResult<TTarget?>.Failed;
            }

            return ReadValueResult<TTarget?>.FromValue(valueResult.Value);
        }
        finally
        {
            OnCurrentDeserializationCompleted(isSuccess);
        }
    }

    private async ValueTask<bool> ReadRefMeta(bool isAsync, CancellationToken cancellationToken)
    {
        var currentFrame = _frameStack.CurrentFrame;
        if (!_frameStack.IsCurrentTheLastFrame || currentFrame.RefMetadata is not null)
        {
            return true;
        }
        var readResult = await _referenceMetaDeserializer.Read(this, isAsync, cancellationToken);
        if (!readResult.IsSuccess)
        {
            return false;
        }
        currentFrame.RefMetadata = readResult.Value;
        return true;
    }

    private async ValueTask<ReadValueResult<TTarget?>> ReadValue<TTarget>(
        TypeRegistration? registrationHint,
        bool isAsync,
        CancellationToken cancellationToken
    )
    {
        var currentFrame = _frameStack.CurrentFrame;
        switch (currentFrame.RefMetadata)
        {
            case { RefFlag: RefFlag.Null }:
                // Maybe we should throw an exception here for value types
                return ReadValueResult<TTarget?>.FromValue(default);
            case { RefFlag: RefFlag.Ref, RefId: var refId }:
                _referenceMetaDeserializer.GetReadValue(refId, out var readValue);
                return ReadValueResult<TTarget?>.FromValue((TTarget)readValue);
            case { RefFlag: RefFlag.RefValue }:
                if (!await ReadTypeMeta(typeof(TTarget), registrationHint, isAsync, cancellationToken))
                {
                    return ReadValueResult<TTarget?>.Failed;
                }
                if (!await ReadReferenceable(isAsync, cancellationToken))
                {
                    return ReadValueResult<TTarget?>.Failed;
                }
                return ReadValueResult<TTarget?>.FromValue((TTarget?)currentFrame.Value);
            case { RefFlag: RefFlag.NotNullValue }:
                if (!await ReadTypeMeta(typeof(TTarget), registrationHint, isAsync, cancellationToken))
                {
                    return ReadValueResult<TTarget?>.Failed;
                }

                return (await ReadUnreferenceable<TTarget>(isAsync, cancellationToken))!;
            default:
                ThrowHelper.ThrowUnreachableException();
                return ReadValueResult<TTarget>.Failed;
        }
    }

    private async ValueTask<bool> ReadTypeMeta(
        Type declaredType,
        TypeRegistration? registrationHint,
        bool isAsync,
        CancellationToken cancellationToken
    )
    {
        var currentFrame = _frameStack.CurrentFrame;
        if (!_frameStack.IsCurrentTheLastFrame || currentFrame.Registration is not null)
        {
            return true;
        }

        var typeMetaResult = await _typeMetaDeserializer.Read(
            this,
            declaredType,
            registrationHint,
            isAsync,
            cancellationToken
        );
        if (!typeMetaResult.IsSuccess)
        {
            return false;
        }
        currentFrame.Registration = typeMetaResult.Value;
        currentFrame.Deserializer = currentFrame.Registration.RentDeserializer();
        return true;
    }

    private async ValueTask<bool> ReadReferenceable(bool isAsync, CancellationToken cancellationToken)
    {
        var currentFrame = _frameStack.CurrentFrame;
        if (currentFrame.Value is not null)
        {
            return true;
        }

        var deserializer = currentFrame.Deserializer!;

        var createResult = ReadValueResult<object>.Failed;
        try
        {
            if (currentFrame.RefMetadata is { RefFlag: RefFlag.RefValue, RefId: var refId })
            {
                // Associate the deserializer with the reference ID before deserialization.
                // So that we can use it to get partial deserialization results when circular references occur.
                _referenceMetaDeserializer.AddInProgressDeserializer(refId, deserializer);
            }
            if (isAsync)
            {
                createResult = await deserializer.DeserializeAsync(this, cancellationToken);
            }
            else
            {
                // ReSharper disable once MethodHasAsyncOverloadWithCancellation
                createResult = deserializer.Deserialize(this);
            }

            return createResult.IsSuccess;
        }
        finally
        {
            if (createResult.IsSuccess)
            {
                currentFrame.Value = createResult.Value;

                if (currentFrame.RefMetadata is { RefFlag: RefFlag.RefValue, RefId: var refId })
                {
                    // If no circular reference occurs, the ReferenceableObject of deserializer will not be called.
                    // To make the result referenceable, we need to associate it with the reference ID here.
                    _referenceMetaDeserializer.AddReadValue(refId, createResult.Value);
                }
                Debug.Assert(createResult.Value is not null);
            }
        }
    }

    private async ValueTask<ReadValueResult<TTarget>> ReadUnreferenceable<TTarget>(
        bool isAsync,
        CancellationToken cancellationToken
    )
    {
        var deserializer = _frameStack.CurrentFrame.Deserializer!;
        if (deserializer is not IDeserializer<TTarget> typedDeserializer)
        {
            ReadValueResult<object> untypedResult;
            if (isAsync)
            {
                untypedResult = await deserializer.DeserializeAsync(this, cancellationToken);
            }
            else
            {
                // ReSharper disable once MethodHasAsyncOverloadWithCancellation
                untypedResult = deserializer.Deserialize(this);
            }

            if (!untypedResult.IsSuccess)
            {
                return ReadValueResult<TTarget>.Failed;
            }
            return ReadValueResult<TTarget>.FromValue((TTarget)untypedResult.Value);
        }

        // If the declared type matches the deserializer type,
        // we can use the typed deserializer for better performance.

        ReadValueResult<TTarget> result;
        if (isAsync)
        {
            result = await typedDeserializer.DeserializeAsync(this, cancellationToken);
        }
        else
        {
            // ReSharper disable once MethodHasAsyncOverloadWithCancellation
            result = typedDeserializer.Deserialize(this);
        }

        return result;
    }

    public ReadResult Read(int sizeHint = 0)
    {
        return _innerReader.Read(sizeHint);
    }

    public ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
    {
        return _innerReader.ReadAsync(0, cancellationToken);
    }

    public ValueTask<ReadResult> ReadAsync(int sizeHint = 0, CancellationToken cancellationToken = default)
    {
        return _innerReader.ReadAsync(sizeHint, cancellationToken);
    }

    internal async ValueTask<ReadResult> Read(int sizeHint, bool isAsync, CancellationToken cancellationToken)
    {
        if (isAsync)
        {
            return await ReadAsync(sizeHint, cancellationToken);
        }

        // ReSharper disable once MethodHasAsyncOverloadWithCancellation
        return Read(sizeHint);
    }

    public void AdvanceTo(SequencePosition consumed)
    {
        _innerReader.AdvanceTo(consumed);
    }

    public void AdvanceTo(SequencePosition consumed, SequencePosition examined)
    {
        _innerReader.AdvanceTo(consumed, examined);
    }

    #region Read Methods

    private ReadValueResult<T> ReadUnmanagedCommon<T>(in ReadResult sequenceResult)
        where T : unmanaged
    {
        var size = Unsafe.SizeOf<T>();
        var buffer = sequenceResult.Buffer;
        var bufferLength = buffer.Length;
        if (bufferLength < size)
        {
            AdvanceTo(buffer.Start, buffer.End);
            return ReadValueResult<T>.Failed;
        }
        if (bufferLength > size)
        {
            buffer = buffer.Slice(size);
        }
        T value = default;
        var destination = MemoryMarshal.AsBytes(SpanHelper.CreateSpan(ref value, 1));
        buffer.CopyTo(destination);
        AdvanceTo(buffer.End);
        return ReadValueResult<T>.FromValue(in value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ReadValueResult<T> ReadUnmanagedAs<T>(int size)
        where T : unmanaged
    {
        var sequenceResult = Read(size);
        return ReadUnmanagedCommon<T>(in sequenceResult);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal async ValueTask<ReadValueResult<T>> ReadUnmanagedAsAsync<T>(int size, CancellationToken cancellationToken)
        where T : unmanaged
    {
        var sequenceResult = await ReadAsync(size, cancellationToken);
        return ReadUnmanagedCommon<T>(in sequenceResult);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<ReadValueResult<T>> ReadUnmanagedAs<T>(
        int size,
        bool isAsync,
        CancellationToken cancellationToken
    )
        where T : unmanaged
    {
        if (isAsync)
        {
            return ReadUnmanagedAsAsync<T>(size, cancellationToken);
        }

        return new ValueTask<ReadValueResult<T>>(ReadUnmanagedAs<T>(size));
    }

    public ReadValueResult<byte> ReadUInt8() => ReadUnmanagedAs<byte>(sizeof(byte));

    public ReadValueResult<sbyte> ReadInt8() => ReadUnmanagedAs<sbyte>(sizeof(sbyte));

    public ReadValueResult<ushort> ReadUInt16() => ReadUnmanagedAs<ushort>(sizeof(ushort));

    public ReadValueResult<short> ReadInt16() => ReadUnmanagedAs<short>(sizeof(short));

    public ReadValueResult<uint> ReadUInt32() => ReadUnmanagedAs<uint>(sizeof(uint));

    public ReadValueResult<int> ReadInt32() => ReadUnmanagedAs<int>(sizeof(int));

    public ReadValueResult<ulong> ReadUInt64() => ReadUnmanagedAs<ulong>(sizeof(ulong));

    public ReadValueResult<long> ReadInt64() => ReadUnmanagedAs<long>(sizeof(long));

    public ReadValueResult<float> ReadFloat32() => ReadUnmanagedAs<float>(sizeof(float));

    public ReadValueResult<double> ReadFloat64() => ReadUnmanagedAs<double>(sizeof(double));

    public ReadValueResult<bool> ReadBoolean() => ReadUnmanagedAs<bool>(sizeof(bool));

    public async ValueTask<ReadValueResult<byte>> ReadUInt8Async(CancellationToken cancellationToken = default) =>
        await ReadUnmanagedAsAsync<byte>(sizeof(byte), cancellationToken);

    public async ValueTask<ReadValueResult<sbyte>> ReadInt8Async(CancellationToken cancellationToken = default) =>
        await ReadUnmanagedAsAsync<sbyte>(sizeof(sbyte), cancellationToken);

    public async ValueTask<ReadValueResult<ushort>> ReadUInt16Async(CancellationToken cancellationToken = default) =>
        await ReadUnmanagedAsAsync<ushort>(sizeof(ushort), cancellationToken);

    public async ValueTask<ReadValueResult<short>> ReadInt16Async(CancellationToken cancellationToken = default) =>
        await ReadUnmanagedAsAsync<short>(sizeof(short), cancellationToken);

    public async ValueTask<ReadValueResult<uint>> ReadUInt32Async(CancellationToken cancellationToken = default) =>
        await ReadUnmanagedAsAsync<uint>(sizeof(uint), cancellationToken);

    public async ValueTask<ReadValueResult<int>> ReadInt32Async(CancellationToken cancellationToken = default) =>
        await ReadUnmanagedAsAsync<int>(sizeof(int), cancellationToken);

    public async ValueTask<ReadValueResult<ulong>> ReadUInt64Async(CancellationToken cancellationToken = default) =>
        await ReadUnmanagedAsAsync<ulong>(sizeof(ulong), cancellationToken);

    public async ValueTask<ReadValueResult<long>> ReadInt64Async(CancellationToken cancellationToken = default) =>
        await ReadUnmanagedAsAsync<long>(sizeof(long), cancellationToken);

    public async ValueTask<ReadValueResult<float>> ReadFloat32Async(CancellationToken cancellationToken = default) =>
        await ReadUnmanagedAsAsync<float>(sizeof(float), cancellationToken);

    public async ValueTask<ReadValueResult<double>> ReadFloat64Async(CancellationToken cancellationToken = default) =>
        await ReadUnmanagedAsAsync<double>(sizeof(double), cancellationToken);

    public async ValueTask<ReadValueResult<bool>> ReadBooleanAsync(CancellationToken cancellationToken = default) =>
        await ReadUnmanagedAsAsync<bool>(sizeof(bool), cancellationToken);

    public ValueTask<ReadValueResult<byte>> ReadUInt8(bool isAsync, CancellationToken cancellationToken = default)
    {
        return isAsync ? ReadUInt8Async(cancellationToken) : new ValueTask<ReadValueResult<byte>>(ReadUInt8());
    }

    public ValueTask<ReadValueResult<sbyte>> ReadInt8(bool isAsync, CancellationToken cancellationToken = default)
    {
        return isAsync ? ReadInt8Async(cancellationToken) : new ValueTask<ReadValueResult<sbyte>>(ReadInt8());
    }

    public ValueTask<ReadValueResult<ushort>> ReadUInt16(bool isAsync, CancellationToken cancellationToken = default)
    {
        return isAsync ? ReadUInt16Async(cancellationToken) : new ValueTask<ReadValueResult<ushort>>(ReadUInt16());
    }

    public ValueTask<ReadValueResult<short>> ReadInt16(bool isAsync, CancellationToken cancellationToken = default)
    {
        return isAsync ? ReadInt16Async(cancellationToken) : new ValueTask<ReadValueResult<short>>(ReadInt16());
    }

    public ValueTask<ReadValueResult<uint>> ReadUInt32(bool isAsync, CancellationToken cancellationToken = default)
    {
        return isAsync ? ReadUInt32Async(cancellationToken) : new ValueTask<ReadValueResult<uint>>(ReadUInt32());
    }

    public ValueTask<ReadValueResult<int>> ReadInt32(bool isAsync, CancellationToken cancellationToken = default)
    {
        return isAsync ? ReadInt32Async(cancellationToken) : new ValueTask<ReadValueResult<int>>(ReadInt32());
    }

    public ValueTask<ReadValueResult<ulong>> ReadUInt64(bool isAsync, CancellationToken cancellationToken = default)
    {
        return isAsync ? ReadUInt64Async(cancellationToken) : new ValueTask<ReadValueResult<ulong>>(ReadUInt64());
    }

    public ValueTask<ReadValueResult<long>> ReadInt64(bool isAsync, CancellationToken cancellationToken = default)
    {
        return isAsync ? ReadInt64Async(cancellationToken) : new ValueTask<ReadValueResult<long>>(ReadInt64());
    }

    public ValueTask<ReadValueResult<float>> ReadFloat32(bool isAsync, CancellationToken cancellationToken = default)
    {
        return isAsync ? ReadFloat32Async(cancellationToken) : new ValueTask<ReadValueResult<float>>(ReadFloat32());
    }

    public ValueTask<ReadValueResult<double>> ReadFloat64(bool isAsync, CancellationToken cancellationToken = default)
    {
        return isAsync ? ReadFloat64Async(cancellationToken) : new ValueTask<ReadValueResult<double>>(ReadFloat64());
    }

    public ValueTask<ReadValueResult<bool>> ReadBoolean(bool isAsync, CancellationToken cancellationToken = default)
    {
        return isAsync ? ReadBooleanAsync(cancellationToken) : new ValueTask<ReadValueResult<bool>>(ReadBoolean());
    }

    private const int MaxBytesOfVarInt32 = 5;

    public ReadValueResult<uint> Read7BitEncodedUint()
    {
        var task = Read7BitEncodedUint(false, CancellationToken.None);
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    public ReadValueResult<int> Read7BitEncodedInt()
    {
        var uintResult = Read7BitEncodedUint();
        if (!uintResult.IsSuccess)
        {
            return ReadValueResult<int>.Failed;
        }
        var value = (int)BitOperations.RotateRight(uintResult.Value, 1);
        return ReadValueResult<int>.FromValue(value);
    }

    public ValueTask<ReadValueResult<uint>> Read7BitEncodedUintAsync(CancellationToken cancellationToken = default)
    {
        return Read7BitEncodedUint(true, cancellationToken);
    }

    public async ValueTask<ReadValueResult<int>> Read7BitEncodedIntAsync(CancellationToken cancellationToken = default)
    {
        var uintResult = await Read7BitEncodedUintAsync(cancellationToken);
        if (!uintResult.IsSuccess)
        {
            return ReadValueResult<int>.Failed;
        }
        var value = (int)BitOperations.RotateRight(uintResult.Value, 1);
        return ReadValueResult<int>.FromValue(value);
    }

    internal async ValueTask<ReadValueResult<uint>> Read7BitEncodedUint(
        bool isAsync,
        CancellationToken cancellationToken
    )
    {
        uint value = 0;
        var reader = new SequenceReader<byte>(ReadOnlySequence<byte>.Empty);

        while (reader.Consumed < MaxBytesOfVarInt32)
        {
            var consumed = (int)reader.Consumed;
            if (!reader.TryRead(out var currentByte))
            {
                if (reader.Consumed > 0)
                {
                    AdvanceTo(reader.Sequence.Start, reader.Position);
                }

                // We do not check if the sequenceResult is success because varint32 may be less than 5 bytes.
                var bytesResult = await Read(MaxBytesOfVarInt32 - consumed, isAsync, cancellationToken);
                reader = new SequenceReader<byte>(bytesResult.Buffer);
                Debug.Assert(reader.Length >= consumed);
                reader.Advance(consumed);

                if (!reader.TryRead(out currentByte))
                {
                    return ReadValueResult<uint>.Failed;
                }
            }

            if (consumed < MaxBytesOfVarInt32 - 1)
            {
                value |= ((uint)currentByte & 0x7F) << (consumed * 7);
                if ((currentByte & 0x80) == 0)
                {
                    AdvanceTo(reader.Position);
                    break;
                }
            }
            else
            {
                if (currentByte > 0b_1111u)
                {
                    ThrowBadDeserializationInputException_VarInt32Overflow();
                }

                value |= (uint)currentByte << ((MaxBytesOfVarInt32 - 1) * 7);
                AdvanceTo(reader.Position);
            }
        }
        return ReadValueResult<uint>.FromValue(in value);
    }

    [DoesNotReturn]
    private static void ThrowBadDeserializationInputException_VarInt32Overflow()
    {
        throw new InvalidOperationException("VarInt32 overflow.");
    }

    private const int MaxBytesOfVarInt64 = 9;

    public ReadValueResult<ulong> Read7BitEncodedUlong()
    {
        var task = Read7BitEncodedUlong(false, CancellationToken.None);
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    public ReadValueResult<long> Read7BitEncodedLong()
    {
        var ulongResult = Read7BitEncodedUlong();
        if (!ulongResult.IsSuccess)
        {
            return ReadValueResult<long>.Failed;
        }
        var value = (long)BitOperations.RotateRight(ulongResult.Value, 1);
        return ReadValueResult<long>.FromValue(value);
    }

    public ValueTask<ReadValueResult<ulong>> Read7BitEncodedUlongAsync(CancellationToken cancellationToken = default)
    {
        return Read7BitEncodedUlong(true, cancellationToken);
    }

    public async ValueTask<ReadValueResult<long>> Read7BitEncodedLongAsync(
        CancellationToken cancellationToken = default
    )
    {
        var ulongResult = await Read7BitEncodedUlongAsync(cancellationToken);
        if (!ulongResult.IsSuccess)
        {
            return ReadValueResult<long>.Failed;
        }
        var value = (long)BitOperations.RotateRight(ulongResult.Value, 1);
        return ReadValueResult<long>.FromValue(value);
    }

    internal async ValueTask<ReadValueResult<ulong>> Read7BitEncodedUlong(
        bool isAsync,
        CancellationToken cancellationToken = default
    )
    {
        ulong value = 0;
        var reader = new SequenceReader<byte>(ReadOnlySequence<byte>.Empty);

        while (reader.Consumed < MaxBytesOfVarInt64)
        {
            var consumed = (int)reader.Consumed;
            if (!reader.TryRead(out var currentByte))
            {
                if (reader.Consumed > 0)
                {
                    AdvanceTo(reader.Sequence.Start, reader.Position);
                }

                // We do not check if the sequenceResult is success because varint64 may be less than 9 bytes.
                var bytesResult = await Read(MaxBytesOfVarInt64 - consumed, isAsync, cancellationToken);
                reader = new SequenceReader<byte>(bytesResult.Buffer);
                Debug.Assert(reader.Length >= consumed);
                reader.Advance(consumed);

                if (!reader.TryRead(out currentByte))
                {
                    return ReadValueResult<ulong>.Failed;
                }
            }

            if (consumed < MaxBytesOfVarInt64 - 1)
            {
                value |= ((ulong)currentByte & 0x7F) << (consumed * 7);
                if ((currentByte & 0x80) == 0)
                {
                    AdvanceTo(reader.Position);
                    break;
                }
            }
            else
            {
                value |= (ulong)currentByte << ((MaxBytesOfVarInt64 - 1) * 7);
                AdvanceTo(reader.Position);
            }
        }
        return ReadValueResult<ulong>.FromValue(in value);
    }
    #endregion
}
