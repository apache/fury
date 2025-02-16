using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Fury.Collections;
using Fury.Meta;
using Fury.Serialization;
using Fury.Serialization.Meta;

namespace Fury.Context;

// Async methods do not work with ref, so DeserializationContext is a class

public sealed class DeserializationContext
{
    public Fury Fury { get; }
    private readonly BatchReader _reader;
    private readonly TypeRegistry _typeRegistry;

    private ReferenceMetaDeserializer _referenceMetaDeserializer = new();
    private TypeMetaDeserializer _typeMetaDeserializer;

    private SpannableList<Frame> _uncompletedFrameStack = [];
    private int _currentFrameIndex = 0;
    private int _depth = 0;

    internal DeserializationContext(Fury fury, BatchReader reader)
    {
        Fury = fury;
        _reader = reader;
        _typeRegistry = fury.TypeRegistry;
        _typeMetaDeserializer = new TypeMetaDeserializer(fury.TypeRegistry);
    }

    internal void Reset(bool clearRecords)
    {
        _referenceMetaDeserializer.Reset(clearRecords);
        _typeMetaDeserializer.Reset(clearRecords);
        if (clearRecords)
        {
            _uncompletedFrameStack.Clear();
            _currentFrameIndex = 0;
        }
    }

    public BatchReader GetReader() => _reader;

    private bool TryGetCurrentFrame(out Frame current)
    {
        if (_currentFrameIndex >= _uncompletedFrameStack.Count)
        {
            current = default;
            return false;
        }
        current = _uncompletedFrameStack[_currentFrameIndex];
        return true;
    }

    public bool Read<TTarget>(ref TTarget? value)
        where TTarget : notnull
    {
        var reader = GetReader();

        var completed = true;
        RefId refId;
        ReferenceFlag refFlag;
        TypeRegistration? registration = null;
        var isResuming = TryGetCurrentFrame(out var frame);
        if (isResuming)
        {
            // Resume reading last meta data
            refId = frame.RefId;
            refFlag = frame.RefFlag;
            registration = frame.Registration;
        }
        else
        {
            var refResult = _referenceMetaDeserializer.Read(reader);
            if (!refResult.Completed)
            {
                return false;
            }
            refId = refResult.RefId;
            refFlag = refResult.ReferenceFlag;
        }
        switch (refFlag)
        {
            case ReferenceFlag.Null:
                // Maybe we should throw an exception here for value types
                value = default;
                completed = true;
                break;
            case ReferenceFlag.Ref:
                _referenceMetaDeserializer.GetReadValue(refId, out var readValue);
                value = (TTarget?)readValue.Value;
                completed = true;
                break;
            case ReferenceFlag.RefValue:
                if (!isResuming && !_typeMetaDeserializer.Read(reader, typeof(TTarget), out registration))
                {
                    value = default;
                    completed = false;
                    break;
                }
                Debug.Assert(registration is not null);
                completed = ReadValueCore(reader, registration!, refId, ref value);
                break;
            case ReferenceFlag.NotNullValue:
                if (!isResuming && !_typeMetaDeserializer.Read(reader, typeof(TTarget), out registration))
                {
                    value = default;
                    completed = false;
                    break;
                }
                Debug.Assert(registration is not null);
                completed = ReadValueCore(reader, registration!, null, ref value);
                break;
            default:
                ThrowHelper.ThrowUnreachableException();
                break;
        }

        return completed;
    }

    public bool ReadNullable<TTarget>(TypeRegistration? targetTypeRegistrationHint, out TTarget? value)
        where TTarget : struct
    {
        throw new NotImplementedException();
    }

    public bool ReadNullable<TTarget>(out TTarget? value)
        where TTarget : struct
    {
        throw new NotImplementedException();
    }

    private bool ReadValueCore<TTarget>(
        BatchReader reader,
        IDeserializer deserializer,
        RefId? refId,
        ref TTarget? value
    )
        where TTarget : notnull
    {
        var completed = true;
        var boxedInstance = Box.Empty;

        try
        {
            if (refId is { } id)
            {
                completed = ReadReferenceable<TTarget>(deserializer, id, ref boxedInstance);
                if (boxedInstance.HasValue)
                {
                    value = boxedInstance.AsTyped<TTarget>().Value;
                }
            }
            else
            {
                completed = ReadUnreferenceable(deserializer, ref value);
            }
        }
        catch (Exception)
        {
            completed = false;
            throw;
        }
        finally
        {
            if (!completed)
            {
                Frame frame;
                if (refId is { } id)
                {
                    frame = new Frame(boxedInstance, registration!, id, ReferenceFlag.RefValue, deserializer);
                }
                else
                {
                    frame = new Frame(boxedInstance, registration!, default, ReferenceFlag.NotNullValue, deserializer);
                }
                _uncompletedFrameStack.Add(frame);
            }
        }
        return completed;
    }

    private bool TryResumeRead<TTarget>(out bool completed, out TTarget? value)
        where TTarget : notnull
    {
        if (_currentFrameIndex >= _uncompletedFrameStack.Count)
        {
            Debug.Assert(_currentFrameIndex == _uncompletedFrameStack.Count);
            completed = false;
            value = default;
            return false;
        }
        var currentFrame = _uncompletedFrameStack[_currentFrameIndex];
        var registration = currentFrame.Registration;
        var deserializer = currentFrame.Deserializer;
        switch (currentFrame.RefFlag)
        {
            case ReferenceFlag.NotNullValue:
                completed = ResumeReadNotNullValue(out value);
                break;
            case ReferenceFlag.RefValue:
                var refId = currentFrame.RefId;
                var boxedValue = currentFrame.UncompletedValue;
                completed = ReadReferenceable<TTarget>(deserializer, refId, ref boxedValue);
                Debug.Assert(_currentFrameIndex == _uncompletedFrameStack.Count - 1);
                if (completed)
                {
                    _uncompletedFrameStack.RemoveAt(_currentFrameIndex);
                    registration.ReturnDeserializer(deserializer);
                }
                else
                {
                    _uncompletedFrameStack[_currentFrameIndex] = currentFrame with { UncompletedValue = boxedValue };
                }
                break;
            case ReferenceFlag.Null:
            case ReferenceFlag.Ref:
            default:
                ThrowHelper.ThrowUnreachableException();
                break;
        }
    }

    private bool ResumeReadNotNullValue<TTarget>(out TTarget? value)
        where TTarget : notnull
    {
        var currentFrame = _uncompletedFrameStack[_currentFrameIndex];
        var deserializer = currentFrame.Deserializer;
        var boxedValue = currentFrame.UncompletedValue.AsTyped<TTarget>();
        ref var uncompletedValueRef = ref boxedValue.GetValueRefOrNullRef();
        var completed = false;
        if (Unsafe.IsNullRef(ref uncompletedValueRef))
        {
            // Reference type
            value = boxedValue.Value;
            try
            {
                completed = ReadUnreferenceable(deserializer, ref value);
            }
            finally
            {
                if (!completed)
                {
                    boxedValue.Value = value;
                    _uncompletedFrameStack[_currentFrameIndex] = currentFrame with
                    {
                        UncompletedValue = boxedValue.AsUntyped(),
                    };
                }
            }
        }
        else
        {
            // Value type
            try
            {
                completed = ReadUnreferenceable(deserializer, ref uncompletedValueRef!);
            }
            finally
            {
                value = uncompletedValueRef;
            }
        }
        if (completed)
        {
            _uncompletedFrameStack.RemoveAt(_currentFrameIndex);
        }

        return completed;
    }

    private bool ResumeReadRefValue<TTarget>(out TTarget? value)
    where TTarget : notnull
    {
        var currentFrame = _uncompletedFrameStack[_currentFrameIndex];
        var deserializer = currentFrame.Deserializer;
        var boxedValue = currentFrame.UncompletedValue;
        var refId = currentFrame.RefId;
        var completed = ReadReferenceable<TTarget>(deserializer, refId, ref boxedValue);
        if (completed)
        {
            _uncompletedFrameStack.RemoveAt(_currentFrameIndex);
        }
        else
        {
            _uncompletedFrameStack[_currentFrameIndex] = currentFrame with { UncompletedValue = boxedValue };
        }
        value = boxedValue.AsTyped<TTarget>().Value;
        return completed;
    }

    private bool ReadReferenceable<TTarget>(IDeserializer deserializer, RefId refId, ref Box boxedValue)
        where TTarget : notnull
    {
        // create instance first to handle circular references


        bool completed;
        if (deserializer is IDeserializer<TTarget> typedDeserializer)
        {
            completed = typedDeserializer.CreateInstance(this, ref boxedValue);
            if (completed)
            {
                _referenceMetaDeserializer.AddReadValue(refId, boxedValue);
            }

            completed = completed && typedDeserializer.FillInstance(this, boxedValue);
        }
        else
        {
            completed = deserializer.CreateInstance(this, ref boxedValue);
            if (completed)
            {
                _referenceMetaDeserializer.AddReadValue(refId, boxedValue);
            }

            completed = completed && deserializer.FillInstance(this, boxedValue);
        }
        return completed;
    }

    private bool ReadUnreferenceable<TTarget>(IDeserializer deserializer, ref TTarget? value)
        where TTarget : notnull
    {
        bool completed;
        if (deserializer is IDeserializer<TTarget> typedDeserializer)
        {
            completed = typedDeserializer.CreateAndFillInstance(this, ref value);
        }
        else
        {
            Box boxedInstance = new(value);
            completed = deserializer.CreateInstance(this, ref boxedInstance);
            completed = completed && deserializer.FillInstance(this, boxedInstance);
        }
        return completed;
    }

    public async ValueTask<TTarget?> ReadAsync<TTarget>(CancellationToken cancellationToken = default)
        where TTarget : notnull
    {
        var refResult = await _referenceMetaDeserializer.ReadAsync(_reader, cancellationToken);
        Debug.Assert(refResult.Completed);
        if (refResult.ReferenceFlag is ReferenceFlag.Null)
        {
            return default;
        }
        if (refResult.ReferenceFlag is ReferenceFlag.Ref)
        {
            _referenceMetaDeserializer.GetReadValue(refResult.RefId, out var readValue);
            return (TTarget?)readValue.Value;
        }

        if (refResult.ReferenceFlag is ReferenceFlag.RefValue)
        {
            var reader = GetReader();
            var targetTypeRegistration = await _typeMetaDeserializer.ReadAsync(
                reader,
                typeof(TTarget),
                cancellationToken
            );
            var deserializer = targetTypeRegistration.RentDeserializer();

            // create instance first to handle circular references
            var boxedValue = await deserializer.CreateInstanceAsync(this, cancellationToken);
            _referenceMetaDeserializer.AddReadValue(refResult.RefId, boxedValue);
            await deserializer.FillInstanceAsync(this, boxedValue, cancellationToken);
            return (TTarget?)boxedValue.Value;
        }

        Debug.Assert(refResult.ReferenceFlag is ReferenceFlag.NotNullValue);
        return await DoReadUnreferenceableAsync<TTarget>(cancellationToken);
    }

    public async ValueTask<TTarget?> ReadNullableAsync<TTarget>(CancellationToken cancellationToken = default)
        where TTarget : struct
    {
        var refResult = await _referenceMetaDeserializer.ReadAsync(_reader, cancellationToken);
        Debug.Assert(refResult.Completed);
        if (refResult.ReferenceFlag is ReferenceFlag.Null)
        {
            return null;
        }
        if (refResult.ReferenceFlag is ReferenceFlag.Ref)
        {
            _referenceMetaDeserializer.GetReadValue(refResult.RefId, out var readValue);
            return (TTarget?)readValue.Value;
        }

        if (refResult.ReferenceFlag is ReferenceFlag.RefValue)
        {
            var boxedValue = await DoReadReferenceableAsync(cancellationToken);
            _referenceMetaDeserializer.AddReadValue(refResult.RefId, boxedValue);
            return (TTarget?)boxedValue.Value;
        }

        Debug.Assert(refResult.ReferenceFlag is ReferenceFlag.NotNullValue);
        return await DoReadUnreferenceableAsync<TTarget>(cancellationToken);
    }

    private async ValueTask<TTarget> DoReadUnreferenceableAsync<TTarget>(CancellationToken cancellationToken = default)
        where TTarget : notnull
    {
        var declaredType = typeof(TTarget);
        var targetTypeRegistration = await ReadTypeMetaAsync(cancellationToken);
        var deserializer = Fury.TypeRegistry.GetDeserializer(targetTypeRegistration);
        if (
            targetTypeRegistration.TargetType == declaredType
            && deserializer is IDeserializer<TTarget> typedDeserializer
        )
        {
            // fast path
            return await typedDeserializer.CreateAndFillInstanceAsync(this, cancellationToken);
        }
        // slow path
        var newObj = await deserializer.CreateInstanceAsync(this, cancellationToken);
        await deserializer.FillInstanceAsync(this, newObj, cancellationToken);
        return (TTarget)newObj.Value!;
    }

    private async ValueTask<Box> DoReadReferenceableAsync(
        BatchReader reader,
        Type declaredType,
        CancellationToken cancellationToken = default
    )
    {
        var targetTypeRegistration = await _typeMetaDeserializer.ReadAsync(reader, declaredType, cancellationToken);
        var deserializer = targetTypeRegistration.RentDeserializer();

        // create instance first to handle circular references
        var newObj = await deserializer.CreateInstanceAsync(this, cancellationToken);
        _refContext.PushReferenceableObject(newObj);
        await deserializer.FillInstanceAsync(this, newObj, cancellationToken);
        return newObj;
    }

    private record struct Frame(
        int depth,
        Box UncompletedValue,
        TypeRegistration Registration,
        RefId RefId,
        ReferenceFlag RefFlag,
        IDeserializer Deserializer
    );
}
