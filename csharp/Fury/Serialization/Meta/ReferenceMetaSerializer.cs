using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Fury.Collections;
using Fury.Context;
using Fury.Meta;

namespace Fury.Serialization.Meta;

internal struct ReferenceMetaSerializer(bool referenceTracking)
{
    private readonly HashSet<object> _objectsBeingSerialized = [];
    private readonly AutoIncrementIdDictionary<object> _writtenRefIds = new();
    private bool _hasWrittenRefId;
    private RefId? _lastUncompletedRefId;
    private bool _hasWrittenRefFlag;
    private ReferenceFlag _lastUncompletedReferenceFlag;

    public void Reset(bool clearContext)
    {
        _hasWrittenRefId = false;
        _lastUncompletedRefId = null;
        _hasWrittenRefFlag = false;
        if (clearContext)
        {
            _objectsBeingSerialized.Clear();
            _writtenRefIds.Clear();
        }
    }

    public bool Write<TTarget>(ref BatchWriter writer, in TTarget? value, out bool needWriteValue)
        where TTarget : notnull
    {
        var completed = true;
        if (value is null)
        {
            needWriteValue = false;
            completed = TryWriteReferenceFlag(ref writer, ReferenceFlag.Null, ref _hasWrittenRefFlag);
        }
        else if (TypeHelper<TTarget>.IsValueType)
        {
            // Objects declared as ValueType are not possible to be referenced

            needWriteValue = true;
            completed =
                completed && TryWriteReferenceFlag(ref writer, ReferenceFlag.NotNullValue, ref _hasWrittenRefFlag);
        }
        else if (referenceTracking)
        {
            var refId = _lastUncompletedRefId;
            var refFlag = _lastUncompletedReferenceFlag;
            if (refId is null)
            {
                // Last write was completed
                var id = _writtenRefIds.AddOrGet(value, out var exists);
                refId = new RefId(id);
                refFlag = exists ? ReferenceFlag.Ref : ReferenceFlag.RefValue;
            }
            completed = completed && TryWriteReferenceFlag(ref writer, refFlag, ref _hasWrittenRefFlag);
            if (refFlag is ReferenceFlag.Ref)
            {
                // A referenceable object has been recorded
                needWriteValue = false;
                completed = completed && writer.TryWrite7BitEncodedUint((uint)refId.Value.Value, ref _hasWrittenRefId);
            }
            else
            {
                // A new referenceable object
                needWriteValue = true;
                Debug.Assert(refFlag is ReferenceFlag.RefValue);
            }

            if (completed)
            {
                _lastUncompletedRefId = null;
            }
            else
            {
                _lastUncompletedRefId = refId;
                _lastUncompletedReferenceFlag = refFlag;
            }
        }
        else
        {
            if (!_objectsBeingSerialized.Add(value))
            {
                ThrowHelper.ThrowBadSerializationInputException_CircularDependencyDetected();
            }

            completed =
                completed && TryWriteReferenceFlag(ref writer, ReferenceFlag.NotNullValue, ref _hasWrittenRefFlag);
            needWriteValue = true;

            _objectsBeingSerialized.Remove(value);
        }

        return completed;
    }

    public bool Write<TTarget>(ref BatchWriter writer, TTarget? value, out bool needWriteValue)
        where TTarget : struct
    {
        var completed = true;
        if (value is null)
        {
            needWriteValue = false;
            completed = TryWriteReferenceFlag(ref writer, ReferenceFlag.Null, ref _hasWrittenRefFlag);
        }
        else
        {
            // Objects declared as ValueType are not possible to be referenced

            needWriteValue = true;
            completed =
                completed && TryWriteReferenceFlag(ref writer, ReferenceFlag.NotNullValue, ref _hasWrittenRefFlag);
        }

        return completed;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryWriteReferenceFlag(ref BatchWriter writer, ReferenceFlag flag, ref bool hasWritten)
    {
        if (!hasWritten)
        {
            hasWritten = writer.TryWrite((sbyte)flag);
        }

        return hasWritten;
    }
}

internal struct ReferenceMetaDeserializer()
{
    private readonly AutoIncrementIdDictionary<Box> _readValues = new();
    private ReferenceFlag? _lastUncompletedReferenceFlag;
    private RefId? _currentRefId;

    public void Reset(bool clearContext)
    {
        _lastUncompletedReferenceFlag = null;
        _currentRefId = null;
        if (clearContext)
        {
            _readValues.Clear();
        }
    }

    public ReadResult Read(BatchReader reader)
    {
        bool completed;
        ReferenceFlag referenceFlag;
        if (_lastUncompletedReferenceFlag is null)
        {
            completed = reader.TryReadReferenceFlag(out referenceFlag);
            _lastUncompletedReferenceFlag = completed ? referenceFlag : null;
            if (completed)
            {
                _lastUncompletedReferenceFlag = referenceFlag;
            }
            else
            {
                return new ReadResult(Completed: false);
            }
        }
        else
        {
            referenceFlag = _lastUncompletedReferenceFlag.Value;
        }

        ReadResult result;
        switch (referenceFlag)
        {
            case ReferenceFlag.Null:
            case ReferenceFlag.NotNullValue:
            case ReferenceFlag.RefValue:
                result = new ReadResult(ReferenceFlag: _lastUncompletedReferenceFlag.Value);
                break;
            case ReferenceFlag.Ref:
                completed = reader.TryReadRefId(out var id);
                _currentRefId = completed ? id : null;
                result = new ReadResult(
                    Completed: completed,
                    RefId: id,
                    ReferenceFlag: _lastUncompletedReferenceFlag.Value
                );
                break;
            default:
                result = ThrowHelper.ThrowUnreachableException<ReadResult>();
                break;
        }

        return result;
    }

    public async ValueTask<ReadResult> ReadAsync(BatchReader reader, CancellationToken cancellationToken = default)
    {
        _lastUncompletedReferenceFlag ??= await reader.ReadReferenceFlagAsync(cancellationToken);
        ReadResult result;
        switch (_lastUncompletedReferenceFlag)
        {
            case ReferenceFlag.Null:
            case ReferenceFlag.NotNullValue:
            case ReferenceFlag.RefValue:
                result = new ReadResult(ReferenceFlag: _lastUncompletedReferenceFlag.Value);
                break;
            case ReferenceFlag.Ref:
                _currentRefId ??= await reader.ReadRefIdAsync(cancellationToken);
                result = new ReadResult(RefId: _currentRefId.Value, ReferenceFlag: _lastUncompletedReferenceFlag.Value);
                break;
            default:
                result = ThrowHelper.ThrowUnreachableException<ReadResult>();
                break;
        }

        return result;
    }

    public void GetReadValue(RefId refId, out Box value)
    {
        if (!_readValues.TryGetValue(refId.Value, out value))
        {
            ThrowHelper.ThrowBadDeserializationInputException_ReferencedObjectNotFound(refId);
        }
    }

    public void AddReadValue(RefId refId, Box value)
    {
        _readValues[refId.Value] = value;
    }

    public record struct ReadResult(
        bool Completed = true,
        RefId RefId = default,
        ReferenceFlag ReferenceFlag = default
    );
}
