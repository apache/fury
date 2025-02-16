using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Fury.Collections;
using Fury.Meta;

namespace Fury.Context;

internal readonly struct RefContext()
{
    private readonly Dictionary<object, RefId> _objectsToRefId = new();
    private readonly SpannableList<RefRecord> _readObjectRecords = [];

    public bool Contains(object value) => _objectsToRefId.ContainsKey(value);

    public bool Contains(RefId refId) => refId.Value >= 0 && refId.Value < _readObjectRecords.Count;

    public RefId GetRefIdAndSetCompletionState(object value, bool completed, out bool exists)
    {
#if NET8_0_OR_GREATER
        ref var refId = ref CollectionsMarshal.GetValueRefOrAddDefault(_objectsToRefId, value, out exists);
#else
        exists = _objectsToRefId.TryGetValue(value, out var refId);
#endif
        if (exists)
        {
            _readObjectRecords[refId.Value] = new(value, completed);
        }
        else
        {
            refId = new RefId(_readObjectRecords.Count);
            _readObjectRecords.Add(new(value, completed));
#if !NET8_0_OR_GREATER
            _objectsToRefId[value] = refId;
#endif
        }
        return refId;
    }

    public bool TryGetValue(RefId refId, [NotNullWhen(true)] out object? value, out bool completed)
    {
        if (!Contains(refId))
        {
            value = null;
            completed = false;
            return false;
        }

        (value, completed) = _readObjectRecords[refId.Value];
        return true;
    }

    public bool TryGetRefRecord(object value, out RefRecord refRecord)
    {
        if (!_objectsToRefId.TryGetValue(value, out var refId))
        {
            refRecord = default;
            return false;
        }

        refRecord = _readObjectRecords[refId.Value];
        return true;
    }

    public ref RefRecord GetRefRecordRef(RefId refId, bool exists)
    {
        if (!Contains(refId))
        {
            return ref Unsafe.NullRef<RefRecord>();
        }

        var records = _readObjectRecords.AsSpan();
        return ref records[refId.Value];
    }

    public ref RefRecord GetRefRecordRefOrAddDefault(object value, out bool exists)
    {
#if NET8_0_OR_GREATER
        var refId = CollectionsMarshal.GetValueRefOrAddDefault(_objectsToRefId, value, out exists);
#else
        exists = _objectsToRefId.TryGetValue(value, out var refId);
#endif
        if (!exists)
        {
            refId = new RefId(_readObjectRecords.Count);
            _readObjectRecords.Add(new RefRecord(value, false));
#if !NET8_0_OR_GREATER
            _objectsToRefId[value] = refId;
#endif
        }

        return ref _readObjectRecords.AsSpan()[refId.Value];
    }

    public void Reset()
    {
        _objectsToRefId.Clear();
        _readObjectRecords.Clear();
    }

    public record struct RefRecord(object Object, bool Completed);
}
