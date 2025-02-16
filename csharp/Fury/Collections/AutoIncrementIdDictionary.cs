using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fury.Collections;

internal sealed class AutoIncrementIdDictionary<TKeyValue>
    : ICollection<TKeyValue>,
        IReadOnlyDictionary<TKeyValue, int>,
        IReadOnlyDictionary<int, TKeyValue>
    where TKeyValue : notnull
{
    private readonly Dictionary<TKeyValue, int> _valueToId = new();
    private readonly SpannableList<TKeyValue> _idToValue = [];

    public int this[TKeyValue key]
    {
        get => _valueToId[key];
        set
        {
            ThrowHelper.ThrowNotSupportedException();
            _ = value;
        }
    }

    public TKeyValue this[int key]
    {
        get => _idToValue[key];
        set
        {
            _idToValue[key] = value;
            _valueToId[value] = key;
        }
    }

    IEnumerable<TKeyValue> IReadOnlyDictionary<TKeyValue, int>.Keys => _valueToId.Keys;

    IEnumerable<int> IReadOnlyDictionary<int, TKeyValue>.Keys => Enumerable.Range(0, _idToValue.Count);

    public ICollection<int> Values => _valueToId.Values;

    IEnumerable<int> IReadOnlyDictionary<TKeyValue, int>.Values => _valueToId.Values;

    IEnumerable<TKeyValue> IReadOnlyDictionary<int, TKeyValue>.Values => _idToValue;

    public int Count => _valueToId.Count;

    public bool IsReadOnly => false;

    public int AddOrGet(TKeyValue value, out bool exists)
    {
#if NET8_0_OR_GREATER
        ref var kind = ref CollectionsMarshal.GetValueRefOrAddDefault(_valueToId, value, out exists);
#else
        exists = _valueToId.TryGetValue(value, out var kind);
#endif
        if (!exists)
        {
            id = _idToValue.Count;
            _idToValue.Add(value);
#if !NET8_0_OR_GREATER
            _valueToId.Add(value, kind);
#endif
        }
        return id;
    }

    void ICollection<TKeyValue>.Add(TKeyValue item)
    {
        AddOrGet(item, out _);
    }

    public bool Remove(TKeyValue item)
    {
        ThrowHelper.ThrowNotSupportedException();
        return false;
    }

    public void Clear()
    {
        _valueToId.Clear();
        _idToValue.Clear();
    }

    bool ICollection<TKeyValue>.Contains(TKeyValue item) => ContainsKey(item);

    public bool ContainsKey(TKeyValue key)
    {
        return _valueToId.ContainsKey(key);
    }

    public bool ContainsKey(int key)
    {
        return key >= 0 && key < _idToValue.Count;
    }

    public void CopyTo(TKeyValue[] array, int arrayIndex)
    {
        _idToValue.CopyTo(array, arrayIndex);
    }

    IEnumerator<KeyValuePair<TKeyValue, int>> IEnumerable<KeyValuePair<TKeyValue, int>>.GetEnumerator()
    {
        return _valueToId.GetEnumerator();
    }

    public bool TryGetValue(TKeyValue key, out int value)
    {
        return _valueToId.TryGetValue(key, out value);
    }

    public bool TryGetValue(int key, out TKeyValue value)
    {
        if (ContainsKey(key))
        {
            value = _idToValue[key];
            return true;
        }

        value = default!;
        return false;
    }

    public ref TKeyValue GetValueRefOrNullRef(int key)
    {
        if (ContainsKey(key))
        {
            var values = _idToValue.AsSpan();
            return ref values[key];
        }

        return ref Unsafe.NullRef<TKeyValue>();
    }

    public Enumerator GetEnumerator()
    {
        return new Enumerator(this);
    }

    IEnumerator<TKeyValue> IEnumerable<TKeyValue>.GetEnumerator()
    {
        ThrowHelper.ThrowNotSupportedException();
        return null!;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        ThrowHelper.ThrowNotSupportedException();
        return null!;
    }

    IEnumerator<KeyValuePair<int, TKeyValue>> IEnumerable<KeyValuePair<int, TKeyValue>>.GetEnumerator()
    {
        ThrowHelper.ThrowNotSupportedException();
        return null!;
    }

    public ref struct Enumerator(AutoIncrementIdDictionary<TKeyValue> idDictionary)
    {
        private int _index = -1;
        private readonly Span<TKeyValue> _entries = idDictionary._idToValue.AsSpan();

        public bool MoveNext()
        {
            return ++_index < _entries.Length;
        }

        public void Reset()
        {
            _index = -1;
        }

        public KeyValuePair<int, TKeyValue> Current => new(_index, _entries[_index]);
    }
}
