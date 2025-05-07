using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Fury.Collections;

internal sealed class AutoIncrementIdDictionary<TValue>
    : ICollection<TValue>,
        IReadOnlyDictionary<TValue, int>,
        IReadOnlyDictionary<int, TValue>
    where TValue : notnull
{
    private readonly Dictionary<TValue, int> _valueToId = new();
    private readonly SpannableList<TValue> _idToValue = [];

    public int this[TValue key] => _valueToId[key];

    public TValue this[int id]
    {
        get
        {
            if (id < 0 || id >= _idToValue.Count)
            {
                ThrowHelper.ThrowArgumentOutOfRangeException(nameof(id));
            }
            return _idToValue[id];
        }
        set
        {
            if (id < 0 || id >= _idToValue.Count)
            {
                ThrowHelper.ThrowArgumentOutOfRangeException(nameof(id));
            }

            if (_valueToId.ContainsKey(value))
            {
                ThrowArgumentException_ValueAlreadyExists(value);
            }
            _idToValue[id] = value;
        }
    }

    [DoesNotReturn]
    private void ThrowArgumentException_ValueAlreadyExists(TValue value)
    {
        ThrowHelper.ThrowArgumentException(
            $"The value '{value}' already exists in the {nameof(AutoIncrementIdDictionary<TValue>)}.",
            nameof(value)
        );
    }

    IEnumerable<TValue> IReadOnlyDictionary<TValue, int>.Keys => _valueToId.Keys;

    IEnumerable<int> IReadOnlyDictionary<int, TValue>.Keys => Enumerable.Range(0, _idToValue.Count);

    public ICollection<int> Values => _valueToId.Values;

    IEnumerable<int> IReadOnlyDictionary<TValue, int>.Values => _valueToId.Values;

    IEnumerable<TValue> IReadOnlyDictionary<int, TValue>.Values => _idToValue;

    public int Count => _valueToId.Count;

    public bool IsReadOnly => false;

    public int GetOrAdd(in TValue value, out bool exists)
    {
        var nextId = _idToValue.Count;
        var id = _valueToId.GetOrAdd(value, nextId, out exists);
        if (!exists)
        {
            _idToValue.Add(value);
        }

        return id;
    }

    void ICollection<TValue>.Add(TValue item)
    {
        GetOrAdd(item, out _);
    }

    public bool Remove(TValue item)
    {
        ThrowHelper.ThrowNotSupportedException();
        return false;
    }

    public void Clear()
    {
        _valueToId.Clear();
        _idToValue.Clear();
    }

    bool ICollection<TValue>.Contains(TValue item) => ContainsKey(item);

    public bool ContainsKey(TValue key)
    {
        return _valueToId.ContainsKey(key);
    }

    public bool ContainsKey(int key)
    {
        return key >= 0 && key < _idToValue.Count;
    }

    public void CopyTo(TValue[] array, int arrayIndex)
    {
        _idToValue.CopyTo(array, arrayIndex);
    }

    IEnumerator<KeyValuePair<TValue, int>> IEnumerable<KeyValuePair<TValue, int>>.GetEnumerator()
    {
        return _valueToId.GetEnumerator();
    }

    public bool TryGetValue(TValue key, out int value)
    {
        return _valueToId.TryGetValue(key, out value);
    }

    public bool TryGetValue(int key, out TValue value)
    {
        if (ContainsKey(key))
        {
            value = _idToValue[key];
            return true;
        }

        value = default!;
        return false;
    }

    public ref TValue GetValueRefOrNullRef(int key)
    {
        if (ContainsKey(key))
        {
            var values = _idToValue.AsSpan();
            return ref values[key];
        }

        return ref Unsafe.NullRef<TValue>();
    }

    public Enumerator GetEnumerator()
    {
        return new Enumerator(this);
    }

    IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator()
    {
        ThrowHelper.ThrowNotSupportedException();
        return null!;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        ThrowHelper.ThrowNotSupportedException();
        return null!;
    }

    IEnumerator<KeyValuePair<int, TValue>> IEnumerable<KeyValuePair<int, TValue>>.GetEnumerator()
    {
        ThrowHelper.ThrowNotSupportedException();
        return null!;
    }

    public ref struct Enumerator(AutoIncrementIdDictionary<TValue> idDictionary)
    {
        private int _index = -1;
        private readonly Span<TValue> _entries = idDictionary._idToValue.AsSpan();

        public bool MoveNext()
        {
            return ++_index < _entries.Length;
        }

        public void Reset()
        {
            _index = -1;
        }

        public KeyValuePair<int, TValue> Current => new(_index, _entries[_index]);
    }
}
