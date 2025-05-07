using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using JetBrains.Annotations;

namespace Fury.Collections;

internal sealed class AutoIncrementIdDictionary<TValue>
    : ICollection<TValue>,
        IReadOnlyDictionary<TValue, int>,
        IReadOnlyDictionary<int, TValue>
    where TValue : notnull
{
    private readonly Dictionary<TValue, int> _valueToId = new();
    private readonly SpannableList<TValue> _idToValueDense = [];
    private readonly Dictionary<int, TValue> _idToValueSparse = new();
    private readonly PriorityQueue<int, TValue> _sparseIds = new();

    public int this[TValue key]
    {
        get => _valueToId[key];
        set => throw new NotImplementedException();
    }

    public TValue this[int id]
    {
        get
        {
            ThrowHelper.ThrowArgumentOutOfRangeExceptionIfNegative(id, nameof(id));
            if (id < _idToValueDense.Count)
            {
                return _idToValueDense[id];
            }
            if (_idToValueSparse.TryGetValue(id, out var value))
            {
                return value;
            }

            ThrowHelper.ThrowArgumentOutOfRangeException(nameof(id), id);
            return default;
        }
        set
        {
            ThrowHelper.ThrowArgumentOutOfRangeExceptionIfNegative(id, nameof(id));
            var result = Add(id, value);
            if (!result.Exists)
            {
                return;
            }

            if (value.Equals(result.Value))
            {
                if (id != result.Id)
                {
                    ThrowArgumentOutOfRangeException_ValueExists(id, in value, nameof(id));
                }
                return;
            }

            if (id < _idToValueDense.Count)
            {
                _idToValueDense[id] = value;
            }
            else
            {
                _idToValueSparse[id] = value;
            }

            _valueToId[value] = id;
        }
    }

    IEnumerable<TValue> IReadOnlyDictionary<TValue, int>.Keys => _valueToId.Keys;

    IEnumerable<int> IReadOnlyDictionary<int, TValue>.Keys => Enumerable.Range(0, _idToValueDense.Count);

    public ICollection<int> Values => _valueToId.Values;

    IEnumerable<int> IReadOnlyDictionary<TValue, int>.Values => _valueToId.Values;

    IEnumerable<TValue> IReadOnlyDictionary<int, TValue>.Values => _idToValueDense;

    public int Count => _valueToId.Count;

    public bool IsReadOnly => false;

    public int AddOrGet(in TValue value, out bool exists)
    {
        var id = _idToValueDense.Count;
        var result = Add(id, value);
        exists = result.Exists;
        return id;
    }

    public AddResult Add(int id, in TValue value)
    {
        ThrowHelper.ThrowArgumentOutOfRangeExceptionIfNegative(id, nameof(id));

        var storedId = _valueToId.GetOrAdd(value, id, out var exists);
        if (exists)
        {
            return new AddResult(true, storedId, value);
        }

        exists = id < _idToValueDense.Count;
        if (exists)
        {
            var existingValue = _idToValueDense[id];
            return new AddResult(true, id, existingValue);
        }

        if (id == _idToValueDense.Count)
        {
            _idToValueDense.Add(value);
            MoveSparseToDense();
            return new AddResult(false, id, value);
        }

        var storedValue = _idToValueSparse.GetOrAdd(id, value, out exists);
        if (exists)
        {
            return new AddResult(true, id, storedValue!);
        }

        _sparseIds.Enqueue(id, value);
        return new AddResult(false, id, value);
    }

    private void MoveSparseToDense()
    {
        while (_sparseIds.TryPeek(out var id, out var value))
        {
            if (id != _idToValueDense.Count)
            {
                break;
            }

            _idToValueDense.Add(value);
            _sparseIds.Dequeue();
        }
    }

    void ICollection<TValue>.Add(TValue item)
    {
        AddOrGet(item, out _);
    }

    public bool Remove(TValue item)
    {
        ThrowHelper.ThrowNotSupportedException();
        return false;
    }

    public void Clear()
    {
        _valueToId.Clear();
        _idToValueDense.Clear();
    }

    bool ICollection<TValue>.Contains(TValue item) => ContainsKey(item);

    public bool ContainsKey(TValue key)
    {
        return _valueToId.ContainsKey(key);
    }

    public bool ContainsKey(int key)
    {
        return key >= 0 && key < _idToValueDense.Count;
    }

    public void CopyTo(TValue[] array, int arrayIndex)
    {
        _idToValueDense.CopyTo(array, arrayIndex);
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
            value = _idToValueDense[key];
            return true;
        }

        value = default!;
        return false;
    }

    public ref TValue GetValueRefOrNullRef(int key)
    {
        if (ContainsKey(key))
        {
            var values = _idToValueDense.AsSpan();
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

    [DoesNotReturn]
    private void ThrowArgumentOutOfRangeException_ValueExists(int id, in TValue value, [InvokerParameterName] string paramName)
    {
            ThrowHelper.ThrowArgumentOutOfRangeException(paramName, id, $"{value} exists at {id}");
    }

    public ref struct Enumerator(AutoIncrementIdDictionary<TValue> idDictionary)
    {
        private int _index = -1;
        private readonly Span<TValue> _entries = idDictionary._idToValueDense.AsSpan();

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

    public record struct AddResult(bool Exists, int Id, TValue Value);
}
