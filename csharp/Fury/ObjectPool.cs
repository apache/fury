using System.Collections.Generic;

namespace Fury;

/// <summary>
/// A simple object pool.
/// </summary>
/// <typeparam name="T"></typeparam>
internal readonly struct ObjectPool<T>()
    where T : class, new()
{
    private readonly List<T> _objects = [];

    public T Rent()
    {
        var lastIndex = _objects.Count - 1;
        if (lastIndex < 0)
        {
            return new T();
        }

        var obj = _objects[lastIndex];
        _objects.RemoveAt(lastIndex);
        return obj;
    }

    public void Return(T obj)
    {
        _objects.Add(obj);
    }
}
