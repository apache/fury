using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Fury.Buffers;

// Copy and modify from Microsoft.Extensions.ObjectPool.DefaultObjectPool<T>

/// <typeparam name="T">
/// The type to pool objects for.
/// </typeparam>
/// <remarks>
/// This implementation keeps a cache of retained objects.
/// This means that if objects are returned when the pool has already reached
/// "maximumRetained" objects they will be available to be Garbage Collected.
/// </remarks>
internal class ObjectPool<T>
    where T : class
{
    private readonly Func<ObjectPool<T>, T> _factory;
    private readonly int _maxCapacity;
    private int _numItems;

    private readonly ConcurrentQueue<T> _items = new();
    private T? _fastItem;

    /// <summary>
    /// Creates an instance of <see cref="ObjectPool{T}"/>.
    /// </summary>
    public ObjectPool(Func<ObjectPool<T>, T> factory)
        : this(factory, Environment.ProcessorCount * 2) { }

    /// <summary>
    /// Creates an instance of <see cref="ObjectPool{T}"/>.
    /// </summary>
    /// <param name="factory">
    /// The factory to use to create new objects when needed.
    /// </param>
    /// <param name="maximumRetained">
    /// The maximum number of objects to retain in the pool.
    /// </param>
    public ObjectPool(Func<ObjectPool<T>, T> factory, int maximumRetained)
    {
        // cache the target interface methods, to avoid interface lookup overhead
        _factory = factory;
        _maxCapacity = maximumRetained - 1; // -1 to account for _fastItem
    }

    public T Rent()
    {
        var item = _fastItem;
        if (item == null || Interlocked.CompareExchange(ref _fastItem, null, item) != item)
        {
            if (_items.TryDequeue(out item))
            {
                Interlocked.Decrement(ref _numItems);
                return item;
            }

            // no object available, so go get a brand new one
            return _factory(this);
        }

        return item;
    }

    public void Return(T obj)
    {
        if (_fastItem != null || Interlocked.CompareExchange(ref _fastItem, obj, null) != null)
        {
            if (Interlocked.Increment(ref _numItems) <= _maxCapacity)
            {
                _items.Enqueue(obj);
            }

            // no room, clean up the count and drop the object on the floor
            Interlocked.Decrement(ref _numItems);
        }
    }
}
