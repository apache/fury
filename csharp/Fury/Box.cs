using System.Runtime.CompilerServices;

namespace Fury;

public struct Box(object value)
{
    public object? Value { get; set; } = value;

    public Box<T> AsTyped<T>()
        where T : notnull
    {
        return new Box<T> { InternalValue = Value };
    }
}

public struct Box<T>(in T value)
    where T : notnull
{
    internal object? InternalValue = value;

    public T? Value
    {
        get => (T?)InternalValue;
        set => InternalValue = value;
    }

    public Box AsUntyped()
    {
        return new Box { Value = InternalValue };
    }

    public static implicit operator Box<T>(in T boxed)
    {
        return new Box<T>(in boxed);
    }
}

public static class BoxExtensions
{
    public static ref T Unbox<T>(this Box<T> box)
        where T : struct
    {
        box.InternalValue ??= new T();
        return ref Unsafe.Unbox<T>(box.InternalValue);
    }
}
