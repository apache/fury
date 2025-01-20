using System.Runtime.CompilerServices;

namespace Fury;

public readonly struct Box(object? value)
{
    public static readonly Box Empty = new(null);

    public object? Value { get; init; } = value;
    public bool HasValue => Value is not null;

    public Box<T> AsTyped<T>()
        where T : notnull
    {
        return new Box<T> { InternalValue = Value };
    }
}

public struct Box<T>(in T? value)
    where T : notnull
{
    public static readonly Box<T> Empty = new(default);

    internal object? InternalValue = value;
    public bool HasValue => InternalValue is not null;

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
    // Users may not know Unsafe.Unbox<T>(ref T) or be afraid of "Unsafe" in the name.

    /// <inheritdoc cref="Unsafe.Unbox{T}"/>
    public static ref T Unbox<T>(this Box<T> box)
        where T : struct
    {
        box.InternalValue ??= new T();
        return ref Unsafe.Unbox<T>(box.InternalValue);
    }
}
