using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Fury;

public readonly struct Box(object? value)
{
    internal static readonly MethodInfo UnboxMethod = typeof(Unsafe).GetMethod(nameof(Unsafe.Unbox))!;
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
    private delegate ref T UnboxDelegate(object value);

    private static UnboxDelegate? _unbox;

    public static readonly Box<T> Empty = new(default);

    internal object? InternalValue = value;
    public bool HasValue => InternalValue is not null;

    static Box()
    {
        if (typeof(T).IsValueType)
        {
            _unbox = (UnboxDelegate)Box.UnboxMethod.MakeGenericMethod(typeof(T)).CreateDelegate(typeof(UnboxDelegate));
        }
    }

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

    public ref T GetValueRefOrNullRef()
    {
        if (typeof(T).IsValueType)
        {
            InternalValue ??= default(T);
            return ref _unbox!(InternalValue!);
        }

        return ref Unsafe.NullRef<T>();
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
