using System;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Fury;

internal static class NullableHelper
{
    public static bool IsNullable(Type type)
    {
        return Nullable.GetUnderlyingType(type) is not null;
    }

#if NET6_0_OR_GREATER
    internal static MethodInfo GetValueOffsetMethodInfo { get; } =
        typeof(NullableHelper).GetMethod(nameof(GetValueOffset), BindingFlags.Static | BindingFlags.NonPublic)!;

    public static ref byte GetValueRefOrNullRef<T>(ref T value)
    {
        if (NullableHelper<T>.ValueOffset is not { } offset)
        {
            return ref Unsafe.NullRef<byte>();
        }

        ref var valueRef = ref Unsafe.AddByteOffset(ref value, offset);
        return ref Unsafe.As<T, byte>(ref valueRef);
    }

    internal static nint GetValueOffset<T>()
        where T : struct
    {
        T? nullable = null;
        ref readonly var valueRef = ref GetValueRefOrDefaultRef(ref nullable);
        var offset = Unsafe.ByteOffset(
            ref Unsafe.As<T?, byte>(ref nullable),
            ref Unsafe.As<T, byte>(ref Unsafe.AsRef(in valueRef))
        );
        return offset;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ref readonly T GetValueRefOrDefaultRef<T>(ref readonly T? value)
        where T : struct
    {
#if NET7_0_OR_GREATER
        return ref Nullable.GetValueRefOrDefaultRef(in value);
#elif NET6_0_OR_GREATER
        return ref value.AsReadOnlyEquivalent().Value;
#endif
    }

#endif
}

#if NET6_0_OR_GREATER
internal static class NullableHelper<T>
{
    // ReSharper disable once StaticMemberInGenericType
    public static readonly nint? ValueOffset;

    static NullableHelper()
    {
        if (Nullable.GetUnderlyingType(typeof(T)) is not null)
        {
            ValueOffset = (nint)
                NullableHelper.GetValueOffsetMethodInfo.MakeGenericMethod(typeof(T)).Invoke(null, null)!;
        }
    }
}

internal static class NullableExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ref NullableEquivalent<T> AsEquivalent<T>(ref this T? value)
        where T : struct
    {
        return ref Unsafe.As<T?, NullableEquivalent<T>>(ref value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ref readonly NullableEquivalent<T> AsReadOnlyEquivalent<T>(ref readonly this T? value)
        where T : struct
    {
        return ref Unsafe.As<T?, NullableEquivalent<T>>(ref Unsafe.AsRef(in value));
    }
}

/// <summary>
/// Equivalent of <see cref="Nullable"/> whose fields can be accessed directly.
/// </summary>
/// <typeparam name="T"></typeparam>
internal struct NullableEquivalent<T>
    where T : struct
{
#pragma warning disable CS0649 // Unassigned fields
    // ReSharper disable once NotAccessedField.Local
    public bool HasValue;
    public T Value;
#pragma warning restore CS0649
}

#endif
