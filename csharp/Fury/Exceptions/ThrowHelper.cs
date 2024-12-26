using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Fury;

internal static partial class ThrowHelper
{
    [DoesNotReturn]
    public static void ThrowNotSupportedException(string? message = null)
    {
        throw new NotSupportedException(message);
    }

    [DoesNotReturn]
    public static TReturn ThrowNotSupportedException<TReturn>(string? message = null)
    {
        throw new NotSupportedException(message);
    }

    [DoesNotReturn]
    public static void ThrowInvalidOperationException(string? message = null)
    {
        throw new InvalidOperationException(message);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowFormatExceptionIf([DoesNotReturnIf(true)] bool condition, string? message = null)
    {
        if (condition)
        {
            ThrowFormatException(message);
        }
    }

    [DoesNotReturn]
    public static void ThrowFormatException(string? message = null)
    {
        throw new FormatException(message);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowArgumentNullExceptionIf(
        [DoesNotReturnIf(true)] bool condition,
        string paramName,
        string? message = null
    )
    {
        if (condition)
        {
            ThrowArgumentNullException(paramName, message);
        }
    }

    [DoesNotReturn]
    public static void ThrowArgumentNullException(string paramName, string? message = null)
    {
        throw new ArgumentNullException(paramName, message);
    }

    [DoesNotReturn]
    public static void ThrowArgumentException(string paramName, string? message = null)
    {
        throw new ArgumentException(message, paramName);
    }

    [DoesNotReturn]
    public static void ThrowArgumentOutOfRangeException(
        string paramName,
        object? actualValue = null,
        string? message = null
    )
    {
        throw new ArgumentOutOfRangeException(paramName, actualValue, message);
    }
}
