using System;
using System.Diagnostics.CodeAnalysis;

namespace Fury;

internal static partial class ThrowHelper
{
    [DoesNotReturn]
    public static void ThrowArgumentOutOfRangeException(
        string paramName,
        object? actualValue = null,
        string? message = null
    )
    {
        throw new ArgumentOutOfRangeException(paramName, actualValue, message);
    }

    [DoesNotReturn]
    public static void ThrowArgumentOutOfRangeException_AttemptedToAdvanceFurtherThanBufferLength(
        string paramName,
        int bufferLength,
        int advanceLength
    )
    {
        throw new ArgumentOutOfRangeException(
            paramName,
            $"Attempted to advance further than the buffer length. Buffer length: {bufferLength}, Advance length: {advanceLength}"
        );
    }
}
