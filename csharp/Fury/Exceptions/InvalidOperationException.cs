using System;
using System.Diagnostics.CodeAnalysis;

namespace Fury;

internal static partial class ThrowHelper
{
    [DoesNotReturn]
    public static void ThrowInvalidOperationException(string? message = null)
    {
        throw new InvalidOperationException(message);
    }

    [DoesNotReturn]
    public static void ThrowInvalidOperationException_AttemptedToWriteToReadOnlyCollection()
    {
        throw new InvalidOperationException("Attempted to write to a read-only collection.");
    }

    [DoesNotReturn]
    public static void ThrowInvalidOperationException_AttemptedToWriteToReadOnlyDeserializationProgress()
    {
        throw new InvalidOperationException("Attempted to write to a read-only deserialization progress.");
    }

    [DoesNotReturn]
    public static void ThrowInvalidOperationException_PooledBufferWriterAdvancedTooFar(int capacity)
    {
        throw new InvalidOperationException(
            $"Cannot advance past the end of the buffer with a capacity of {capacity}."
        );
    }

    [DoesNotReturn]
    public static void ThrowInvalidOperationException_TypeNameCollision(
        Type newType,
        Type existingType,
        string? ns,
        string name
    )
    {
        throw new InvalidOperationException(
            $"Attempted to register type '{newType}' with the same namespace '{ns}' and name '{name}' as type '{existingType}'."
        );
    }
}
