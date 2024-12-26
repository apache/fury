using System;
using System.Diagnostics.CodeAnalysis;

namespace Fury;

public class UnregisteredTypeException(Type objectType, string? message = null) : Exception(message)
{
    public Type ObjectType { get; } = objectType;
}

internal static partial class ThrowHelper
{
    [DoesNotReturn]
    public static void ThrowUnregisteredTypeException(Type objectType, string? message = null)
    {
        throw new UnregisteredTypeException(objectType, message);
    }
}
