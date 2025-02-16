using System;
using System.Diagnostics.CodeAnalysis;
using Fury.Meta;

namespace Fury;

public sealed class InvalidTypeRegistrationException(string? message = null) : Exception(message);

internal static partial class ThrowHelper
{
    [DoesNotReturn]
    public static void ThrowInvalidTypeRegistrationException_CannotFindRegistrationByName(string fullName)
    {
        throw new InvalidTypeRegistrationException($"Cannot find registration by name '{fullName}'.");
    }

    [DoesNotReturn]
    public static void ThrowInvalidTypeRegistrationException_CannotFindRegistrationById(int typeId)
    {
        throw new InvalidTypeRegistrationException($"Cannot find registration by ID '{typeId}'.");
    }

    [DoesNotReturn]
    public static void ThrowInvalidTypeRegistrationException_CannotFindRegistrationByTypeKind(
        TypeKind typeKind,
        Type declaredType
    )
    {
        throw new InvalidTypeRegistrationException(
            $"Cannot find registration by type kind '{typeKind}' and declared type '{declaredType}'."
        );
    }

    [DoesNotReturn]
    public static void ThrowInvalidTypeRegistrationException_NoCustomSerializer(Type type)
    {
        throw new InvalidTypeRegistrationException(
            $"Type '{type}' uses custom deserializer but no custom serializer is provided."
        );
    }

    [DoesNotReturn]
    public static void ThrowInvalidTypeRegistrationException_NoCustomDeserializer(Type type)
    {
        throw new InvalidTypeRegistrationException(
            $"Type '{type}' uses custom serializer but no custom deserializer is provided."
        );
    }
}
