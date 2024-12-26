using System;
using System.Diagnostics.CodeAnalysis;

namespace Fury;

public class BadSerializationDataException(string? message = null) : Exception(message);

internal static partial class ThrowHelper
{
    [DoesNotReturn]
    public static void ThrowBadSerializationDataException(string? message = null)
    {
        throw new BadSerializationDataException(message);
    }

    [DoesNotReturn]
    public static TReturn ThrowBadSerializationDataException<TReturn>(string? message = null)
    {
        throw new BadSerializationDataException(message);
    }
}
