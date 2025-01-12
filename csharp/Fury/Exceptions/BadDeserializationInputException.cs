using System;
using System.Diagnostics.CodeAnalysis;
using Fury.Meta;

namespace Fury;

public class BadDeserializationInputException(string? message = null) : Exception(message);

internal static partial class ThrowHelper
{
    [DoesNotReturn]
    public static void ThrowBadSerializationDataException(string? message = null)
    {
        throw new BadDeserializationInputException(message);
    }

    [DoesNotReturn]
    public static TReturn ThrowBadSerializationDataException<TReturn>(string? message = null)
    {
        throw new BadDeserializationInputException(message);
    }

    [DoesNotReturn]
    public static void ThrowBadSerializationDataException_UnrecognizedMetaStringCodePoint(byte codePoint)
    {
        throw new BadDeserializationInputException($"Unrecognized MetaString code point: {codePoint}");
    }

    [DoesNotReturn]
    public static void ThrowBadSerializationDataException_UpperCaseFlagCannotAppearConsecutively()
    {
        throw new BadDeserializationInputException(
            $"The '{AllToLowerSpecialEncoding.UpperCaseFlag}' cannot appear consecutively"
        );
    }
}
