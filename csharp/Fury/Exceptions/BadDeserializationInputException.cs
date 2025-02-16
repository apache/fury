using System;
using System.Diagnostics.CodeAnalysis;
using Fury.Meta;

namespace Fury;

public class BadDeserializationInputException(string? message = null, Exception? innerException = null) : Exception(message, innerException);

internal static partial class ThrowHelper
{
    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException(string? message = null)
    {
        throw new BadDeserializationInputException(message);
    }

    [DoesNotReturn]
    public static TReturn ThrowBadDeserializationInputException<TReturn>(string? message = null)
    {
        throw new BadDeserializationInputException(message);
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_UnrecognizedMetaStringCodePoint(byte codePoint)
    {
        throw new BadDeserializationInputException($"Unrecognized MetaString code point: {codePoint}");
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_UpperCaseFlagCannotAppearConsecutively()
    {
        throw new BadDeserializationInputException(
            $"The '{AllToLowerSpecialEncoding.UpperCaseFlag}' cannot appear consecutively"
        );
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_UnknownMetaStringId(int id)
    {
        throw new BadDeserializationInputException($"Unknown MetaString ID: {id}");
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_TypeRegistrationNotFound(InternalTypeKind kind)
    {
        throw new BadDeserializationInputException($"No type registration found for type kind '{kind}'.");
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_ReferencedObjectNotFound(RefId refId)
    {
        throw new BadDeserializationInputException($"Referenced object not found for ref kind '{refId}'.");
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_InsufficientData()
    {
        throw new BadDeserializationInputException("Insufficient data.");
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_VarInt32Overflow()
    {
        throw new BadDeserializationInputException("VarInt32 overflow.");
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_InvalidMagicNumber()
    {
        throw new BadDeserializationInputException("Invalid magic number.");
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_NotCrossLanguage()
    {
        throw new BadDeserializationInputException("Not cross language.");
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_NotLittleEndian()
    {
        throw new BadDeserializationInputException("Not little endian.");
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_NoDeserializerFactoryProvider(Type targetType)
    {
        throw new BadDeserializationInputException(
            $"Can not find an appropriate deserializer factory provider for type '{targetType.FullName}'."
        );
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_UnrecognizedTypeKind(int kind, Exception innerException)
    {
        throw new BadDeserializationInputException($"Unrecognized type kind: {kind}", innerException);
    }

    [DoesNotReturn]
    public static void ThrowBadDeserializationInputException_BadMetaStringHashCodeOrBytes()
    {
        throw new BadDeserializationInputException("The bytes of meta string do not match the prefixed hash code.");
    }
}
