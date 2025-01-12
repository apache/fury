﻿using System;
using System.Diagnostics.CodeAnalysis;

namespace Fury;

public sealed class BadSerializationInputException(string? message = null) : Exception(message);

internal static partial class ThrowHelper
{
    [DoesNotReturn]
    public static void ThrowBadSerializationInputException(string? message = null)
    {
        throw new BadSerializationInputException(message);
    }

    [DoesNotReturn]
    public static TReturn ThrowBadSerializationInputException<TReturn>(string? message = null)
    {
        throw new BadSerializationInputException(message);
    }

    [DoesNotReturn]
    public static void ThrowBadSerializationInputException_UnsupportedMetaStringChar(char c)
    {
        throw new BadSerializationInputException($"Unsupported MetaString character: '{c}'");
    }
}