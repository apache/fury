﻿using System;
using System.Diagnostics.CodeAnalysis;

namespace Fury;

internal static partial class ThrowHelper
{

    [DoesNotReturn]
    public static void ThrowArgumentException_InsufficientSpaceInTheOutputBuffer(string? paramName = null)
    {
        throw new ArgumentException("Insufficient space in the output buffer.", paramName);
    }
}
