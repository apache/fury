using System;
using System.Diagnostics.CodeAnalysis;

namespace Fury;

internal partial class ThrowHelper
{
    [DoesNotReturn]
    public static void ThrowIndexOutOfRangeException()
    {
        throw new IndexOutOfRangeException();
    }
}
