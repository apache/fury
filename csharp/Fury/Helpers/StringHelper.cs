using System;
using System.Buffers;

namespace Fury;

internal sealed class StringHelper
{
    public static string Create<TState>(int length, in TState state, SpanAction<char, TState> action)
    {
        if (length == 0)
        {
            return string.Empty;
        }
#if NET8_0_OR_GREATER
        return string.Create(length, state, action);
#else
        if (length <= StaticConfigs.CharStackAllocLimit)
        {
            Span<char> chars = stackalloc char[length];
            action(chars, state);
            return chars.ToString();
        }
        else
        {
            var chars = ArrayPool<char>.Shared.Rent(length);
            try
            {
                action(chars, state);
                return new string(chars, 0, length);
            }
            finally
            {
                ArrayPool<char>.Shared.Return(chars);
            }
        }
#endif
    }

    public static string ToFullName(string? ns, string name)
    {
        if (ns is null)
        {
            return name;
        }
        return ns + "." + name;
    }
}
