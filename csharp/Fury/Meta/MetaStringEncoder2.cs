using System;
using System.Text;

namespace Fury.Meta;

internal sealed class MetaStringEncoder2
{
    private const char ArrayPrefix = '1';
    private const char EnumPrefix = '2';

    private readonly StringBuilder _sharedStringBuilder = new();

    public (string namespaceName, string name) EncodeNamespaceAndName(Type type)
    {
        var namespaceName = type.Namespace ?? string.Empty;
        var name = type.Name;
        if (type.IsArray)
        {
            if (TypeHelper.TryGetUnderlyingElementType(type, out var elementType, out var rank))
            {
                // primitive array has special format like [[[III.
                if (!elementType.IsPrimitive)
                {
                    namespaceName = elementType.Namespace ?? string.Empty;
                    _sharedStringBuilder.Append(ArrayPrefix, rank);
                    if (elementType.IsEnum)
                    {
                        _sharedStringBuilder.Append(EnumPrefix);
                    }

                    _sharedStringBuilder.Append(elementType.Name);
                    name = _sharedStringBuilder.ToString();
                    _sharedStringBuilder.Clear();
                }
            }
        }
        else if (type.IsEnum)
        {
            name = EnumPrefix + name;
        }

        return (namespaceName, name);
    }
}
