using System;
using System.Diagnostics.CodeAnalysis;
using Fury.Context;
using Fury.Meta;

namespace Fury.Serialization;

internal sealed class HybridProvider : ISerializationProvider
{
    public bool TryGetTypeName(Type targetType, out string? @namespace, [NotNullWhen(true)] out string? name)
    {
        @namespace = targetType.Namespace;
        name = targetType.Name;
        return true;
    }

    public bool TryGetType(string? @namespace, string? name, [NotNullWhen(true)] out Type? targetType)
    {
        ThrowHelper.ThrowNotSupportedException_SearchTypeByNamespaceAndName();
        targetType = null;
        return false;
    }

    public bool TryGetType(TypeKind targetTypeKind, Type declaredType, [NotNullWhen(true)] out Type? targetType)
    {
        return ArraySerializationProvider.TryGetType(targetTypeKind, declaredType, out targetType)
            || CollectionSerializationProvider.TryGetType(targetTypeKind, declaredType, out targetType);
    }

    public bool TryGetTypeKind(Type targetType, out TypeKind targetTypeKind)
    {
        // Enum can be named or unnamed, we can't determine the type kind here
        var success = CollectionSerializationProvider.TryGetTypeKind(targetType, out targetTypeKind);
        success = success || ArraySerializationProvider.TryGetTypeKind(targetType, out targetTypeKind);
        return success;
    }

    public bool TryGetSerializerFactory(
        TypeRegistry registry,
        Type targetType,
        [NotNullWhen(true)] out Func<ISerializer>? serializerFactory
    )
    {
        return EnumSerializationProvider.TryGetSerializerFactory(registry, targetType, out serializerFactory)
            || ArraySerializationProvider.TryGetSerializerFactory(registry, targetType, out serializerFactory)
            || CollectionSerializationProvider.TryGetSerializerFactory(registry, targetType, out serializerFactory);
    }

    public bool TryGetDeserializerFactory(
        TypeRegistry registry,
        Type targetType,
        [NotNullWhen(true)] out Func<IDeserializer>? deserializerFactory
    )
    {
        return EnumSerializationProvider.TryGetDeserializerFactory(registry, targetType, out deserializerFactory)
            || ArraySerializationProvider.TryGetDeserializerFactory(registry, targetType, out deserializerFactory)
            || CollectionSerializationProvider.TryGetDeserializerFactory(registry, targetType, out deserializerFactory);
    }
}
