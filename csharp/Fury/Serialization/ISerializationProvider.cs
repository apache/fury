using System;
using System.Diagnostics.CodeAnalysis;
using Fury.Context;
using Fury.Meta;

namespace Fury.Serialization;

public interface ISerializationProvider
{
    bool TryGetTypeName(Type targetType, out string? @namespace, [NotNullWhen(true)] out string? name);
    bool TryGetType(string? @namespace, string? name, [NotNullWhen(true)] out Type? targetType);
    bool TryGetType(TypeKind targetTypeKind, Type declaredType, [NotNullWhen(true)] out Type? targetType);
    bool TryGetTypeKind(Type targetType, out TypeKind targetTypeKind);
    bool TryGetSerializerFactory(
        TypeRegistry registry,
        Type targetType,
        [NotNullWhen(true)] out Func<ISerializer>? serializerFactory
    );
    bool TryGetDeserializerFactory(
        TypeRegistry registry,
        Type targetType,
        [NotNullWhen(true)] out Func<IDeserializer>? deserializerFactory
    );
}

[Flags]
internal enum ProviderSource
{
    BuiltIn = 1, // 01
    Custom = 2, // 10
    Both = 3, // 11
}
