using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Fury.Meta;
using Fury.Serialization;

namespace Fury.Context;

public sealed class TypeRegistry
{
    private record struct CompositeDeclaredType(TypeKind TypeKind, Type DeclaredType);
    private record struct CompositeName(string? Namespace, string Name);

    private readonly ConcurrentDictionary<Type, TypeRegistration> _typeToRegistrations = new();
    private readonly ConcurrentDictionary<CompositeDeclaredType, TypeRegistration> _declaredTypeToRegistrations = new();
    private readonly ConcurrentDictionary<CompositeName, TypeRegistration> _nameToRegistrations = new();
    private readonly ConcurrentDictionary<int, TypeRegistration> _typeIdToRegistrations = new();

    private readonly HybridProvider _builtInProvider = new();
    private readonly ISerializationProvider _customProvider;

    internal MetaStringStorage MetaStringStorage { get; } = new();

    internal TypeRegistry(ISerializationProvider customProvider)
    {
        _customProvider = customProvider;
    }

    public TypeRegistration GetTypeRegistration(Type type)
    {
        var typeRegistration = _typeToRegistrations.GetOrAdd(
            type,
            t =>
            {
                var useCustomSerialization =
                    TryGetSerializerFactoryFromProvider(t, ProviderSource.Custom, out var serializerFactory)
                    | TryGetDeserializerFactoryFromProvider(t, ProviderSource.Custom, out var deserializerFactory);
                Debug.Assert(!useCustomSerialization == (serializerFactory is null && deserializerFactory is null));
                if (!useCustomSerialization)
                {
                    var success = TryGetSerializerFactoryFromProvider(t, ProviderSource.BuiltIn, out serializerFactory);
                    Debug.Assert(success && serializerFactory is not null);
                    success = TryGetDeserializerFactoryFromProvider(t, ProviderSource.BuiltIn, out deserializerFactory);
                    Debug.Assert(success && deserializerFactory is not null);
                }
                var isNamed = TryGetTypeNameFromProvider(t, ProviderSource.Custom, out var ns, out var name);
                Debug.Assert(!isNamed == name is null);
                if (!isNamed)
                {
                    var success = TryGetTypeNameFromProvider(t, ProviderSource.BuiltIn, out ns, out name);
                    Debug.Assert(success && name is not null);
                }
                var internalTypeKind = GetTypeKind(t, useCustomSerialization, isNamed);
                var newRegistration = new TypeRegistration(
                    this,
                    t,
                    internalTypeKind,
                    ns,
                    name!,
                    isNamed,
                    serializerFactory!,
                    deserializerFactory!,
                    useCustomSerialization
                );
                if (newRegistration.TypeKind is { } typeKind)
                {
                    _declaredTypeToRegistrations[new CompositeDeclaredType(typeKind, newRegistration.TargetType)] =
                        newRegistration;
                }

                var registered = _nameToRegistrations.GetOrAdd(new CompositeName(ns, name!), newRegistration);
                if (registered != newRegistration)
                {
                    ThrowHelper.ThrowInvalidOperationException_TypeNameCollision(
                        newRegistration.TargetType,
                        registered.TargetType,
                        ns,
                        name!
                    );
                }

                return newRegistration;
            }
        );

        return typeRegistration;
    }

    public bool TryGetTypeRegistration(string? ns, string name, [NotNullWhen(true)] out TypeRegistration? registration)
    {
        return _nameToRegistrations.TryGetValue(new CompositeName(ns, name), out registration);
    }

    public bool TryGetTypeRegistration(
        TypeKind typeKind,
        Type declaredType,
        [NotNullWhen(true)] out TypeRegistration? registration
    )
    {
        if (_declaredTypeToRegistrations.TryGetValue(new CompositeDeclaredType(typeKind, declaredType), out registration))
        {
            return true;
        }

        if (!TryGetTypeFromProvider(typeKind, declaredType, ProviderSource.Both, out var targetType))
        {
            return false;
        }

        registration = GetTypeRegistration(targetType);
        return true;
    }

    public bool TryGetTypeRegistration(int typeId, [NotNullWhen(true)] out TypeRegistration? registration)
    {
        return _typeIdToRegistrations.TryGetValue(typeId, out registration);
    }

    private InternalTypeKind GetTypeKind(Type targetType, bool useCustomSerialization, bool isNamed)
    {
        InternalTypeKind internalTypeKind;
        if (TryGetTypeKindFromProvider(targetType, ProviderSource.Both, out var typeKind))
        {
            internalTypeKind = typeKind.ToInternal();
        }
        else
        {
            // Other prefixes, such as "Polymorphic" or "Compatible", depend on configuration and object being serialized.
            // We can't determine them here, so we'll just use the "Struct" and "Ext" and handle them in the serialization code.

            if (targetType.IsEnum)
            {
                internalTypeKind = isNamed ? InternalTypeKind.NamedEnum : InternalTypeKind.Enum;
            }
            else if (useCustomSerialization)
            {
                internalTypeKind = isNamed ? InternalTypeKind.NamedExt : InternalTypeKind.Ext;
            }
            else
            {
                internalTypeKind = isNamed ? InternalTypeKind.NamedStruct : InternalTypeKind.Struct;
            }
        }

        return internalTypeKind;
    }

    private bool TryGetTypeNameFromProvider(
        Type targetType,
        ProviderSource source,
        out string? ns,
        [NotNullWhen(true)] out string? name
    )
    {
        var success = false;
        ns = null;
        name = null;
        if (source.HasFlag(ProviderSource.Custom))
        {
            success = _customProvider.TryGetTypeName(targetType, out ns, out name);
        }
        if (!success && source.HasFlag(ProviderSource.BuiltIn))
        {
            success = _builtInProvider.TryGetTypeName(targetType, out ns, out name);
        }
        return success;
    }

    private bool TryGetTypeFromProvider(
        string? ns,
        string? name,
        ProviderSource source,
        [NotNullWhen(true)] out Type? targetType
    )
    {
        var success = false;
        targetType = null;
        if (source.HasFlag(ProviderSource.Custom))
        {
            success = _customProvider.TryGetType(ns, name, out targetType);
        }
        if (!success && source.HasFlag(ProviderSource.BuiltIn))
        {
            success = _builtInProvider.TryGetType(ns, name, out targetType);
        }
        return success;
    }

    private bool TryGetTypeFromProvider(
        TypeKind typeKind,
        Type declaredType,
        ProviderSource source,
        [NotNullWhen(true)] out Type? targetType
    )
    {
        var success = false;
        targetType = null;
        if (source.HasFlag(ProviderSource.Custom))
        {
            success = _customProvider.TryGetType(typeKind, declaredType, out targetType);
        }
        if (!success && source.HasFlag(ProviderSource.BuiltIn))
        {
            success = _builtInProvider.TryGetType(typeKind, declaredType, out targetType);
        }
        return success;
    }

    private bool TryGetTypeKindFromProvider(Type targetType, ProviderSource source, out TypeKind targetTypeKind)
    {
        var success = false;
        targetTypeKind = default;
        if (source.HasFlag(ProviderSource.Custom))
        {
            success = _customProvider.TryGetTypeKind(targetType, out targetTypeKind);
        }
        if (!success && source.HasFlag(ProviderSource.BuiltIn))
        {
            success = _builtInProvider.TryGetTypeKind(targetType, out targetTypeKind);
        }
        return success;
    }

    internal bool TryGetSerializerFactoryFromProvider(
        Type targetType,
        ProviderSource source,
        [NotNullWhen(true)] out Func<ISerializer>? serializerFactory
    )
    {
        var success = false;
        serializerFactory = null;
        if (source.HasFlag(ProviderSource.Custom))
        {
            success = _customProvider.TryGetSerializerFactory(this, targetType, out serializerFactory);
        }
        if (!success && source.HasFlag(ProviderSource.BuiltIn))
        {
            success = _builtInProvider.TryGetSerializerFactory(this, targetType, out serializerFactory);
        }
        return success;
    }

    internal bool TryGetDeserializerFactoryFromProvider(
        Type targetType,
        ProviderSource source,
        [NotNullWhen(true)] out Func<IDeserializer>? deserializerFactory
    )
    {
        var success = false;
        deserializerFactory = null;
        if (source.HasFlag(ProviderSource.Custom))
        {
            success = _customProvider.TryGetDeserializerFactory(this, targetType, out deserializerFactory);
        }
        if (!success && source.HasFlag(ProviderSource.BuiltIn))
        {
            success = _builtInProvider.TryGetDeserializerFactory(this, targetType, out deserializerFactory);
        }
        return success;
    }
}
