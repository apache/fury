using System;
using System.Diagnostics;
using Fury.Buffers;
using Fury.Meta;
using Fury.Serialization;
using JetBrains.Annotations;

namespace Fury.Context;

public class TypeRegistration
{
    private readonly TypeRegistry _registry;

    private readonly ObjectPool<ISerializer>? _serializerPool;
    private readonly ObjectPool<IDeserializer>? _deserializerPool;

    [PublicAPI]
    public Func<ISerializer>? SerializerFactory { get; private set; }

    [PublicAPI]
    public Func<IDeserializer>? DeserializerFactory { get; private set; }

    internal MetaString NameMetaString { get; }
    internal MetaString NamespaceMetaString { get; }

    public bool UseCustomSerialization { get; }

    public bool IsNamed { get; }
    public string? Namespace { get; }
    public string Name { get; }

    public Type TargetType { get; }
    public TypeKind? TypeKind { get; }
    internal InternalTypeKind InternalTypeKind { get; }

    internal TypeRegistration(
        TypeRegistry registry,
        Type targetType,
        InternalTypeKind internalTypeKind,
        string? ns,
        string name,
        bool isNamed,
        Func<ISerializer>? serializerFactory,
        Func<IDeserializer>? deserializerFactory,
        bool useCustomSerialization
    )
    {
        _registry = registry;
        TargetType = targetType;

        SerializerFactory = serializerFactory;
        DeserializerFactory = deserializerFactory;
        UseCustomSerialization = useCustomSerialization;
        if (serializerFactory is not null)
        {
            _serializerPool = new ObjectPool<ISerializer>(_ => serializerFactory());
        }

        if (deserializerFactory is not null)
        {
            _deserializerPool = new ObjectPool<IDeserializer>(_ => deserializerFactory());
        }

        Namespace = ns;
        Name = name;
        IsNamed = isNamed;
        GetMetaStrings(out var namespaceMetaString, out var nameMetaString);
        NamespaceMetaString = namespaceMetaString;
        NameMetaString = nameMetaString;

        InternalTypeKind = internalTypeKind;
        if (internalTypeKind.TryToBeTypeKind(out var typeKind))
        {
            TypeKind = typeKind;
        }
        else
        {
            TypeKind = null;
        }
    }

    internal ISerializer RentSerializer()
    {
        CheckPool(true);
        Debug.Assert(_serializerPool is not null);
        return _serializerPool.Rent();
    }

    internal void ReturnSerializer(ISerializer serializer)
    {
        CheckPool(true);
        Debug.Assert(_serializerPool is not null);
        _serializerPool.Return(serializer);
    }

    internal IDeserializer RentDeserializer()
    {
        CheckPool(false);
        Debug.Assert(_deserializerPool is not null);
        return _deserializerPool.Rent();
    }

    internal void ReturnDeserializer(IDeserializer deserializer)
    {
        CheckPool(false);
        Debug.Assert(_deserializerPool is not null);
        _deserializerPool.Return(deserializer);
    }

    private void GetMetaStrings(out MetaString namespaceMetaString, out MetaString nameMetaString)
    {
        var storage = _registry.MetaStringStorage;
        namespaceMetaString = storage.GetMetaString(Name, MetaStringStorage.EncodingPolicy.Namespace);
        nameMetaString = storage.GetMetaString(Namespace, MetaStringStorage.EncodingPolicy.Name);
    }

    private void CheckPool(bool isSerializer)
    {
        if (isSerializer)
        {
            if (_serializerPool is not null)
            {
                return;
            }

            if (UseCustomSerialization)
            {
                ThrowHelper.ThrowInvalidTypeRegistrationException_NoCustomSerializer(TargetType);
            }
            else
            {
                ThrowHelper.ThrowNotSupportedException_NotSupportedBuiltInSerializer(TargetType);
            }
        }
        else
        {
            if (_deserializerPool is not null)
            {
                return;
            }

            if (UseCustomSerialization)
            {
                ThrowHelper.ThrowInvalidTypeRegistrationException_NoCustomDeserializer(TargetType);
            }
            else
            {
                ThrowHelper.ThrowNotSupportedException_NotSupportedBuiltInDeserializer(TargetType);
            }
        }
    }
}
