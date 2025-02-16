using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Fury.Collections;
using Fury.Context;
using Fury.Meta;

namespace Fury.Serialization.Meta;

internal struct TypeMetaSerializer
{
    private readonly AutoIncrementIdDictionary<MetaString> _sharedMetaStringContext;
    private MetaStringSerializer _nameMetaStringSerializer;
    private MetaStringSerializer _namespaceMetaStringSerializer;

    private bool _hasWrittenTypeKind;

    public TypeMetaSerializer()
    {
        _sharedMetaStringContext = new AutoIncrementIdDictionary<MetaString>();
        _nameMetaStringSerializer = new MetaStringSerializer(_sharedMetaStringContext);
        _namespaceMetaStringSerializer = new MetaStringSerializer(_sharedMetaStringContext);
    }

    public void Reset(bool clearContext)
    {
        _hasWrittenTypeKind = false;
        _nameMetaStringSerializer.Reset();
        _namespaceMetaStringSerializer.Reset();
        if (clearContext)
        {
            _sharedMetaStringContext.Clear();
        }
    }

    public bool Write(ref BatchWriter writer, TypeRegistration registration)
    {
        var typeKind = registration.InternalTypeKind;

        var completed = true;
        completed = completed && writer.TryWrite7BitEncodedUint((uint)typeKind, ref _hasWrittenTypeKind);
        if (typeKind.IsNamed())
        {
            completed = completed && _namespaceMetaStringSerializer.Write(ref writer, registration.NamespaceMetaString);
            completed = completed && _nameMetaStringSerializer.Write(ref writer, registration.NameMetaString);
        }

        return completed;
    }
}

internal struct TypeMetaDeserializer
{
    private readonly TypeRegistry _typeRegistry;
    private readonly AutoIncrementIdDictionary<MetaString> _sharedMetaStringContext;
    private MetaStringDeserializer _nameMetaStringDeserializer;
    private MetaStringDeserializer _namespaceMetaStringDeserializer;

    private uint? _compositeIdValue;
    private MetaString? _namespaceMetaString;
    private MetaString? _nameMetaString;

    public TypeMetaDeserializer(TypeRegistry registry)
    {
        _typeRegistry = registry;
        _sharedMetaStringContext = new AutoIncrementIdDictionary<MetaString>();
        _nameMetaStringDeserializer = new MetaStringDeserializer(
            _sharedMetaStringContext,
            registry.MetaStringStorage,
            MetaStringStorage.EncodingPolicy.Name
        );
        _namespaceMetaStringDeserializer = new MetaStringDeserializer(
            _sharedMetaStringContext,
            registry.MetaStringStorage,
            MetaStringStorage.EncodingPolicy.Namespace
        );
    }

    public void Reset(bool clearContext)
    {
        _compositeIdValue = null;
        _namespaceMetaString = null;
        _nameMetaString = null;
        _nameMetaStringDeserializer.Reset();
        _namespaceMetaStringDeserializer.Reset();
        if (clearContext)
        {
            _sharedMetaStringContext.Clear();
        }
    }

    public bool Read(BatchReader reader, Type declaredType, [NotNullWhen(true)] out TypeRegistration? registration)
    {
        var completed = reader.TryRead7BitEncodedUint(ref _compositeIdValue);
        registration = null;
        if (_compositeIdValue is null || !completed)
        {
            return false;
        }

        var compositeId = CompositeTypeKind.FromUint(_compositeIdValue.Value);
        var (internalTypeKind, typeId) = compositeId;
        if (internalTypeKind.TryToBeTypeKind(out var typeKind))
        {
            registration = GetRegistrationByTypeKind(typeKind, declaredType);
        }
        else
        {
            if (internalTypeKind.IsNamed())
            {
                completed = completed && _namespaceMetaStringDeserializer.Read(reader, ref _namespaceMetaString);
                completed = completed && _nameMetaStringDeserializer.Read(reader, ref _nameMetaString);
                if (completed)
                {
                    registration = GetRegistrationByName();
                }
            }
            else
            {
                registration = GetRegistrationById();
            }
        }

        return completed;
    }

    public async ValueTask<TypeRegistration> ReadAsync(
        BatchReader reader,
        Type declaredType,
        CancellationToken cancellationToken = default
    )
    {
        _compositeIdValue ??= await reader.Read7BitEncodedUintAsync(cancellationToken);
        var compositeId = CompositeTypeKind.FromUint(_compositeIdValue.Value);
        var (internalTypeKind, typeId) = compositeId;
        TypeRegistration registration;
        if (internalTypeKind.TryToBeTypeKind(out var typeKind))
        {
            registration = GetRegistrationByTypeKind(typeKind, declaredType);
        }
        else
        {
            if (internalTypeKind.IsNamed())
            {
                _namespaceMetaString ??= await _namespaceMetaStringDeserializer.ReadAsync(reader, cancellationToken);
                _nameMetaString ??= await _nameMetaStringDeserializer.ReadAsync(reader, cancellationToken);

                registration = GetRegistrationByName();
            }
            else
            {
                registration = GetRegistrationById();
            }
        }

        return registration;
    }

    private TypeRegistration GetRegistrationByTypeKind(TypeKind typeKind, Type declaredType)
    {
        if (!_typeRegistry.TryGetTypeRegistration(typeKind, declaredType, out var registration))
        {
            ThrowHelper.ThrowInvalidTypeRegistrationException_CannotFindRegistrationByTypeKind(typeKind, declaredType);
        }

        return registration;
    }

    private TypeRegistration GetRegistrationByName()
    {
        Debug.Assert(_namespaceMetaString is not null);
        Debug.Assert(_nameMetaString is not null);

        var ns = _namespaceMetaString?.Value;
        var name = _nameMetaString!.Value;
        if (!_typeRegistry.TryGetTypeRegistration(ns, name, out var registration))
        {
            ThrowHelper.ThrowInvalidTypeRegistrationException_CannotFindRegistrationByName(StringHelper.ToFullName(ns, name));
        }

        return registration;
    }

    private TypeRegistration GetRegistrationById()
    {
        Debug.Assert(_compositeIdValue is not null);
        var typeId = CompositeTypeKind.FromUint(_compositeIdValue.Value).TypeId;
        if (!_typeRegistry.TryGetTypeRegistration(typeId, out var registration))
        {
            ThrowHelper.ThrowInvalidTypeRegistrationException_CannotFindRegistrationById(typeId);
        }

        return registration;
    }
}
