using System;
using System.Diagnostics.CodeAnalysis;
using Fury.Context;
using Fury.Meta;

namespace Fury.Serialization;

public sealed class BuiltInTypeRegistrationProvider : ITypeRegistrationProvider
{
    public TypeRegistration RegisterType(TypeRegistry registry, Type targetType)
    {
        if (!TryRegisterType(registry, targetType, out var registration))
        {
            ThrowNotSupportedException_TypeNotSupported(targetType);
        }

        return registration;
    }

    public TypeRegistration GetTypeRegistration(TypeRegistry registry, TypeKind targetTypeKind, Type declaredType)
    {
        throw new NotImplementedException();
    }

    public TypeRegistration GetTypeRegistration(TypeRegistry registry, int id)
    {
        throw new NotImplementedException();
    }

    public TypeRegistration GetTypeRegistration(TypeRegistry registry, string? @namespace, string name)
    {
        throw new NotImplementedException();
    }

    public bool TryRegisterType(TypeRegistry registry, Type targetType, [NotNullWhen(true)] out TypeRegistration? registration)
    {
        if (EnumTypeRegistrationProvider.TryRegisterType(registry, targetType, out registration))
        {
            return true;
        }

        if (ArrayTypeRegistrationProvider.TryRegisterType(registry, targetType, out registration))
        {
            return true;
        }

        return false;
    }

    [DoesNotReturn]
    private void ThrowNotSupportedException_TypeNotSupported(Type targetType)
    {
        throw new NotSupportedException($"Type `{targetType}` is not supported by built-in type registration provider.");
    }
}
