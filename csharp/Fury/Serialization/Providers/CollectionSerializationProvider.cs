using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Contracts;
using System.Reflection;
using Fury.Context;
using Fury.Meta;

namespace Fury.Serialization;

internal static class CollectionSerializationProvider
{
    private const string EnumerableInterfaceName = nameof(IEnumerable);
    private static readonly string GenericEnumerableInterfaceName = typeof(IEnumerable<>).Name;
    private static readonly string ListInterfaceName = typeof(IList<>).Name;
    private static readonly string DictionaryInterfaceName = typeof(IDictionary<,>).Name;
    private static readonly string SetInterfaceName = typeof(ISet<>).Name;
    private static readonly string CollectionInterfaceName = typeof(ICollection<>).Name;

    private static readonly MethodInfo CreateEnumerableSerializerMethod =
        typeof(CollectionSerializationProvider).GetMethod(
            nameof(CreateEnumerableSerializer),
            BindingFlags.NonPublic | BindingFlags.Static
        )!;

    private static readonly MethodInfo CreateNullableEnumerableSerializerMethod =
        typeof(CollectionSerializationProvider).GetMethod(
            nameof(CreateNullableEnumerableSerializer),
            BindingFlags.NonPublic | BindingFlags.Static
        )!;

    private static readonly MethodInfo CreateListDeserializerMethod = typeof(CollectionSerializationProvider).GetMethod(
        nameof(CreateListDeserializer),
        BindingFlags.NonPublic | BindingFlags.Static
    )!;

    private static readonly MethodInfo CreateNullableListDeserializerMethod =
        typeof(CollectionSerializationProvider).GetMethod(
            nameof(CreateNullableListDeserializer),
            BindingFlags.NonPublic | BindingFlags.Static
        )!;

    public static bool TryGetType(TypeKind targetTypeKind, Type declaredType, [NotNullWhen(true)] out Type? targetType)
    {
        targetType = null;
        if (!TryGetElementType(declaredType, true, out var elementType))
        {
            return false;
        }

        // TODO: Add support for Dictionary<,> and Set<>
        if (targetTypeKind is TypeKind.List)
        {
            var listType = typeof(List<>).MakeGenericType(elementType);
            if (declaredType.IsAssignableFrom(listType))
            {
                targetType = listType;
                return true;
            }
        }

        return false;
    }

    public static bool TryGetTypeKind(Type targetType, out TypeKind targetTypeKind)
    {
        if (targetType.IsArray && targetType.GetArrayRank() == 1)
        {
            // Variable bound array is not supported yet.
            targetTypeKind = TypeKind.List;
            return true;
        }

        if (targetType is { IsGenericType: true, IsGenericTypeDefinition: false })
        {
            if (targetType.GetInterface(ListInterfaceName) is not null)
            {
                targetTypeKind = TypeKind.List;
                return true;
            }

            if (targetType.GetInterface(DictionaryInterfaceName) is not null)
            {
                targetTypeKind = TypeKind.Map;
                return true;
            }

            if (targetType.GetInterface(SetInterfaceName) is not null)
            {
                targetTypeKind = TypeKind.Set;
                return true;
            }
        }

        targetTypeKind = default;
        return false;
    }

    [Pure]
    private static bool TryGetElementType(Type targetType, bool allowAbstract, [NotNullWhen(true)] out Type? elementType)
    {
        elementType = null;
        if (targetType.IsAbstract && !allowAbstract)
        {
            return false;
        }

        if (targetType.GetInterface(GenericEnumerableInterfaceName) is { } enumerableInterface)
        {
            elementType = enumerableInterface.GetGenericArguments()[0];
        }
        else if(targetType.GetInterface(EnumerableInterfaceName) is not null)
        {
            elementType = typeof(object);
        }
        else
        {
            return false;
        }

        return !elementType.IsGenericParameter;
    }

    public static bool TryGetSerializerFactory(
        TypeRegistry registry,
        Type targetType,
        [NotNullWhen(true)] out Func<ISerializer>? serializerFactory
    )
    {
        serializerFactory = null;
        if (!TryGetElementType(targetType, false, out var elementType))
        {
            return false;
        }

        var underlyingType = Nullable.GetUnderlyingType(elementType);
        MethodInfo selectMethod;
        if (underlyingType is null)
        {
            selectMethod = CreateEnumerableSerializerMethod;
        }
        else
        {
            selectMethod = CreateNullableEnumerableSerializerMethod;
            elementType = underlyingType;
        }

        var createMethod =
            (Func<TypeRegistration?, ISerializer>)
                selectMethod
                    .MakeGenericMethod(elementType, targetType)
                    .CreateDelegate(typeof(Func<TypeRegistration?, ISerializer>));

        if (elementType.IsSealed)
        {
            var elementRegistration = registry.GetTypeRegistration(elementType);
            serializerFactory = () => createMethod(elementRegistration);
        }
        else
        {
            serializerFactory = () => createMethod(null);
        }

        return true;
    }

    private static ISerializer CreateEnumerableSerializer<TElement, TEnumerable>(TypeRegistration? elementRegistration)
        where TElement : notnull
        where TEnumerable : IEnumerable<TElement>
    {
        return new EnumerableSerializer<TElement, TEnumerable>(elementRegistration);
    }

    private static ISerializer CreateNullableEnumerableSerializer<TElement, TEnumerable>(
        TypeRegistration? elementRegistration
    )
        where TElement : struct
        where TEnumerable : IEnumerable<TElement?>
    {
        return new NullableEnumerableSerializer<TElement, TEnumerable>(elementRegistration);
    }

    private static bool TryGetDeserializerFactoryCommon(TypeRegistry registry, Type desiredType, Type elementType,
        out Func<IDeserializer>? deserializerFactory)
    {
        Debug.Assert(!desiredType.IsAbstract);
        Debug.Assert(!desiredType.IsGenericTypeDefinition);
        var underlyingType = Nullable.GetUnderlyingType(elementType);
        MethodInfo? createMethodInfo = null;
        // TODO: Add support for Dictionary<,> and Set<>
        if (underlyingType is null)
        {
            if (desiredType.GetGenericTypeDefinition() == typeof(List<>))
            {
                createMethodInfo = CreateListDeserializerMethod;
            }
        }
        else
        {
            if (desiredType.GetGenericTypeDefinition() == typeof(List<>))
            {
                createMethodInfo = CreateNullableListDeserializerMethod;
            }
        }

        if (createMethodInfo is null)
        {
            deserializerFactory = null;
            return false;
        }

        var createMethod =
            (Func<TypeRegistration?, IDeserializer>)
            createMethodInfo
                .MakeGenericMethod(elementType)
                .CreateDelegate(typeof(Func<TypeRegistration?, IDeserializer>));

        if (elementType.IsSealed)
        {
            var elementRegistration = registry.GetTypeRegistration(elementType);
            deserializerFactory = () => createMethod(elementRegistration);
        }
        else
        {
            deserializerFactory = () => createMethod(null);
        }

        return true;
    }

    public static bool TryGetDeserializerFactory(
        TypeRegistry registry,
        Type targetType,
        [NotNullWhen(true)] out Func<IDeserializer>? deserializerFactory
    )
    {
        deserializerFactory = null;
        if (!TryGetElementType(targetType, false, out var elementType))
        {
            return false;
        }
        if (targetType.GetInterface(CollectionInterfaceName) is null)
        {
            return false;
        }

        return TryGetDeserializerFactoryCommon(registry, targetType, elementType, out deserializerFactory);
    }

    private static IDeserializer CreateListDeserializer<TElement>(TypeRegistration? elementRegistration)
        where TElement : notnull
    {
        return new ListDeserializer<TElement>(elementRegistration);
    }

    private static IDeserializer CreateNullableListDeserializer<TElement>(TypeRegistration? elementRegistration)
        where TElement : struct
    {
        return new NullableListDeserializer<TElement>(elementRegistration);
    }
}
