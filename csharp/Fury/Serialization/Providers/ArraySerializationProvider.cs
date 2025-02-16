using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Contracts;
using System.Reflection;
using Fury.Context;
using Fury.Meta;

namespace Fury.Serialization;

internal static class ArraySerializationProvider
{
    private static readonly MethodInfo CreateArraySerializerMethod = typeof(ArraySerializationProvider).GetMethod(
        nameof(CreateArraySerializer),
        BindingFlags.NonPublic | BindingFlags.Static
    )!;
    private static readonly MethodInfo CreateNullableArraySerializerMethod =
        typeof(ArraySerializationProvider).GetMethod(
            nameof(CreateNullableArraySerializer),
            BindingFlags.NonPublic | BindingFlags.Static
        )!;

    private static readonly MethodInfo CreateArrayDeserializerMethod = typeof(ArraySerializationProvider).GetMethod(
        nameof(CreateArrayDeserializer),
        BindingFlags.NonPublic | BindingFlags.Static
    )!;

    private static readonly MethodInfo CreateNullableArrayDeserializerMethod =
        typeof(ArraySerializationProvider).GetMethod(
            nameof(CreateNullableArrayDeserializer),
            BindingFlags.NonPublic | BindingFlags.Static
        )!;

    public static bool TryGetType(TypeKind targetTypeKind, Type declaredType, [NotNullWhen(true)] out Type? targetType)
    {
        targetType = null;
        if (!TryGetElementType(targetTypeKind, out var candidateElementTypes))
        {
            return false;
        }
        Type? arrayType = null;
        var success = TrySetArrayType(candidateElementTypes.Item1) || TrySetArrayType(candidateElementTypes.Item2);
        if (!success)
        {
            return false;
        }
        Debug.Assert(arrayType is not null);
        targetType = arrayType;
        return true;

        bool TrySetArrayType(Type? elementType)
        {
            if (elementType is null)
            {
                return false;
            }

            arrayType = elementType.MakeArrayType();
            return declaredType.IsAssignableFrom(arrayType);
        }
    }

    public static bool TryGetTypeKind(Type targetType, out TypeKind targetTypeKind)
    {
        if (!targetType.IsArray || targetType.GetArrayRank() > 1)
        {
            // Variable bound arrays are not supported yet.
            targetTypeKind = default;
            return false;
        }

        if (targetType == typeof(byte[]))
        {
            targetTypeKind = TypeKind.Int8Array;
        }
        else if (targetType == typeof(sbyte[]))
        {
            targetTypeKind = TypeKind.Int8Array;
        }
        else if (targetType == typeof(short[]))
        {
            targetTypeKind = TypeKind.Int16Array;
        }
        else if (targetType == typeof(ushort[]))
        {
            targetTypeKind = TypeKind.Int16Array;
        }
        else if (targetType == typeof(int[]))
        {
            targetTypeKind = TypeKind.Int32Array;
        }
        else if (targetType == typeof(uint[]))
        {
            targetTypeKind = TypeKind.Int32Array;
        }
        else if (targetType == typeof(long[]))
        {
            targetTypeKind = TypeKind.Int64Array;
        }
        else if (targetType == typeof(ulong[]))
        {
            targetTypeKind = TypeKind.Int64Array;
        }
        else if (targetType == typeof(float[]))
        {
            targetTypeKind = TypeKind.Float32Array;
        }
        else if (targetType == typeof(double[]))
        {
            targetTypeKind = TypeKind.Float64Array;
        }
        else if (targetType == typeof(bool[]))
        {
            targetTypeKind = TypeKind.BoolArray;
        }
        else
        {
            var elementType = targetType.GetElementType();
            if (elementType is { IsArray: true })
            {
                while (elementType is { IsArray: true })
                {
                    elementType = elementType.GetElementType();
                }

                if (elementType is { IsPrimitive: true })
                {
                    switch (Type.GetTypeCode(elementType))
                    {
                        case TypeCode.Byte:
                        case TypeCode.SByte:
                        case TypeCode.Int16:
                        case TypeCode.UInt16:
                        case TypeCode.Int32:
                        case TypeCode.UInt32:
                        case TypeCode.Int64:
                        case TypeCode.UInt64:
                        case TypeCode.Single:
                        case TypeCode.Double:
                        case TypeCode.Boolean:
                            targetTypeKind = TypeKind.Array;
                            return true;
                    }
                }
            }

            targetTypeKind = default;
            return false;
        }

        return true;
    }

    [Pure]
    private static bool TryGetElementType(Type targetType, [NotNullWhen(true)] out Type? candidateElementTypes)
    {
        if (!targetType.IsArray)
        {
            candidateElementTypes = null;
            return false;
        }

        candidateElementTypes = targetType.GetElementType();
        return candidateElementTypes is { IsGenericParameter: false };
    }

    [Pure]
    private static bool TryGetElementType(TypeKind typeKind, out (Type?, Type?) candidateElementTypes)
    {
        // TODO: Add support for TypeKind.Array
        candidateElementTypes = typeKind switch
        {
            TypeKind.Int8Array => (typeof(byte), typeof(sbyte)),
            TypeKind.Int16Array => (typeof(short), typeof(ushort)),
            TypeKind.Int32Array => (typeof(int), typeof(uint)),
            TypeKind.Int64Array => (typeof(long), typeof(ulong)),
#if NET8_0_OR_GREATER
            TypeKind.Float16Array => (typeof(Half), null),
#endif
            TypeKind.Float32Array => (typeof(float), null),
            TypeKind.Float64Array => (typeof(double), null),
            TypeKind.BoolArray => (typeof(bool), null),
            _ => default,
        };
        return candidateElementTypes is not (null, null);
    }

    public static bool TryGetSerializerFactory(
        TypeRegistry registry,
        Type targetType,
        [NotNullWhen(true)] out Func<ISerializer>? serializerFactory
    )
    {
        if (!TryGetElementType(targetType, out var elementType))
        {
            serializerFactory = null;
            return false;
        }

        var underlyingType = Nullable.GetUnderlyingType(elementType);
        Func<TypeRegistration?, ISerializer> createMethod;
        if (underlyingType is null)
        {
            createMethod =
                (Func<TypeRegistration?, ISerializer>)
                    CreateArraySerializerMethod
                        .MakeGenericMethod(elementType)
                        .CreateDelegate(typeof(Func<TypeRegistration?, ISerializer>));
        }
        else
        {
            elementType = underlyingType;
            createMethod =
                (Func<TypeRegistration?, ISerializer>)
                    CreateNullableArraySerializerMethod
                        .MakeGenericMethod(elementType)
                        .CreateDelegate(typeof(Func<TypeRegistration?, ISerializer>));
        }

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

    private static ISerializer CreateArraySerializer<TElement>(TypeRegistration? elementRegistration)
        where TElement : notnull
    {
        return new ArraySerializer<TElement>(elementRegistration);
    }

    private static ISerializer CreateNullableArraySerializer<TElement>(TypeRegistration? elementRegistration)
        where TElement : struct
    {
        return new NullableArraySerializer<TElement>(elementRegistration);
    }

    private static bool TryGetDeserializerFactoryCommon(
        TypeRegistry registry,
        Type elementType,
        [NotNullWhen(true)] out Func<IDeserializer>? deserializerFactory
    )
    {
        var underlyingType = Nullable.GetUnderlyingType(elementType);
        Func<TypeRegistration?, IDeserializer> createMethod;
        if (underlyingType is null)
        {
            createMethod =
                (Func<TypeRegistration?, IDeserializer>)
                    CreateArrayDeserializerMethod
                        .MakeGenericMethod(elementType)
                        .CreateDelegate(typeof(Func<TypeRegistration?, IDeserializer>));
        }
        else
        {
            elementType = underlyingType;
            createMethod =
                (Func<TypeRegistration?, IDeserializer>)
                    CreateNullableArrayDeserializerMethod
                        .MakeGenericMethod(elementType)
                        .CreateDelegate(typeof(Func<TypeRegistration?, IDeserializer>));
        }

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
        if (!TryGetElementType(targetType, out var elementType))
        {
            deserializerFactory = null;
            return false;
        }

        return TryGetDeserializerFactoryCommon(registry, elementType, out deserializerFactory);
    }

    private static IDeserializer CreateArrayDeserializer<TElement>(TypeRegistration? elementRegistration)
        where TElement : notnull
    {
        return new ArrayDeserializer<TElement>(elementRegistration);
    }

    private static IDeserializer CreateNullableArrayDeserializer<TElement>(TypeRegistration? elementRegistration)
        where TElement : struct
    {
        return new NullableArrayDeserializer<TElement>(elementRegistration);
    }
}
