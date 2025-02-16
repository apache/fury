using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Fury.Context;
using Fury.Meta;

namespace Fury.Serialization;

internal static class EnumSerializationProvider
{
    private static MethodInfo CreateEnumSerializerMethod { get; } =
        typeof(EnumSerializationProvider).GetMethod(
            nameof(CreateEnumSerializer),
            BindingFlags.NonPublic | BindingFlags.Static
        )!;

    private static MethodInfo CreateEnumDeserializerMethod { get; } =
        typeof(EnumSerializationProvider).GetMethod(
            nameof(CreateEnumDeserializer),
            BindingFlags.NonPublic | BindingFlags.Static
        )!;

    private static bool CheckCanHandle(Type targetType)
    {
        return targetType.IsEnum;
    }

    public static bool TryGetSerializerFactory(
        TypeRegistry registry,
        Type targetType,
        [NotNullWhen(true)] out Func<ISerializer>? serializerFactory
    )
    {
        if (!CheckCanHandle(targetType))
        {
            serializerFactory = null;
            return false;
        }

        var method = CreateEnumSerializerMethod.MakeGenericMethod(targetType);
        serializerFactory = (Func<ISerializer>)method.CreateDelegate(typeof(Func<ISerializer>));
        return true;
    }

    private static ISerializer CreateEnumSerializer<TEnum>()
        where TEnum : struct
    {
        return new EnumSerializer<TEnum>();
    }

    public static bool TryGetDeserializerFactory(
        TypeRegistry registry,
        Type targetType,
        [NotNullWhen(true)] out Func<IDeserializer>? deserializerFactory
    )
    {
        if (!CheckCanHandle(targetType))
        {
            deserializerFactory = null;
            return false;
        }

        var method = CreateEnumDeserializerMethod.MakeGenericMethod(targetType);
        deserializerFactory = (Func<IDeserializer>)method.CreateDelegate(typeof(Func<IDeserializer>));
        return true;
    }

    private static IDeserializer CreateEnumDeserializer<TEnum>()
        where TEnum : struct
    {
        return new EnumDeserializer<TEnum>();
    }
}
