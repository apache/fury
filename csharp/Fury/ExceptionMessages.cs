using System;

namespace Fury;

internal static class ExceptionMessages
{
    public static string SerializerNotFound(Type type) => $"No serializer found for type '{type.FullName}'.";

    public static string DeserializerNotFound(Type type) => $"No deserializer found for type '{type.FullName}'.";

    public static string UnreferenceableType(Type type) => $"Type '{type.FullName}' is not referenceable.";

    public static string TypeInfoNotFound(TypeId id) => $"No type info found for type id '{id}'.";

    public static string UnregisteredType(Type type) => $"Type '{type.FullName}' is not registered.";

    public static string ReferencedObjectNotFound(RefId refId) => $"Referenced object not found for ref id '{refId}'.";

    public static string FailedToGetElementType(Type collectionType) =>
        $"Failed to get element type for collection type '{collectionType.FullName}'.";

    public static string ReferenceTypeExpected(Type type) => $"Reference type expected, but got '{type.FullName}'.";

    public static string NotNullValueExpected(Type type) =>
        $"Not null value expected, but got null for type '{type.FullName}'.";

    public static string RefIdInvalidOrOutOfRange(RefId refId) => $"Ref id '{refId}' is invalid or out of range.";

    public static string InsufficientData() => "Insufficient data.";

    public static string VarInt32Overflow() => "VarInt32 overflow.";

    public static string VarInt32Truncated() => "VarInt32 truncated.";

    public static string CircularDependencyDetected() => "Circular dependency detected.";

    public static string NotSupportedSerializer(Type type) =>
        $"This serializer for type '{type.FullName}' is not supported yet.";

    public static string NotSupportedDeserializer(Type type) =>
        $"This deserializer for type '{type.FullName}' is not supported yet.";

    public static string InvalidMagicNumber() => "Invalid magic number.";

    public static string NotCrossLanguage() => "Not cross language.";

    public static string NotLittleEndian() => "Not little endian.";
}
