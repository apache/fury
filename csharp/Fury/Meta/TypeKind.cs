namespace Fury.Meta;

public enum TypeKind : byte
{
    /// <inheritdoc cref="InternalTypeKind.Bool"/>
    Bool = 1,

    /// <inheritdoc cref="InternalTypeKind.Int8"/>
    Int8 = 2,

    /// <inheritdoc cref="InternalTypeKind.Int16"/>
    Int16 = 3,

    /// <inheritdoc cref="InternalTypeKind.Int32"/>
    Int32 = 4,

    /// <inheritdoc cref="InternalTypeKind.VarInt32"/>
    VarInt32 = 5,

    /// <inheritdoc cref="InternalTypeKind.Int64"/>
    Int64 = 6,

    /// <inheritdoc cref="InternalTypeKind.VarInt64"/>
    VarInt64 = 7,

    /// <inheritdoc cref="InternalTypeKind.SliInt64"/>
    SliInt64 = 8,

    /// <inheritdoc cref="InternalTypeKind.Float16"/>
    Float16 = 9,

    /// <inheritdoc cref="InternalTypeKind.Float32"/>
    Float32 = 10,

    /// <inheritdoc cref="InternalTypeKind.Float64"/>
    Float64 = 11,

    /// <inheritdoc cref="InternalTypeKind.String"/>
    String = 12,

    /// <inheritdoc cref="InternalTypeKind.List"/>
    List = 27,

    /// <inheritdoc cref="InternalTypeKind.Set"/>
    Set = 28,

    /// <inheritdoc cref="InternalTypeKind.Map"/>
    Map = 29,

    /// <inheritdoc cref="InternalTypeKind.Duration"/>
    Duration = 30,

    /// <inheritdoc cref="InternalTypeKind.Timestamp"/>
    Timestamp = 31,

    /// <inheritdoc cref="InternalTypeKind.LocalDate"/>
    LocalDate = 32,

    /// <inheritdoc cref="InternalTypeKind.Decimal"/>
    Decimal = 33,

    /// <inheritdoc cref="InternalTypeKind.Binary"/>
    Binary = 34,

    /// <inheritdoc cref="InternalTypeKind.Array"/>
    Array = 35,

    /// <inheritdoc cref="InternalTypeKind.BoolArray"/>
    BoolArray = 36,

    /// <inheritdoc cref="InternalTypeKind.Int8Array"/>
    Int8Array = 37,

    /// <inheritdoc cref="InternalTypeKind.Int16Array"/>
    Int16Array = 38,

    /// <inheritdoc cref="InternalTypeKind.Int32Array"/>
    Int32Array = 39,

    /// <inheritdoc cref="InternalTypeKind.Int64Array"/>
    Int64Array = 40,

    /// <inheritdoc cref="InternalTypeKind.Float16Array"/>
    Float16Array = 41,

    /// <inheritdoc cref="InternalTypeKind.Float32Array"/>
    Float32Array = 42,

    /// <inheritdoc cref="InternalTypeKind.Float64Array"/>
    Float64Array = 43,

    /// <inheritdoc cref="InternalTypeKind.ArrowRecordBatch"/>
    ArrowRecordBatch = 44,

    /// <inheritdoc cref="InternalTypeKind.ArrowTable"/>
    ArrowTable = 45,
}

internal static class TypeKindExtensions
{
    public static InternalTypeKind ToInternal(this TypeKind typeKind)
    {
        return (InternalTypeKind)(byte)typeKind;
    }
}
