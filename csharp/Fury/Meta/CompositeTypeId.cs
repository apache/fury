namespace Fury.Meta;

internal readonly record struct CompositeTypeKind(InternalTypeKind TypeKind, int TypeId)
{
    private const int TypeKindBits = 8;
    private const uint TypeKindMask = (1u << TypeKindBits) - 1;

    public static CompositeTypeKind FromUint(uint value)
    {
        var typeKind = (InternalTypeKind)(value & TypeKindMask);
        var extId = (int)(value >>> TypeKindBits);
        return new CompositeTypeKind(typeKind, extId);
    }

    public uint ToUint()
    {
        return (uint)TypeKind | ((uint)TypeId << TypeKindBits);
    }
}
