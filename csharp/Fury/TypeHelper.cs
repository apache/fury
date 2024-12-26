namespace Fury;

internal static class TypeHelper<T>
{
    public static readonly bool IsSealed = typeof(T).IsSealed;
    public static readonly bool IsValueType = typeof(T).IsValueType;


}
