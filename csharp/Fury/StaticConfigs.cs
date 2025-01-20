namespace Fury;

internal static class StaticConfigs
{
    public const int StackAllocLimit = 256;
    public const int CharsStackAllocLimit = StackAllocLimit / sizeof(char);

    public const int BuiltInListDefaultCapacity = 16;
}
