using System;

namespace Fury.Meta;

[Flags]
public enum HeaderFlag : byte
{
    NullRootObject = 1,
    LittleEndian = 1 << 1,
    CrossLanguage = 1 << 2,
    OutOfBand = 1 << 3,
}
