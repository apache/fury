using System.Runtime.CompilerServices;

namespace Fury.Context;

internal readonly struct MemberReference(object? memberMemoryOwner, nint memoryOffsetOrAddress)
{
    public unsafe ref T GetReference<T>()
    {
        var owner = memberMemoryOwner;
        if (owner is null)
        {
            // Address
            return ref Unsafe.AsRef<T>((void*)memoryOffsetOrAddress);
        }

        // Memory offset
        fixed (object* pOwner = &owner)
        {
            var ppOwnerMemory = (void**)pOwner;
            return ref Unsafe.AsRef<T>(ppOwnerMemory + memoryOffsetOrAddress);
        }
    }
}
