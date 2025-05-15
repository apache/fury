using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fury.Buffers;

#if NET5_0_OR_GREATER
internal sealed class ListMemoryManager<TElement> : MemoryManager<TElement>
{
    public List<TElement>? List { get; set; }
    private GCHandle _handle;

    protected override void Dispose(bool disposing)
    {
        List = null;
    }

    public override Span<TElement> GetSpan()
    {
        return CollectionsMarshal.AsSpan(List);
    }

    public override unsafe MemoryHandle Pin(int elementIndex = 0)
    {
        ThrowIfListIsNull();

        _handle = GCHandle.Alloc(List, GCHandleType.Pinned);
        var p = Unsafe.AsPointer(ref GetSpan().GetPinnableReference());
        return new MemoryHandle(p, _handle);
    }

    public override void Unpin()
    {
        ThrowIfListIsNull();

        _handle.Free();
    }

    private void ThrowIfListIsNull()
    {
        if (List is null)
        {
            ThrowHelper.ThrowInvalidOperationException();
        }
    }
}
#endif
