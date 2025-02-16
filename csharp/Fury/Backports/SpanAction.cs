#if !NET8_0_OR_GREATER
// ReSharper disable once CheckNamespace
namespace System.Buffers;
internal delegate void SpanAction<T, in TArg>(Span<T> span, TArg arg);
#endif
