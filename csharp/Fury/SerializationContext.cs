using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Fury.Serializer;

namespace Fury;

// BatchWriter is ref struct, so SerializationContext must be ref struct too

public ref struct SerializationContext
{
    public Fury Fury { get; }
    public BatchWriter Writer;
    private RefResolver RefResolver { get; }

    internal SerializationContext(Fury fury, BatchWriter writer, RefResolver refResolver)
    {
        Fury = fury;
        Writer = writer;
        RefResolver = refResolver;
    }

    public bool TryGetSerializer<TValue>([NotNullWhen(true)] out ISerializer? serializer)
    {
        return Fury.TypeResolver.TryGetOrCreateSerializer(typeof(TValue), out serializer);
    }

    public ISerializer GetSerializer<TValue>()
    {
        if (!TryGetSerializer<TValue>(out var serializer))
        {
            ThrowHelper.ThrowSerializerNotFoundException(
                typeof(TValue),
                message: ExceptionMessages.SerializerNotFound(typeof(TValue))
            );
        }
        return serializer;
    }

    public void Write<TValue>(in TValue? value, ReferenceTrackingPolicy referenceable, ISerializer? serializer = null)
        where TValue : notnull
    {
        if (value is null)
        {
            Writer.Write(ReferenceFlag.Null);
            return;
        }

        var declaredType = typeof(TValue);
        if (referenceable == ReferenceTrackingPolicy.Enabled)
        {
            if (declaredType.IsValueType)
            {
                RefResolver.AddRefId();
                Writer.Write(ReferenceFlag.RefValue);
                DoWriteValueType(in value, serializer);
                return;
            }

            var refId = RefResolver.GetOrPushRefId(value, out var processingState);
            if (processingState == RefResolver.ObjectProcessingState.Unprocessed)
            {
                Writer.Write(ReferenceFlag.RefValue);
                DoWriteReferenceType(value, serializer);
                RefResolver.MarkFullyProcessed(refId);
            }
            else
            {
                Writer.Write(ReferenceFlag.Ref);
                Writer.Write7BitEncodedInt(refId.Value);
            }
        }
        else
        {
            if (declaredType.IsValueType)
            {
                Writer.Write(ReferenceFlag.NotNullValue);
                DoWriteValueType(in value, serializer);
                return;
            }

            var refId = RefResolver.GetOrPushRefId(value, out var processingState);
            if (processingState == RefResolver.ObjectProcessingState.PartiallyProcessed)
            {
                // Circular dependency detected
                if (referenceable == ReferenceTrackingPolicy.OnlyCircularDependency)
                {
                    Writer.Write(ReferenceFlag.Ref);
                    Writer.Write7BitEncodedInt(refId.Value);
                }
                else
                {
                    ThrowHelper.ThrowCircularDependencyException(ExceptionMessages.CircularDependencyDetected());
                }
                return;
            }

            // processingState should not be FullyProcessed
            // because we pop the referenceable object after writing it

            var flag = referenceable == ReferenceTrackingPolicy.OnlyCircularDependency
                ? ReferenceFlag.RefValue
                : ReferenceFlag.NotNullValue;
            Writer.Write(flag);
            DoWriteReferenceType(value, serializer);
            RefResolver.PopReferenceableObject();
        }
    }

    public void Write<TValue>(in TValue? value, ReferenceTrackingPolicy referenceable, ISerializer? serializer = null)
        where TValue : struct
    {
        if (value is null)
        {
            Writer.Write(ReferenceFlag.Null);
            return;
        }

        if (referenceable == ReferenceTrackingPolicy.Enabled)
        {
            RefResolver.GetOrPushRefId(value.Value, out _);
            Writer.Write(ReferenceFlag.RefValue);
        }
        else
        {
            Writer.Write(ReferenceFlag.NotNullValue);
        }
#if NET8_0_OR_GREATER
        DoWriteValueType(in Nullable.GetValueRefOrDefaultRef(in value), serializer);
#else
        DoWriteValueType(value.Value, serializer);
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write<TValue>(in TValue? value, ISerializer? serializer = null)
        where TValue : notnull
    {
        Write(value, Fury.Config.ReferenceTracking, serializer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write<TValue>(in TValue? value, ISerializer? serializer = null)
        where TValue : struct
    {
        Write(value, Fury.Config.ReferenceTracking, serializer);
    }

    private void DoWriteValueType<TValue>(in TValue value, ISerializer? serializer)
    where TValue : notnull
    {
        var type = typeof(TValue);
        var typeInfo = GetOrRegisterTypeInfo(type);
        WriteTypeMeta(typeInfo);

        switch (typeInfo.TypeId) {
            // TODO: Fast path for primitive types
        }

        var typedSerializer = (ISerializer<TValue>)(serializer ?? GetPreferredSerializer(type));
        typedSerializer.Write(this, in value);
    }

    private void DoWriteReferenceType(object value, ISerializer? serializer)
    {
        var type = value.GetType();
        var typeInfo = GetOrRegisterTypeInfo(type);
        WriteTypeMeta(typeInfo);

        switch (typeInfo.TypeId) {
            // TODO: Fast path for string, string array and primitive arrays
        }

        serializer ??= GetPreferredSerializer(type);
        serializer.Write(this, value);
    }

    private TypeInfo GetOrRegisterTypeInfo(Type typeOfSerializedObject)
    {
        if (!Fury.TypeResolver.TryGetTypeInfo(typeOfSerializedObject, out var typeInfo))
        {
            ThrowHelper.ThrowUnregisteredTypeException(
                typeOfSerializedObject,
                ExceptionMessages.UnregisteredType(typeOfSerializedObject)
            );
        }

        return typeInfo;
    }

    private void WriteTypeMeta(TypeInfo typeInfo)
    {
        Writer.Write(typeInfo.TypeId);
        switch (typeInfo.TypeId) {
            // TODO: Write package name and class name when new spec is implemented
        }
    }

    private ISerializer GetPreferredSerializer(Type typeOfSerializedObject)
    {
        if (!Fury.TypeResolver.TryGetOrCreateSerializer(typeOfSerializedObject, out var serializer))
        {
            ThrowHelper.ThrowSerializerNotFoundException(
                typeOfSerializedObject,
                message: ExceptionMessages.SerializerNotFound(typeOfSerializedObject)
            );
        }
        return serializer;
    }
}
