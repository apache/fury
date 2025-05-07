using System;
using System.Diagnostics;
using System.IO.Pipelines;
using Fury.Collections;
using Fury.Meta;
using Fury.Serialization;
using Fury.Serialization.Meta;
using JetBrains.Annotations;

namespace Fury.Context;

public sealed class SerializationWriter : IDisposable
{
    private sealed class Frame
    {
        public bool NeedNotifyWriteValueCompleted;
        public TypeRegistration? Registration;
        public ISerializer? Serializer;

        public void Reset()
        {
            Debug.Assert(Registration is not null || Serializer is null);
            if (Registration is not null && Serializer is not null)
            {
                Registration.ReturnSerializer(Serializer);
            }
            NeedNotifyWriteValueCompleted = false;
            Registration = null;
            Serializer = null;
        }
    }

    public SerializationConfig Config { get; private set; } = SerializationConfig.Default;
    private readonly BatchWriter _innerWriter = new();

    public TypeRegistry TypeRegistry { get; }

    private readonly HeaderSerializer _headerSerializer = new();
    private readonly ReferenceMetaSerializer _referenceMetaSerializer = new();
    private readonly TypeMetaSerializer _typeMetaSerializer;

    internal AutoIncrementIdDictionary<MetaString> MetaStringContext { get; } = new();
    private readonly FrameStack<Frame> _frameStack = new();

    public SerializationWriterRef ByrefWriter => new(this, _innerWriter);

    internal SerializationWriter(TypeRegistry registry)
    {
        TypeRegistry = registry;
        _typeMetaSerializer = CreateTypeMetaSerializer();
        _typeMetaSerializer.Initialize(MetaStringContext);
    }

    internal void Reset()
    {
        _innerWriter.Reset();
        _headerSerializer.Reset();
        _referenceMetaSerializer.Reset();
        _typeMetaSerializer.Reset();
        foreach (var frame in _frameStack.Frames)
        {
            frame.Reset();
        }
        _frameStack.Reset();
    }

    private void ResetCurrent()
    {
        _referenceMetaSerializer.ResetCurrent();
        _typeMetaSerializer.Reset();
    }

    internal void Initialize(PipeWriter pipeWriter, SerializationConfig config)
    {
        Config = config;
        _innerWriter.Initialize(pipeWriter);
        _referenceMetaSerializer.Initialize(config.ReferenceTracking);
    }

    public void Dispose()
    {
        _innerWriter.Dispose();
        TypeRegistry.Dispose();
    }

    internal TypeMetaSerializer CreateTypeMetaSerializer() => new();

    private void OnCurrentSerializationCompleted(bool isSuccess)
    {
        if (isSuccess)
        {
            ResetCurrent();
            _frameStack.PopFrame().Reset();
        }
        else
        {
            _frameStack.MoveLast();
        }
    }

    internal bool WriteHeader(bool rootObjectIsNull)
    {
        var writerRef = ByrefWriter;
        return _headerSerializer.Write(ref writerRef, rootObjectIsNull);
    }

    [MustUseReturnValue]
    public bool Serialize<TTarget>(in TTarget? value, TypeRegistration? registrationHint = null)
    {
        _frameStack.MoveNext();

        var isSuccess = false;
        try
        {
            var writer = ByrefWriter;
            isSuccess = WriteRefMeta(ref writer, in value, out var needWriteValue);
            if (!isSuccess)
            {
                return false;
            }
            if (!needWriteValue)
            {
                return true;
            }
            Debug.Assert(value is not null);
            isSuccess = WriteTypeMeta(ref writer, in value, registrationHint);
            if (!isSuccess)
            {
                return false;
            }
            isSuccess = WriteValue(ref writer, in value);
        }
        finally
        {
            OnCurrentSerializationCompleted(isSuccess);
        }
        return isSuccess;
    }

    [MustUseReturnValue]
    public bool Serialize<TTarget>(in TTarget? value, TypeRegistration? registrationHint = null)
        where TTarget : struct
    {
        _frameStack.MoveNext();

        var isSuccess = false;
        try
        {
            var writer = ByrefWriter;
            isSuccess = WriteRefMeta(ref writer, in value, out var needWriteValue);
            if (!isSuccess)
            {
                return false;
            }
            if (!needWriteValue)
            {
                return true;
            }
            Debug.Assert(value is not null);
            isSuccess = WriteTypeMeta(ref writer, in value, registrationHint);
            if (!isSuccess)
            {
                return false;
            }
#if NET6_0_OR_GREATER
            ref readonly var valueRef = ref NullableHelper.GetValueRefOrDefaultRef(in value);
            isSuccess = WriteValue(ref writer, in valueRef);
#else
            isSuccess = WriteValue(ref writer, value.Value);
#endif
        }
        finally
        {
            OnCurrentSerializationCompleted(isSuccess);
        }
        return isSuccess;
    }

    private bool WriteRefMeta<TTarget>(ref SerializationWriterRef writerRef, in TTarget? value, out bool needWriteValue)
    {
        if (!_frameStack.IsCurrentTheLastFrame)
        {
            // The write calls which write RefFlag.Null or RefFlag.Ref do not need to write value,
            // so that they will not produce new write calls and can only be stored in the last
            // frame of the stack.
            // If the current frame is not the last frame, it must be the RefFlag.RefValue or
            // RefFlag.NotNullValue case, which means that the value is not null, and we need to write it.
            needWriteValue = true;
            return true;
        }

        var writeMetaSuccess = _referenceMetaSerializer.Write(ref writerRef, in value, out var writtenFlag);
        if (!writeMetaSuccess)
        {
            needWriteValue = false;
            return false;
        }

        needWriteValue = writtenFlag is RefFlag.RefValue or RefFlag.NotNullValue;
        _frameStack.CurrentFrame.NeedNotifyWriteValueCompleted = writtenFlag is RefFlag.RefValue;
        return true;
    }

    private bool WriteTypeMeta<TTarget>(
        ref SerializationWriterRef writerRef,
        in TTarget value,
        TypeRegistration? registrationHint
    )
    {
        if (!_frameStack.IsCurrentTheLastFrame)
        {
            return true;
        }

        var desiredType = value!.GetType();
        if (registrationHint?.TargetType != desiredType)
        {
            Debug.WriteLine("Type registration hint does not match the actual type.");
            registrationHint = null;
        }

        var currentFrame = _frameStack.CurrentFrame;
        currentFrame.Registration = registrationHint ?? TypeRegistry.GetTypeRegistration(desiredType);
        Debug.Assert(currentFrame.Registration.TargetType == desiredType);
        return _typeMetaSerializer.Write(ref writerRef, currentFrame.Registration);
    }

    [MustUseReturnValue]
    private bool WriteValue<TTarget>(ref SerializationWriterRef writerRef, in TTarget value)
    {
        var currentFrame = _frameStack.CurrentFrame;
        Debug.Assert(currentFrame.Registration is not null);
        switch (currentFrame.Registration!.TypeKind) {
            // TODO: Fast path for primitive types, string, string array and primitive arrays
        }

        currentFrame.Serializer ??= currentFrame.Registration.RentSerializer();

        bool success;

        var serializer = currentFrame.Serializer;
        if (serializer is ISerializer<TTarget> typedSerializer)
        {
            success = typedSerializer.Serialize(this, in value);
        }
        else
        {
            success = serializer.Serialize(this, value!);
        }

        if (success && currentFrame.NeedNotifyWriteValueCompleted)
        {
            _referenceMetaSerializer.HandleWriteValueCompleted(in value);
        }

        return success;
    }

    #region Write Methods

    [MustUseReturnValue]
    internal bool WriteUnmanaged<T>(T value)
        where T : unmanaged => ByrefWriter.WriteUnmanaged(value);

    /// <inheritdoc cref="SerializationWriterRef.Write(ReadOnlySpan{byte})"/>
    [MustUseReturnValue]
    public int Write(scoped ReadOnlySpan<byte> bytes) => ByrefWriter.Write(bytes);

    /// <inheritdoc cref="SerializationWriterRef.Write(byte)"/>
    [MustUseReturnValue]
    public bool Write(byte value) => ByrefWriter.Write(value);

    /// <inheritdoc cref="SerializationWriterRef.Write(sbyte)"/>
    [MustUseReturnValue]
    public bool Write(sbyte value) => ByrefWriter.Write(value);

    /// <inheritdoc cref="SerializationWriterRef.Write(sbyte)"/>
    [MustUseReturnValue]
    public bool Write(ushort value) => ByrefWriter.Write(value);

    /// <inheritdoc cref="SerializationWriterRef.Write(sbyte)"/>
    [MustUseReturnValue]
    public bool Write(short value) => ByrefWriter.Write(value);

    /// <inheritdoc cref="SerializationWriterRef.Write(sbyte)"/>
    [MustUseReturnValue]
    public bool Write(uint value) => ByrefWriter.Write(value);

    /// <inheritdoc cref="SerializationWriterRef.Write(sbyte)"/>
    [MustUseReturnValue]
    public bool Write(int value) => ByrefWriter.Write(value);

    /// <inheritdoc cref="SerializationWriterRef.Write(sbyte)"/>
    [MustUseReturnValue]
    public bool Write(ulong value) => ByrefWriter.Write(value);

    /// <inheritdoc cref="SerializationWriterRef.Write(sbyte)"/>
    [MustUseReturnValue]
    public bool Write(long value) => ByrefWriter.Write(value);

    /// <inheritdoc cref="SerializationWriterRef.Write(sbyte)"/>
    [MustUseReturnValue]
    public bool Write(float value) => ByrefWriter.Write(value);

    /// <inheritdoc cref="SerializationWriterRef.Write(sbyte)"/>
    [MustUseReturnValue]
    public bool Write(double value) => ByrefWriter.Write(value);

    /// <inheritdoc cref="SerializationWriterRef.Write(sbyte)"/>
    [MustUseReturnValue]
    public bool Write(bool value) => ByrefWriter.Write(value);

    /// <inheritdoc cref="SerializationWriterRef.Write7BitEncodedInt"/>
    [MustUseReturnValue]
    public bool Write7BitEncodedInt(int value) => ByrefWriter.Write7BitEncodedInt(value);

    /// <inheritdoc cref="SerializationWriterRef.Write7BitEncodedUint"/>
    [MustUseReturnValue]
    public bool Write7BitEncodedUint(uint value) => ByrefWriter.Write7BitEncodedUint(value);

    /// <inheritdoc cref="SerializationWriterRef.Write7BitEncodedLong"/>
    [MustUseReturnValue]
    public bool Write7BitEncodedLong(long value) => ByrefWriter.Write7BitEncodedLong(value);

    /// <inheritdoc cref="SerializationWriterRef.Write7BitEncodedUlong"/>
    [MustUseReturnValue]
    public bool Write7BitEncodedUlong(ulong value) => ByrefWriter.Write7BitEncodedUlong(value);
    #endregion
}
