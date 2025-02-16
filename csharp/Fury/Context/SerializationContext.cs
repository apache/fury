using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using Fury.Serialization;
using Fury.Serialization.Meta;
using JetBrains.Annotations;

namespace Fury.Context;

public sealed class SerializationContext
{
    public Fury Fury { get; }
    private readonly BatchWriter.Context _writerContext = new();

    private readonly Stack<ISerializer> _uncompletedSerializers = new();

    private ReferenceMetaSerializer _referenceMetaSerializer;
    private TypeMetaSerializer _typeMetaSerializer = new();

    internal SerializationContext(Fury fury, PipeWriter writer)
    {
        Fury = fury;
        _writerContext.Initialize(writer);
        _referenceMetaSerializer = new ReferenceMetaSerializer(fury.Config.ReferenceTracking);
    }

    [PublicAPI]
    public BatchWriter GetWriter() => new(_writerContext);

    [MustUseReturnValue]
    public bool Write<TTarget>(in TTarget? value, TypeRegistration? registrationHint = null)
        where TTarget : notnull
    {
        var writer = GetWriter();
        var completed = _referenceMetaSerializer.Write(ref writer, value, out var needWriteValue);
        if (!needWriteValue)
        {
            return completed;
        }

        try
        {
            if (TypeHelper<TTarget>.IsValueType)
            {
                completed = completed && DoWrite(ref writer, in value!, registrationHint);
            }
            else
            {
                completed = completed && DoWrite(ref writer, value!, registrationHint);
            }
        }
        catch (Exception)
        {
            completed = false;
            throw;
        }
        finally
        {
            if (completed)
            {
                _referenceMetaSerializer.Reset(false);
            }
        }

        return completed;
    }

    [MustUseReturnValue]
    public bool Write<TTarget>(in TTarget? value, TypeRegistration? registrationHint = null)
        where TTarget : struct
    {
        var writer = GetWriter();
        var completed = _referenceMetaSerializer.Write(ref writer, value, out var needWriteValue);
        if (!needWriteValue)
        {
            return completed;
        }

        completed = completed && DoWrite(ref writer, value!.Value, registrationHint);

        try
        {
            completed = completed && DoWrite(ref writer, value!.Value, registrationHint);
        }
        catch (Exception)
        {
            completed = false;
            throw;
        }
        finally
        {
            if (completed)
            {
                _referenceMetaSerializer.Reset(false);
            }
        }
        return completed;
    }

    [MustUseReturnValue]
    private bool DoWrite<TTarget>(ref BatchWriter writer, in TTarget value, TypeRegistration? registrationHint)
        where TTarget : notnull
    {
        var completed = true;
        var desiredType = value.GetType();
        if (registrationHint?.GetType() != desiredType)
        {
            registrationHint = null;
        }

        registrationHint ??= Fury.TypeRegistry.GetTypeRegistration(desiredType);
        completed = completed && _typeMetaSerializer.Write(ref writer, registrationHint);

        switch (registrationHint.TypeKind) {
            // TODO: Fast path for primitive types, string, string array and primitive arrays
        }

        if (completed)
        {
            ISerializer serializer;
            if (_uncompletedSerializers.Count == 0)
            {
                // No uncompleted serializers
                if (!registrationHint.TryGetSerializer(out serializer!))
                {
                    ThrowHelper.ThrowBadSerializationInputException_NoSerializerFactoryProvider(
                        registrationHint.TargetType
                    );
                }
            }
            else
            {
                // resume uncompleted serializer
                serializer = _uncompletedSerializers.Pop();
            }

            try
            {
                if (serializer is ISerializer<TTarget> typedSerializer)
                {
                    completed = completed && typedSerializer.Write(this, in value);
                }
                else
                {
                    completed = completed && serializer.Write(this, value);
                }
            }
            catch (Exception)
            {
                completed = false;
                throw;
            }
            finally
            {
                if (!completed)
                {
                    _uncompletedSerializers.Push(serializer);
                }
            }

            if (completed)
            {
                _typeMetaSerializer.Reset(false);
            }
        }
        return completed;
    }

    internal void Reset()
    {
        _writerContext.Reset();
        _referenceMetaSerializer.Reset(true);
        _typeMetaSerializer.Reset(true);
        _uncompletedSerializers.Clear();
    }
}
