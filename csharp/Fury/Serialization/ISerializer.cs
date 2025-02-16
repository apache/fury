using System;
using System.Threading;
using System.Threading.Tasks;
using Fury.Context;

namespace Fury.Serialization;

// This interface is used to support polymorphism.
public interface ISerializer : IDisposable
{
    bool Write(SerializationContext context, object value);

    void Reset();
}

public interface IDeserializer : IDisposable
{
    // It is very common that the data is not all available at once, so we need to read it asynchronously.

    /// <summary>
    /// Try to create an instance of the object which will be deserialized.
    /// </summary>
    /// <param name="context">
    ///     The context which contains the state of the deserialization process.
    /// </param>
    /// <param name="boxedInstance"></param>
    /// <returns>
    /// <see langword="true"/> if the instance is created completely; otherwise, <see langword="false"/>.
    /// </returns>
    /// <seealso cref="CreateInstanceAsync"/>
    bool CreateInstance(DeserializationContext context, ref Box boxedInstance);

    /// <summary>
    /// Try to read the serialized data and populate the given object.
    /// </summary>
    /// <param name="context">
    ///     The context which contains the state of the deserialization process.
    /// </param>
    /// <param name="boxedInstance"></param>
    /// <returns>
    /// <see langword="true"/> if the object is deserialized completely; otherwise, <see langword="false"/>.
    /// </returns>
    /// <seealso cref="FillInstanceAsync"/>
    bool FillInstance(DeserializationContext context, Box boxedInstance);

    /// <summary>
    /// Create an instance of the object which will be deserialized.
    /// </summary>
    /// <param name="context">
    ///     The context which contains the state of the deserialization process.
    /// </param>
    /// <param name="cancellationToken">
    ///     The token to monitor for cancellation requests.
    /// </param>
    /// <returns>
    /// An instance of the object which is not deserialized yet.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This method is used to solve the circular reference problem.
    /// When deserializing an object which may be referenced by itself or its child objects,
    /// we need to create an instance before reading its fields.
    /// So that we can reference it before it is fully deserialized.
    /// </para>
    /// <para>
    /// You can read some necessary data from the context to create the instance, e.g. the length of an array.
    /// </para>
    /// <para>
    /// If the object certainly does not have circular references, you can return a fully deserialized object
    /// and keep the <see cref="FillInstanceAsync"/> method empty.<br/>
    /// Be careful that the default implementation of <see cref="IDeserializer{TTarget}.CreateAndFillInstanceAsync"/>
    /// in <see cref="AbstractDeserializer{TTarget}"/> use this method to create an instance.<br/>
    /// If you want to do all the deserialization here, it is recommended to override
    /// <see cref="IDeserializer{TTarget}.CreateAndFillInstanceAsync"/> and call it in this method.
    /// </para>
    /// </remarks>
    /// <seealso cref="CreateInstance"/>
    ValueTask<Box> CreateInstanceAsync(DeserializationContext context, CancellationToken cancellationToken = default);

    /// <summary>
    /// Read the serialized data and populate the given object.
    /// </summary>
    /// <param name="context">
    ///     The context which contains the state of the deserialization process.
    /// </param>
    /// <param name="instance">
    ///     The object which is not deserialized yet. It is created by <see cref="CreateInstanceAsync"/>.
    /// </param>
    /// <param name="cancellationToken">
    ///     The token to monitor for cancellation requests.
    /// </param>
    /// <seealso cref="FillInstance"/>
    ValueTask FillInstanceAsync(
        DeserializationContext context,
        Box instance,
        CancellationToken cancellationToken = default
    );

    void Reset();
}

public interface ISerializer<TTarget> : ISerializer
    where TTarget : notnull
{
    bool Write(SerializationContext context, in TTarget value);
}

public interface IDeserializer<TTarget> : IDeserializer
    where TTarget : notnull
{
    bool CreateAndFillInstance(DeserializationContext context, ref TTarget? instance);

    /// <summary>
    /// Read the serialized data and create an instance of the object.
    /// </summary>
    /// <param name="context">
    /// The context which contains the state of the deserialization process.
    /// </param>
    /// <param name="cancellationToken">
    /// The token to monitor for cancellation requests.
    /// </param>
    /// <returns>
    /// An instance of the object which is deserialized.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This is a faster way to deserialize an object without creating an instance first,
    /// which means it is not suitable for objects may be referenced.
    /// </para>
    /// <para>
    /// This method can be used to avoid boxing when deserializing a value type.
    /// </para>
    /// </remarks>
    /// <seealso cref="CreateAndFillInstance"/>
    ValueTask<TTarget> CreateAndFillInstanceAsync(
        DeserializationContext context,
        CancellationToken cancellationToken = default
    );
}
