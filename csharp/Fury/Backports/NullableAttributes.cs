#if !NET8_0_OR_GREATER
// ReSharper disable once CheckNamespace
namespace System.Diagnostics.CodeAnalysis;

/// <summary>Specifies that when a method returns <see cref="P:System.Diagnostics.CodeAnalysis.NotNullWhenAttribute.ReturnValue" />, the parameter will not be <see langword="null" /> even if the corresponding type allows it.</summary>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class NotNullWhenAttribute : Attribute
{
    /// <summary>Initializes the attribute with the specified return value condition.</summary>
    /// <param name="returnValue">The return value condition. If the method returns this value, the associated parameter will not be <see langword="null" />.</param>
    public NotNullWhenAttribute(bool returnValue) => this.ReturnValue = returnValue;

    /// <summary>Gets the return value condition.</summary>
    /// <returns>The return value condition. If the method returns this value, the associated parameter will not be <see langword="null" />.</returns>
    public bool ReturnValue { get; }
}

/// <summary>Specifies that the method will not return if the associated <see cref="T:System.Boolean" /> parameter is passed the specified value.</summary>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class DoesNotReturnIfAttribute : Attribute
{
    /// <summary>Initializes a new instance of the <see cref="T:System.Diagnostics.CodeAnalysis.DoesNotReturnIfAttribute" /> class with the specified parameter value.</summary>
    /// <param name="parameterValue">The condition parameter value. Code after the method is considered unreachable by diagnostics if the argument to the associated parameter matches this value.</param>
    public DoesNotReturnIfAttribute(bool parameterValue) => this.ParameterValue = parameterValue;

    /// <summary>Gets the condition parameter value.</summary>
    /// <returns>The condition parameter value. Code after the method is considered unreachable by diagnostics if the argument to the associated parameter matches this value.</returns>
    public bool ParameterValue { get; }
}
#endif
