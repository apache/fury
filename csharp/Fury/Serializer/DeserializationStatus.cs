namespace Fury.Serializer;

public enum DeserializationStatus
{
    InstanceNotCreated,
    InstanceCreated,
    Completed,
}

public static class DeserializationStatusExtensions
{
    public static bool HasCreatedInstance(this DeserializationStatus status)
    {
        return status switch
        {
            DeserializationStatus.InstanceCreated or DeserializationStatus.Completed => true,
            _ => false,
        };
    }
}
