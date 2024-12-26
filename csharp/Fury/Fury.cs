using System.IO.Pipelines;

namespace Fury;

public sealed class Fury(Config config)
{
    public Config Config { get; } = config;

    private const short MagicNumber = 0x62D4;

    public TypeResolver TypeResolver { get; } = new(config.SerializerProviders, config.DeserializerProviders);

    private readonly ObjectPool<RefResolver> _refResolverPool = new();

    public void Serialize<T>(PipeWriter writer, in T? value)
        where T : notnull
    {
        var refResolver = _refResolverPool.Rent();
        try
        {
            if (SerializeCommon(new BatchWriter(writer), in value, refResolver, out var context))
            {
                context.Write(in value);
            }
        }
        finally
        {
            _refResolverPool.Return(refResolver);
        }
    }

    public void Serialize<T>(PipeWriter writer, in T? value)
        where T : struct
    {
        var refResolver = _refResolverPool.Rent();
        try
        {
            if (SerializeCommon(new BatchWriter(writer), in value, refResolver, out var context))
            {
                context.Write(in value);
            }
        }
        finally
        {
            _refResolverPool.Return(refResolver);
        }
    }

    private bool SerializeCommon<T>(
        BatchWriter writer,
        in T? value,
        RefResolver refResolver,
        out SerializationContext context
    )
    {
        writer.Write(MagicNumber);
        var headerFlag = HeaderFlag.LittleEndian | HeaderFlag.CrossLanguage;
        if (value is null)
        {
            headerFlag |= HeaderFlag.NullRootObject;
            writer.Write((byte)headerFlag);
            context = default;
            return false;
        }
        writer.Write((byte)headerFlag);
        writer.Write((byte)Language.Csharp);
        context = new SerializationContext(this, writer, refResolver);
        return true;
    }
}
