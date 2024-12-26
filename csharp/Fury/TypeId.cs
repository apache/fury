namespace Fury;

public readonly record struct TypeId
{
    internal int Value { get; }

    public TypeId(int value)
    {
        Value = value;
    }

    /// A NULL type having no physical storage
    public static readonly TypeId Na = new(0);

    public static readonly TypeId Bool = new(1);
    public static readonly TypeId Int8 = new(2);
    public static readonly TypeId Int16 = new(3);
    public static readonly TypeId Int32 = new(4);
    public static readonly TypeId VarInt32 = new(5);
    public static readonly TypeId Int64 = new(6);
    public static readonly TypeId VarInt64 = new(7);
    public static readonly TypeId SliInt64 = new(8);
    public static readonly TypeId Float16 = new(9);
    public static readonly TypeId Float32 = new(10);
    public static readonly TypeId Float64 = new(11);
    public static readonly TypeId String = new(12);
    public static readonly TypeId Enum = new(13);
    public static readonly TypeId NsEnum = new(14);
    public static readonly TypeId Struct = new(15);
    public static readonly TypeId PolymorphicStruct = new(16);
    public static readonly TypeId CompatibleStruct = new(17);
    public static readonly TypeId PolymorphicCompatibleStruct = new(18);
    public static readonly TypeId NsStruct = new(19);
    public static readonly TypeId NsPolymorphicStruct = new(20);
    public static readonly TypeId NsCompatibleStruct = new(21);
    public static readonly TypeId NsPolymorphicCompatibleStruct = new(22);
    public static readonly TypeId Ext = new(23);
    public static readonly TypeId PolymorphicExt = new(24);
    public static readonly TypeId NsExt = new(25);
    public static readonly TypeId NsPolymorphicExt = new(26);
    public static readonly TypeId List = new(27);
    public static readonly TypeId Set = new(28);
    public static readonly TypeId Map = new(29);
    public static readonly TypeId Duration = new(30);
    public static readonly TypeId Timestamp = new(31);
    public static readonly TypeId LocalDate = new(32);
    public static readonly TypeId Decimal = new(33);
    public static readonly TypeId Binary = new(34);
    public static readonly TypeId Array = new(35);
    public static readonly TypeId BoolArray = new(36);
    public static readonly TypeId Int8Array = new(37);
    public static readonly TypeId Int16Array = new(38);
    public static readonly TypeId Int32Array = new(39);
    public static readonly TypeId Int64Array = new(40);
    public static readonly TypeId Float16Array = new(41);
    public static readonly TypeId Float32Array = new(42);
    public static readonly TypeId Float64Array = new(43);
    public static readonly TypeId ArrowRecordBatch = new(44);
    public static readonly TypeId ArrowTable = new(45);

    public bool IsStructType()
    {
        return this == Struct
            || this == PolymorphicStruct
            || this == CompatibleStruct
            || this == PolymorphicCompatibleStruct
            || this == NsStruct
            || this == NsPolymorphicStruct
            || this == NsCompatibleStruct
            || this == NsPolymorphicCompatibleStruct;
    }
}
