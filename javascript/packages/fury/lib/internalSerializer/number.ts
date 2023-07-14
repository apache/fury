import { InternalSerializerType, RefFlags, Fury } from "../type";

export const uInt8Serializer = (fury: Fury) => {
    const { binaryWriter, binaryView } = fury;
    const { writeInt8, writeInt16, writeUInt8 } = binaryWriter;
    const { readUInt8 } = binaryView;

    return {
        read: () => {
            return readUInt8();
        },
        write: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.UINT8);
            writeUInt8(v);
        },
        config: () => {
            return {
                reserve: 4,
            }
        }
    };
};

export const floatSerializer = (fury: Fury) => {
    const { binaryWriter, binaryView } = fury;
    const { writeInt8, writeInt16, writeFloat } = binaryWriter;
    const { readFloat } = binaryView;

    return {
        read: () => {
            return readFloat();
        },
        write: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.FLOAT);
            writeFloat(v);
        },
        config: () => {
            return {
                reserve: 7,
            }
        }
    };
};

export const doubleSerializer = (fury: Fury) => {
    const { binaryWriter, binaryView } = fury;
    const { writeInt8, writeInt16, writeDouble } = binaryWriter;
    const { readDouble } = binaryView;

    return {
        read: () => {
            return readDouble();
        },
        write: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.DOUBLE);
            writeDouble(v);
        },
        config: () => {
            return {
                reserve: 11,
            }
        }
    };
};

export const int8Serializer = (fury: Fury) => {
    const { binaryWriter, binaryView } = fury;
    const { writeInt8, writeInt16 } = binaryWriter;
    const { readInt8 } = binaryView;

    return {
        read: () => {
            return readInt8();
        },
        write: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.INT8);
            writeInt8(v);
        },
        config: () => {
            return {
                reserve: 4,
            }
        }
    };
};

export const uInt16Serializer = (fury: Fury) => {
    const { binaryWriter, binaryView } = fury;
    const { writeInt8, writeInt16, writeUInt16 } = binaryWriter;
    const { readUInt16 } = binaryView;

    return {
        read: () => {
            return readUInt16();
        },
        write: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.UINT16);
            writeUInt16(v);
        },
        config: () => {
            return {
                reserve: 5,
            }
        }
    };
};

export const int16Serializer = (fury: Fury) => {
    const { binaryWriter, binaryView } = fury;
    const { writeInt16, writeInt8 } = binaryWriter;
    const { readInt16 } = binaryView;

    return {
        read: () => {
            return readInt16();
        },
        write: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.INT16);
            writeInt16(v);
        },
        config: () => {
            return {
                reserve: 5,
            }
        }
    };
};

export const uInt32Serializer = (fury: Fury) => {
    const { binaryWriter, binaryView } = fury;
    const { writeInt8, writeInt16, writeUInt32 } = binaryWriter;
    const { readUInt32 } = binaryView;

    return {
        read: () => {
            return readUInt32();
        },
        write: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.UINT32);
            writeUInt32(v);
        },
        config: () => {
            return {
                reserve: 7,
            }
        }
    };
};

export const int32Serializer = (fury: Fury) => {
    const { binaryWriter, binaryView } = fury;
    const { writeInt8, writeInt16, writeInt32 } = binaryWriter;
    const { readInt32 } = binaryView;

    return {
        read: () => {
            return readInt32();
        },
        write: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.INT32);
            writeInt32(v);
        },
        config: () => {
            return {
                reserve: 7,
            }
        }
    };
};

export const uInt64Serializer = (fury: Fury) => {
    const { binaryWriter, binaryView } = fury;
    const { writeInt8, writeInt16, writeUInt64 } = binaryWriter;
    const { readUInt64 } = binaryView;

    return {
        read: () => {
            return readUInt64();
        },
        write: (v: bigint) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.UINT64);
            writeUInt64(v);
        },
        config: () => {
            return {
                reserve: 11,
            }
        }
    };
};

export const int64Serializer = (fury: Fury) => {
    const { binaryWriter, binaryView } = fury;
    const { writeInt8, writeInt16, writeInt64 } = binaryWriter;
    const { readInt64 } = binaryView;

    return {
        read: () => {
            return readInt64();
        },
        write: (v: bigint) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.INT64);
            writeInt64(v);
        },
        config: () => {
            return {
                reserve: 11,
            }
        }
    };
};
