/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { InternalSerializerType, RefFlags, Fury } from "../type";

export const uInt8Serializer = (fury: Fury) => {
    const { binaryWriter, binaryReader, referenceResolver } = fury;
    const { int8: writeInt8, uint8: writeUInt8 } = binaryWriter;
    const { uint8: readUInt8 } = binaryReader;
    return {
        ...referenceResolver.deref(() => {
            return readUInt8();
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.UINT8, (v: number) => {
            writeUInt8(v);
        }),
        config: () => {
            return {
                reserve: 4,
            }
        }
    };
};

export const floatSerializer = (fury: Fury) => {
    const { binaryWriter, binaryReader, referenceResolver } = fury;
    const { int8: writeInt8, float: writeFloat } = binaryWriter;
    const { float: readFloat } = binaryReader;

    return {
        ...referenceResolver.deref(() => {
            return readFloat();
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.FLOAT, (v: number) => {
            writeFloat(v);
        }),
        writeWithoutType: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
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
    const { binaryWriter, binaryReader, referenceResolver } = fury;
    const { int8: writeInt8, double: writeDouble } = binaryWriter;
    const { double: readDouble } = binaryReader;

    return {
        ...referenceResolver.deref(() => {
            return readDouble();
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.DOUBLE, (v: number) => {
            writeDouble(v);
        }),
        writeWithoutType: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
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
    const { binaryWriter, binaryReader, referenceResolver } = fury;
    const { int8: writeInt8 } = binaryWriter;
    const { int8: readInt8 } = binaryReader;
    return {
        ...referenceResolver.deref(() => {
            return readInt8();
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.INT8, (v: number) => {
            writeInt8(v);
        }),
        config: () => {
            return {
                reserve: 4,
            }
        }
    };
};

export const uInt16Serializer = (fury: Fury) => {
    const { binaryWriter, binaryReader, referenceResolver } = fury;
    const { int8: writeInt8, uint16: writeUInt16 } = binaryWriter;
    const { uint16: readUInt16 } = binaryReader;

    return {
        ...referenceResolver.deref(() => {
            return readUInt16();
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.UINT16, (v: number) => {
            writeUInt16(v);
        }),
        config: () => {
            return {
                reserve: 5,
            }
        }
    };
};

export const int16Serializer = (fury: Fury) => {
    const { binaryWriter, binaryReader, referenceResolver } = fury;
    const { int16: writeInt16, int8: writeInt8 } = binaryWriter;
    const { int16: readInt16 } = binaryReader;

    return {
        ...referenceResolver.deref(() => {
            return readInt16();
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.INT16, (v: number) => {
            writeInt16(v);
        }),
        writeWithoutType: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
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
    const { binaryWriter, binaryReader, referenceResolver } = fury;
    const { int8: writeInt8, uint32: writeUInt32 } = binaryWriter;
    const { uint32: readUInt32 } = binaryReader;

    return {
        ...referenceResolver.deref(() => {
            return readUInt32();
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.UINT32, (v: number) => {
            writeUInt32(v);
        }),
        config: () => {
            return {
                reserve: 7,
            }
        }
    };
};

export const int32Serializer = (fury: Fury) => {
    const { binaryWriter, binaryReader, referenceResolver } = fury;
    const { int8: writeInt8, int32: writeInt32 } = binaryWriter;
    const { int32: readInt32 } = binaryReader;

    return {
        ...referenceResolver.deref(() => {
            return readInt32();
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.INT32, (v: number) => {
            writeInt32(v);
        }),
        writeWithoutType: (v: number) => {
            writeInt8(RefFlags.NotNullValueFlag);
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
    const { binaryWriter, binaryReader, referenceResolver } = fury;
    const { int8: writeInt8, uint64: writeUInt64 } = binaryWriter;
    const { uint64: readUInt64 } = binaryReader;

    return {
        ...referenceResolver.deref(() => {
            return readUInt64();
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.UINT64, (v: bigint) => {
            writeUInt64(v);
        }),
        config: () => {
            return {
                reserve: 11,
            }
        }
    };
};

export const int64Serializer = (fury: Fury) => {
    const { binaryWriter, binaryReader, referenceResolver } = fury;
    const { int8: writeInt8, int64: writeInt64 } = binaryWriter;
    const { int64: readInt64 } = binaryReader;

    return {
        ...referenceResolver.deref(() => {
            return readInt64();
        }),
        write: referenceResolver.withNotNullableWriter(InternalSerializerType.INT64, (v: bigint) => {
            writeInt64(v);
        }),
        writeWithoutType: (v: bigint) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt64(v);
        },
        config: () => {
            return {
                reserve: 11,
            }
        }
    };
};
