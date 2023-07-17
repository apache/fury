import { Fury } from "../type";
import { InternalSerializerType, RefFlags } from "../type";



export default (fury: Fury) => {
    const { binaryView, binaryWriter, writeNull } = fury;
    const { writeInt8, writeInt16, writeStringOfVarInt32 } = binaryWriter
    const { readVarInt32, readStringUtf8 } = binaryView;

    return {
        read: () => {
            // todo support latin1
            //const type = readUInt8();
            const len = readVarInt32();
            const result = readStringUtf8(len);
            return result;
        },
        write: (v: string) => {
            if (writeNull(v)) {
                return;
            }
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.STRING);
            writeStringOfVarInt32(v);
        },
        writeWithOutType: (v: string) => {
            if (writeNull(v)) {
                return;
            }
            writeInt8(RefFlags.NotNullValueFlag);
            writeStringOfVarInt32(v);
        },
        config: () => {
            return {
                reserve: 7,
            }
        }
    }
}
