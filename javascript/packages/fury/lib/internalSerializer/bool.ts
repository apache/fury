import { Fury } from "../type";
import { InternalSerializerType, RefFlags } from "../type";


export default (fury: Fury) => {
    const { binaryView, binaryWriter } = fury;
    const { readUInt8 } = binaryView;
    const { writeInt8, writeInt16, writeUInt8 } = binaryWriter;
    return {
        read: () => {
            return readUInt8() === 0 ? false : true
        },
        write: (v: boolean) => {
            writeInt8(RefFlags.NotNullValueFlag);
            writeInt16(InternalSerializerType.BOOL);
            writeUInt8(v ? 1 : 0)
        },
        config: () => {
            return {
                reserve: 4,
            }
        }
    }
}