import { Fury } from "../type";
import { InternalSerializerType, RefFlags, LATIN1 } from "../type";



export default (fury: Fury) => {
    const { binaryView, binaryWriter, writeNull, referenceResolver } = fury;
    const { writeInt8, writeInt16, writeStringOfVarInt32 } = binaryWriter
    const { readVarInt32, readStringUtf8, readUInt8, readStringLatin1 } = binaryView;
    const { pushReadObject } = referenceResolver;

    return {
        read: (shouldSetRef: boolean) => {
            const type = readUInt8();
            const len = readVarInt32();
            const result = type === LATIN1 ? readStringLatin1(len) : readStringUtf8(len);
            if (shouldSetRef) {
                pushReadObject(result);
            }
            return result;
        },
        write: (v: string) => {
            if (writeNull(v)) {
                return;
            }
            writeInt8(RefFlags.RefValueFlag);
            writeInt16(InternalSerializerType.STRING);
            writeStringOfVarInt32(v);
        },
        writeWithOutType: (v: string) => {
            if (writeNull(v)) {
                return;
            }
            writeInt8(RefFlags.RefValueFlag);
            writeStringOfVarInt32(v);
        },
        reserveWhenWrite: () => {
            return 7; 
        }
    }
}
