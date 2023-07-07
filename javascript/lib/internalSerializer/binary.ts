import { Fury } from "../type";
import { InternalSerializerType, RefFlags } from "../type";


export default (fury: Fury) => {
    const { binaryView, binaryWriter, referenceResolver, writeNullOrRef } = fury;
    const { writeInt8, writeInt16, writeUInt8, writeInt32, writeBuffer } = binaryWriter;
    const { readUInt8, readInt32, readBuffer } = binaryView;
    const { pushReadObject, pushWriteObject } = referenceResolver;
    
    return {
        read: (shouldSetRef: boolean) => {
            readUInt8(); // isInBand
            const len = readInt32();
            const result = readBuffer(len);
            if (shouldSetRef) {
                pushReadObject(result);
            }
            return result
        },
        write: (v: Uint8Array) => {
            if (writeNullOrRef(v)) {
                return;
            }
            writeInt8(RefFlags.RefValueFlag);
            writeInt16(InternalSerializerType.BINARY);
            pushWriteObject(v);
            writeUInt8(1); // is inBand
            writeInt32(v.byteLength);
            writeBuffer(v);
        },
        reserveWhenWrite: () => {
            return 8; 
        }
    }
}