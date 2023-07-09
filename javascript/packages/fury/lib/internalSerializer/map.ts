import { InternalSerializerType, RefFlags, Fury } from "../type";



export default (fury: Fury) => {
    const { binaryView, binaryWriter, writeNullOrRef, read, referenceResolver, write } = fury;
    const { readUInt32 } = binaryView;
    const { writeInt8, writeInt16, writeUInt32 } = binaryWriter;
    const { pushReadObject, pushWriteObject } = referenceResolver;

    return {
        read: (shouldSetRef: boolean) => {
            const len = readUInt32();
            const result = new Map();
            if (shouldSetRef) {
                pushReadObject(result);
            }
            for (let index = 0; index < len; index++) {
                const key = read();
                const value = read();
                result.set(key, value);
            }
            return result;
        },
        write: (v: Map<any, any>) => {
            if (writeNullOrRef(v)) {
                return;
            }
            writeInt8(RefFlags.RefValueFlag);
            writeInt16(InternalSerializerType.MAP);
            pushWriteObject(v);
            const len = v.size;
            writeUInt32(len);
            for (const [key, value] of v.entries()) {
                write(key);
                write(value);
            }
        },
        reserveWhenWrite: () => {
            return 7; 
        }
    }
}
