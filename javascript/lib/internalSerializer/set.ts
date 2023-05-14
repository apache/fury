import { Fury } from "../type";
import { InternalSerializerType, RefFlags } from "../type";


export default (fury: Fury) => {
    const { binaryView, binaryWriter, writeNullOrRef, referenceResolver, write, read } = fury;
    const { writeInt8, writeInt16, writeUInt32 } = binaryWriter;
    const { readUInt32 } = binaryView;

    const { pushReadObject, pushWriteObject } = referenceResolver;
    return {
        read: (shouldSetRef: boolean) => {
            const len = readUInt32();
            const result = new Set();
            if (shouldSetRef) {
                pushReadObject(result);
            }
            for (let index = 0; index < len; index++) {
                result.add(read());
            }
            return result;
        },
        write: (v: Set<any>) => {
            if (writeNullOrRef(v)) {
                return;
            }
            writeInt8(RefFlags.RefValueFlag);
            writeInt16(InternalSerializerType.FURY_SET);
            pushWriteObject(v);
            const len = v.size;
            writeUInt32(len);
            for (const value of v.values()) {
                write(value);
            }
        },
    }
}
