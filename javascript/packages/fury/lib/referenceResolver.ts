import { RefFlags, BinaryReader } from "./type";


export const ReferenceResolver = () => {
    let readObjects: any[] = [];
    let writeObjects: any[] = [];

    function reset() {
        readObjects = [];
        writeObjects =[];
    }

    function getReadObjectByRefId(refId: number) {
        return readObjects[refId];
    }

    function readRefFlag(binaryView: BinaryReader) {
        return binaryView.readInt8() as RefFlags;
    }

    function pushReadObject(object: any) {
        readObjects.push(object);
    }

    function pushWriteObject(object: any) {
        writeObjects.push(object);
    }

    function existsWriteObject(obj: any) {
        for (let index = 0; index < writeObjects.length; index++) {
            if (writeObjects[index] === obj) {
                return index;
            }
        }
    }
    return {
        existsWriteObject, pushWriteObject, pushReadObject, readRefFlag, getReadObjectByRefId, reset
    }
}
