import { RefFlags, BinaryView } from "./type";


export const ReferenceResolver = () => {
    let readObjects: any[] = [];
    let writeObjects: Map<any, number> = new Map<any, number>();

    function reset() {
        readObjects = [];
        writeObjects = new Map();
    }

    function getReadObjectByRefId(refId: number) {
        return readObjects[refId];
    }

    function readRefFlag(binaryView: BinaryView) {
        return binaryView.readInt8() as RefFlags;
    }

    function pushReadObject(object: any) {
        readObjects.push(object);
    }

    function pushWriteObject(object: any) {
        writeObjects.set(object, writeObjects.size);
    }

    function existsWriteObject(obj: any) {
        return writeObjects.get(obj);
    }
    return {
        existsWriteObject, pushWriteObject, pushReadObject, readRefFlag, getReadObjectByRefId, reset
    }
}
