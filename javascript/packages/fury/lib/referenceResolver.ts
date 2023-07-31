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
