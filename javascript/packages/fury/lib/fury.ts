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

import ClassResolver from './classResolver';
import { BinaryWriter } from './writer';
import { BinaryReader } from './reader';
import { ReferenceResolver } from './referenceResolver';
import { ConfigFlags, Serializer, Config, InternalSerializerType } from './type';

export default (config: Config) => {
    const binaryReader = BinaryReader(config);
    const binaryWriter = BinaryWriter(config);

    const classResolver = new ClassResolver();
    const referenceResolver = ReferenceResolver(config, binaryWriter, binaryReader, classResolver);

    const fury = {
        config,
        deserialize,
        serialize,
        referenceResolver,
        classResolver,
        binaryReader,
        binaryWriter,
    }
    classResolver.init(fury);


    function deserialize<T = any>(bytes: Buffer): T | null {
        referenceResolver.reset();
        classResolver.reset();
        binaryReader.reset(bytes);
        const bitmap = binaryReader.uint8();
        if ((bitmap & ConfigFlags.isNullFlag) === ConfigFlags.isNullFlag) {
            return null;
        }
        const isLittleEndian = (bitmap & ConfigFlags.isLittleEndianFlag) === ConfigFlags.isLittleEndianFlag;
        if (!isLittleEndian) {
            throw new Error('big endian is not supported now')
        }
        const isCrossLanguage = (bitmap & ConfigFlags.isCrossLanguageFlag) == ConfigFlags.isCrossLanguageFlag
        if (!isCrossLanguage) {
            throw new Error('support crosslanguage mode only')
        }
        binaryReader.uint8(); // skip language type
        const isOutOfBandEnabled = (bitmap & ConfigFlags.isOutOfBandFlag) === ConfigFlags.isOutOfBandFlag;
        if (isOutOfBandEnabled) {
            throw new Error('outofband mode is not supported now')
        }
        binaryReader.int32(); // native object offset. should skip.  javascript support cross mode only
        binaryReader.int32(); // native object size. should skip.
        return classResolver.getSerializerById(InternalSerializerType.ANY).read();
    }

    function serialize<T = any>(data: T, serializer?: Serializer) {
        referenceResolver.reset();
        classResolver.reset();
        binaryWriter.reset();
        let bitmap = 0;
        if (data === null) {
            bitmap |= ConfigFlags.isNullFlag;
        }
        bitmap |= ConfigFlags.isLittleEndianFlag;
        bitmap |= ConfigFlags.isCrossLanguageFlag
        binaryWriter.uint8(bitmap);
        binaryWriter.uint8(4); // todo: replace with javascript
        binaryWriter.skip(4) // preserve 4-byte for nativeObjects start offsets.
        binaryWriter.skip(4) // preserve 4-byte for nativeObjects length.
        if (serializer) {
            serializer.write(data);
        } else {
            classResolver.getSerializerById(InternalSerializerType.ANY).write(data);
        }
        return binaryWriter.dump();
    }
    return fury;
}
