import ClassResolver from './classResolver';
import { BinaryReader, BinaryWriter } from './io';
import { ReferenceResolver } from './referenceResolver';
import { ConfigFlags, InternalSerializerType, Serializer, RefFlags, SerializerRead, Hps } from './type';


export default (config?: {
    hps: Hps | null;
}) => {
    const classResolver = new ClassResolver();
    const referenceResolver = ReferenceResolver();
    const binaryView = BinaryReader();
    const binaryWriter = BinaryWriter(config);

    const fury = {
        skipType,
        readBySerializerWithOutTypeId,
        read,
        config,
        deserialize,
        writeNull,
        writeNullOrRef,
        write,
        serialize,
        referenceResolver,
        classResolver,
        binaryView,
        binaryWriter,
    }
    classResolver.init(fury);

    const { write: writeInt64 } = classResolver.getSerializerById(InternalSerializerType.INT64);
    const { write: writeBool } = classResolver.getSerializerById(InternalSerializerType.BOOL);
    const { write: stringWrite } = classResolver.getSerializerById(InternalSerializerType.STRING);
    const { write: arrayWrite } = classResolver.getSerializerById(InternalSerializerType.ARRAY);
    const { write: mapWrite } = classResolver.getSerializerById(InternalSerializerType.MAP);
    const { write: setWrite } = classResolver.getSerializerById(InternalSerializerType.FURY_SET);
    const { write: timestampWrite } = classResolver.getSerializerById(InternalSerializerType.TIMESTAMP);

    function readSerializer() {
        const typeId = binaryView.readInt16();
        let serializer: Serializer;
        if (typeId === InternalSerializerType.FURY_TYPE_TAG) {
            serializer = classResolver.getSerializerByTag(classResolver.readTag(binaryView));
        } else {
            serializer = classResolver.getSerializerById(typeId);
        }
        if (!serializer) {
            throw new Error(`cant find implements of typeId: ${typeId}`);
        }
        return serializer;
    }

    function skipType() {
        const typeId = binaryView.readInt16();
        if (typeId === InternalSerializerType.FURY_TYPE_TAG) {
            classResolver.readTag(binaryView);
        }
    }

    function readBySerializerWithOutTypeId(read: SerializerRead) {
        const flag = referenceResolver.readRefFlag(binaryView);
        switch (flag) {
            case RefFlags.RefValueFlag:
                return read(true)
            case RefFlags.RefFlag:
                return referenceResolver.getReadObjectByRefId(binaryView.readVarInt32());
            case RefFlags.NullFlag:
                return null;
            case RefFlags.NotNullValueFlag:
                return read(false)
        }
    }

    function read() {
        const flag = referenceResolver.readRefFlag(binaryView);
        switch (flag) {
            case RefFlags.RefValueFlag:
                return readSerializer().read(true);
            case RefFlags.RefFlag:
                return referenceResolver.getReadObjectByRefId(binaryView.readVarInt32());
            case RefFlags.NullFlag:
                return null;
            case RefFlags.NotNullValueFlag:
                return readSerializer().read(false);
        }
    }

    function deserialize<T = any>(bytes: Buffer): T | null {
        referenceResolver.reset();
        classResolver.reset();
        binaryView.reset(bytes);
        const bitmap = binaryView.readUInt8();
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
        binaryView.readUInt8(); // skip language type
        const isOutOfBandEnabled = (bitmap & ConfigFlags.isOutOfBandFlag) === ConfigFlags.isOutOfBandFlag;
        if (isOutOfBandEnabled) {
            throw new Error('outofband mode is not supported now')
        }
        binaryView.readInt32(); // native object offset. should skip.  javascript support cross mode only
        binaryView.readInt32(); // native object size. should skip.
        return read();
    }

    function writeNull(v: any) {
        if (v === null) {
            binaryWriter.writeInt8(RefFlags.NullFlag);
            return true;
        } else {
            return false;
        }
    }

    function writeNullOrRef(v: any) {
        if (v !== null) {
            const existsId = referenceResolver.existsWriteObject(v);
            if (typeof existsId === 'number') {
                binaryWriter.writeInt8(RefFlags.RefFlag);
                binaryWriter.writeVarInt32(existsId);
                return true;
            }
            return false;
        } else {
            binaryWriter.writeInt8(RefFlags.NullFlag);
            return true;
        }
    }

    function write(v: any) {
        // NullFlag
        if (v === null || v === undefined) {
            binaryWriter.writeInt8(RefFlags.NullFlag); // null
            return;
        }

        // NotNullValueFlag
        if (typeof v === "number") {
            writeInt64(BigInt(v));
            return;
        }
        if (typeof v === "bigint") {
            writeInt64(v)
            return;
        }

        if (typeof v === "boolean") {
            writeBool(v)
            return;
        }

        // RefFlag
        const existsId = referenceResolver.existsWriteObject(v);
        if (typeof existsId === 'number') {
            binaryWriter.writeInt8(RefFlags.RefFlag);
            binaryWriter.writeVarInt32(existsId);
            return;
        }

        // RefValueFlag
        if (v instanceof Map) {
            mapWrite(v);
            return;
        }
        if (v instanceof Set) {
            setWrite(v);
            return;
        }
        if (Array.isArray(v)) {
            arrayWrite(v);
            return;
        }

        if (v instanceof Date) {
            timestampWrite(v);
            return;
        }

        if (typeof v === "string") {
            stringWrite(v);
            return;
        }
        throw new Error(`serializer not support ${typeof v} yet`);
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
        binaryWriter.writeUInt8(bitmap);
        binaryWriter.writeUInt8(4); // todo: replace with javascript
        binaryWriter.skip(4) // preserve 4-byte for nativeObjects start offsets.
        binaryWriter.skip(4) // preserve 4-byte for nativeObjects length.
        if (serializer) {
            serializer.write(data);
        } else {
            write(data);
        }
        return binaryWriter.dump();
    }
    return fury;
}
