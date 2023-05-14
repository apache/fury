import { LATIN1, UTF8 } from "./type";


export const BinaryWriter = () => {
    let cursor = 0;
    let dataView = Buffer.allocUnsafe(Buffer.poolSize / 2); // Buffer.poolSize is 8k, it'll fit it in to a pre-allocated 8KB chunk of memory.
    function reserves(len: number) {
        if (dataView.byteLength - (cursor + 1) <= len) {
            dataView = Buffer.concat([dataView, Buffer.allocUnsafe(dataView.byteLength)]);
        }
    }
    function reset() {
        cursor = 0;
    }

    function move(len: number) {
        const result = cursor;
        cursor += len;
        return result;
    }


    function writeUInt8(v: number) {
        reserves(1);
        return dataView.writeUint8(v, move(1));
    }

    function writeInt8(v: number) {
        reserves(1);
        return dataView.writeInt8(v, move(1));
    }

    function writeUInt16(v: number) {
        reserves(2);
        return dataView.writeUInt16LE(v, move(2));
    }

    function writeInt16(v: number) {
        reserves(2);
        return dataView.writeInt16LE(v, move(2));
    }

    function skip(len: number) {
        reserves(len);
        dataView.fill(0, move(len), cursor);
    }

    function writeInt32(v: number) {
        reserves(4);
        return dataView.writeInt32LE(v, move(4));
    }

    function writeUInt32(v: number) {
        reserves(4);
        return dataView.writeUInt32LE(v, move(4));
    }

    function writeInt64(v: bigint | number) {
        reserves(8);
        return dataView.writeBigInt64LE(BigInt(v), move(8));
    }

    function writeFloat(v: number) {
        reserves(4);
        return dataView.writeFloatLE(v, move(4));
    }

    function writeDouble(v: number) {
        reserves(8);
        return dataView.writeDoubleLE(v, move(8));
    }

    function writeBuffer(v: Buffer) {
        const len = v.byteLength;
        reserves(len);
        v.copy(dataView, move(len));
    }

    function writeUInt64(v: bigint | number) {
        reserves(8);
        return dataView.writeBigUInt64LE(BigInt(v), move(8));
    }

    function writeStringOfInt16(v: string) {
        const len = Buffer.byteLength(v, 'utf8');
        writeInt16(len);
        reserves(len);
        return dataView.write(v, move(len), 'utf8');
    }

    function writeStringOfVarInt32(v: string) {
        const len = Buffer.byteLength(v, 'utf8');
        // type and len
        reserves(len + 6);
        const isLatin1 = v.length === len;
        dataView.writeUint8(isLatin1 ? LATIN1 : UTF8, move(1));
        writeVarInt32(len);
        if (isLatin1) {
            return dataView.write(v, move(len), 'latin1');
        }
        return dataView.write(v, move(len), 'utf8');
    }

    function writeVarInt32(value: number) {
        reserves(5);
        if (value >> 7 == 0) {
            dataView[cursor] = value
            move(1)
            return 1
        }
        if (value >> 14 == 0) {
            dataView[cursor] = (value & 0x7F) | 0x80
            dataView[cursor + 1] = value >> 7
            move(2)
            return 2
        }
        if (value >> 21 == 0) {
            dataView[cursor] = (value & 0x7F) | 0x80
            dataView[cursor + 1] = value >> 7 | 0x80
            dataView[cursor + 2] = value >> 14
            move(3)
            return 3
        }
        if (value >> 28 == 0) {
            dataView[cursor] = (value & 0x7F) | 0x80
            dataView[cursor + 1] = value >> 7 | 0x80
            dataView[cursor + 2] = value >> 14 | 0x80
            dataView[cursor + 3] = value >> 21
            move(4)
            return 4
        }
        dataView[cursor] = (value & 0x7F) | 0x80
        dataView[cursor + 1] = value >> 7 | 0x80
        dataView[cursor + 2] = value >> 14 | 0x80
        dataView[cursor + 3] = value >> 21 | 0x80
        dataView[cursor + 4] = value >> 28
        move(5)
        return 5
    }

    function dump() {
        return ArrayBuffer.prototype.slice.apply(dataView.buffer, [0, cursor]);
    }

    return { skip, reset, writeUInt16, writeInt8, dump, writeUInt8, writeInt16, writeVarInt32, writeStringOfVarInt32, writeStringOfInt16, writeUInt64, writeBuffer, writeDouble, writeFloat, writeInt64, writeUInt32, writeInt32 }
}

export const BinaryReader = () => {
    let cursor = 0;
    let dataView!: Buffer;


    function reset(buffer: Buffer) {
        dataView = buffer;
        cursor = 0;
    }

    function readUInt8() {
        return dataView.readUint8(cursor++);
    }

    function readInt8() {
        return dataView.readInt8(cursor++);
    }

    function readUInt16() {
        const result = dataView.readUInt16LE(cursor);
        cursor += 2;
        return result;
    }

    function readInt16() {
        const result = dataView.readInt16LE(cursor);
        cursor += 2;
        return result;
    }

    function skip(len: number) {
        cursor += len;
    }



    function readInt32() {
        const result = dataView.readInt32LE(cursor);
        cursor += 4;
        return result;
    }

    function readUInt32() {
        const result = dataView.readUInt32LE(cursor);
        cursor += 4;
        return result;
    }

    function readInt64() {
        const result = dataView.readBigInt64LE(cursor);
        cursor += 8;
        return Number(result);
    }

    function readUInt64() {
        const result = dataView.readBigUInt64LE(cursor);
        cursor += 8;
        return Number(result);
    }


    function readFloat() {
        const result = dataView.readFloatLE(cursor);
        cursor += 4;
        return result;
    }

    function readDouble() {
        const result = dataView.readDoubleLE(cursor);
        cursor += 8;
        return result;
    }

    function readStringUtf8(len: number) {
        const result = (dataView as any).utf8Slice(cursor, cursor + len);
        cursor += len;
        return result;
    }


    function readStringLatin1(len: number) {
        const result = (dataView as any).latin1Slice(cursor, cursor + len);
        cursor += len;
        return result;
    }

    function readBuffer(len: number) {
        return Buffer.from(ArrayBuffer.prototype.slice.apply(dataView.buffer, [cursor, cursor + len]));
    }


    function readVarInt32() {
        let byte_ = readInt8();
        let result = byte_ & 0x7F
        if ((byte_ & 0x80) != 0) {
            byte_ = readInt8()
            result |= (byte_ & 0x7F) << 7
            if ((byte_ & 0x80) != 0) {
                byte_ = readInt8()
                result |= (byte_ & 0x7F) << 14
                if ((byte_ & 0x80) != 0) {
                    byte_ = readInt8()
                    result |= (byte_ & 0x7F) << 21
                    if ((byte_ & 0x80) != 0) {
                        byte_ = readInt8()
                        result |= (byte_ & 0x7F) << 28
                    }
                }
            }
        }
        return result
    }

    return { readVarInt32, readInt8, readBuffer, readUInt8, reset, readStringUtf8, readStringLatin1, readDouble, readFloat, readUInt16, readInt16, readUInt64, skip, readInt64, readUInt32, readInt32 }
}