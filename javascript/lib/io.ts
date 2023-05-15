import { LATIN1, UTF8 } from "./type";
import { latin1Decoder, utf8Decoder, utf8Encoder } from "./util";


export const BinaryWriter = () => {
    let cursor = 0;
    let arrayBuffer = new Uint8Array(1024 * 100);
    let dataView = new DataView(arrayBuffer.buffer);
    function reserves(len: number) {
        if (dataView.byteLength - (cursor + 1) <= len) {
            // resize the arrayBuffer
            let newArrayBuffer = new Uint8Array(dataView.byteLength * 2);
            newArrayBuffer.set(arrayBuffer);
            arrayBuffer = newArrayBuffer;
            dataView = new DataView(arrayBuffer.buffer);
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
        return dataView.setUint8(move(1), v);
    }

    function writeInt8(v: number) {
        reserves(1);
        return dataView.setInt8(move(1), v);
    }

    function writeUInt16(v: number) {
        reserves(2);
        return dataView.setUint16(move(2), v, true);
    }

    function writeInt16(v: number) {
        reserves(2);
        return dataView.setInt16(move(2), v, true);
    }

    function skip(len: number) {
        reserves(len);
        move(len);
    }

    function writeInt32(v: number) {
        reserves(4);
        return dataView.setInt32(move(4), v, true);
    }

    function writeUInt32(v: number) {
        reserves(4);
        return dataView.setUint32(move(4), v, true);
    }

    function writeInt64(v: bigint | number) {
        reserves(8);
        if (typeof v === 'number') {
            return dataView.setBigInt64(move(8), BigInt(v), true);
        }
        return dataView.setBigInt64(move(8), v, true);
    }

    function writeFloat(v: number) {
        reserves(4);
        return dataView.setFloat32(move(4), v, true);
    }

    function writeDouble(v: number) {
        reserves(8);
        return dataView.setFloat64(move(8), v, true);
    }

    function writeBuffer(v: Uint8Array) {
        const len = v.byteLength;
        reserves(len);
        arrayBuffer.set(v, move(len));
    }

    function writeUInt64(v: bigint | number) {
        reserves(8);
        if (typeof v === 'number') {
            return dataView.setBigUint64(move(8), BigInt(v), true);
        }
        return dataView.setBigUint64(move(8), v, true);
    }

    function writeUtf8StringOfInt16(v: string) {
        const ab = utf8Encoder.encode(v);
        const len = ab.byteLength;
        writeInt16(len);
        reserves(len);
        arrayBuffer.set(ab, move(len));
    }

    function writeStringOfVarInt32(v: string) {
        const ab = utf8Encoder.encode(v);
        const len = ab.byteLength;
        // type and len
        reserves(len + 6);
        const isLatin1 = v.length === len;
        // write type
        dataView.setUint8(move(1), isLatin1 ? LATIN1 : UTF8);
        writeVarInt32(len);
        arrayBuffer.set(ab, move(len));
    }

    function writeVarInt32(value: number) {
        reserves(5);
        if (value >> 7 == 0) {
            arrayBuffer[cursor] = value
            move(1)
            return 1
        }
        if (value >> 14 == 0) {
            arrayBuffer[cursor] = (value & 0x7F) | 0x80
            arrayBuffer[cursor + 1] = value >> 7
            move(2)
            return 2
        }
        if (value >> 21 == 0) {
            arrayBuffer[cursor] = (value & 0x7F) | 0x80
            arrayBuffer[cursor + 1] = value >> 7 | 0x80
            arrayBuffer[cursor + 2] = value >> 14
            move(3)
            return 3
        }
        if (value >> 28 == 0) {
            arrayBuffer[cursor] = (value & 0x7F) | 0x80
            arrayBuffer[cursor + 1] = value >> 7 | 0x80
            arrayBuffer[cursor + 2] = value >> 14 | 0x80
            arrayBuffer[cursor + 3] = value >> 21
            move(4)
            return 4
        }
        arrayBuffer[cursor] = (value & 0x7F) | 0x80
        arrayBuffer[cursor + 1] = value >> 7 | 0x80
        arrayBuffer[cursor + 2] = value >> 14 | 0x80
        arrayBuffer[cursor + 3] = value >> 21 | 0x80
        arrayBuffer[cursor + 4] = value >> 28
        move(5)
        return 5
    }

    function dump() {
        return arrayBuffer.slice(0, cursor);
    }

    return { skip, reset, writeUInt16, writeInt8, dump, writeUInt8, writeInt16, writeVarInt32, writeStringOfVarInt32, writeUtf8StringOfInt16, writeUInt64, writeBuffer, writeDouble, writeFloat, writeInt64, writeUInt32, writeInt32 }
}

export const BinaryReader = () => {
    let cursor = 0;
    let dataView!: DataView;
    let arrayBuffer!: Uint8Array;


    function reset(buffer: Uint8Array) {
        dataView = new DataView(buffer.buffer);
        arrayBuffer = buffer;
        cursor = 0;
    }

    function readUInt8() {
        return dataView.getUint8(cursor++);
    }

    function readInt8() {
        return dataView.getInt8(cursor++);
    }

    function readUInt16() {
        const result = dataView.getUint16(cursor, true);
        cursor += 2;
        return result;
    }

    function readInt16() {
        const result = dataView.getInt16(cursor, true);
        cursor += 2;
        return result;
    }

    function skip(len: number) {
        cursor += len;
    }



    function readInt32() {
        const result = dataView.getInt32(cursor, true);
        cursor += 4;
        return result;
    }

    function readUInt32() {
        const result = dataView.getUint32(cursor, true);
        cursor += 4;
        return result;
    }

    function readInt64() {
        const result = dataView.getBigInt64(cursor, true);
        cursor += 8;
        return Number(result);
    }

    function readUInt64() {
        const result = dataView.getBigUint64(cursor, true);
        cursor += 8;
        return Number(result);
    }


    function readFloat() {
        const result = dataView.getFloat32(cursor, true);
        cursor += 4;
        return result;
    }

    function readDouble() {
        const result = dataView.getFloat64(cursor, true);
        cursor += 8;
        return result;
    }

    function readStringUtf8(len: number) {
        const result = utf8Decoder.decode(arrayBuffer.subarray(cursor, cursor + len));
        cursor += len;
        return result;
    }


    function readStringLatin1(len: number) {
        const result = latin1Decoder.decode(arrayBuffer.subarray(cursor, cursor + len));
        cursor += len;
        return result;
    }

    function readBuffer(len: number) {
        return arrayBuffer.slice(cursor, cursor + len);
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