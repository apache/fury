import { LATIN1, UTF8 } from "./type";
const { isLatin1: detectIsLatin1 } = require('/Users/quanzheng/nan-example-eol/build/Release/hello.node');

export const BinaryWriter = () => {
    let cursor = 0;
    let arrayBuffer = Buffer.alloc(1024 * 100);
    let dataView = new DataView(arrayBuffer.buffer);
    
    function reserves(len: number) {
        if (dataView.byteLength - (cursor + 1) <= len) {
            // resize the arrayBuffer
            arrayBuffer = Buffer.concat([arrayBuffer, Buffer.alloc(dataView.byteLength)]);
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
        return dataView.setUint8(move(1), v);
    }

    function writeInt8(v: number) {
        return dataView.setInt8(move(1), v);
    }

    function writeUInt16(v: number) {
        return dataView.setUint16(move(2), v, true);
    }

    function writeInt16(v: number) {
        return dataView.setInt16(move(2), v, true);
    }

    function skip(len: number) {
        move(len);
    }

    function writeInt32(v: number) {
        return dataView.setInt32(move(4), v, true);
    }

    function writeUInt32(v: number) {
        return dataView.setUint32(move(4), v, true);
    }

    function writeInt64(v: bigint | number) {
        if (typeof v === 'number') {
            return dataView.setBigInt64(move(8), BigInt(v), true);
        }
        return dataView.setBigInt64(move(8), v, true);
    }

    function writeFloat(v: number) {
        return dataView.setFloat32(move(4), v, true);
    }

    function writeDouble(v: number) {
        return dataView.setFloat64(move(8), v, true);
    }

    function writeBuffer(v: Uint8Array) {
        const len = v.byteLength;
        arrayBuffer.set(v, move(len));
    }

    function writeUInt64(v: bigint | number) {
        if (typeof v === 'number') {
            return dataView.setBigUint64(move(8), BigInt(v), true);
        }
        return dataView.setBigUint64(move(8), v, true);
    }

    function writeUtf8StringOfInt16(bf: Buffer) {
        const len = bf.byteLength;
        writeInt16(len);
        reserves(len);
        bf.copy(arrayBuffer, move(len));
    }

    function fastWriteStringLatin1(string: string, buffer: Buffer, offset: number) {
        const start: number = offset;
        let c1: number;
        for (let i = 0; i < string.length; ++i) {
            c1 = string.charCodeAt(i);
            buffer[offset++] = c1;
        }
        return offset - start;
    }

    function fastWriteStringUtf8(string: string, buffer: Buffer, offset: number) {
        const start: number = offset;
        let c1: number;
        let c2: number;
        for (let i = 0; i < string.length; ++i) {
            c1 = string.charCodeAt(i);
            if (c1 < 128) {
                buffer[offset++] = c1;
            } else if (c1 < 2048) {
                buffer[offset++] = c1 >> 6 | 192;
                buffer[offset++] = c1 & 63 | 128;
            } else if ((c1 & 0xFC00) === 0xD800 && ((c2 = string.charCodeAt(i + 1)) & 0xFC00) === 0xDC00) {
                c1 = 0x10000 + ((c1 & 0x03FF) << 10) + (c2 & 0x03FF);
                ++i;
                buffer[offset++] = c1 >> 18 | 240;
                buffer[offset++] = c1 >> 12 & 63 | 128;
                buffer[offset++] = c1 >> 6 & 63 | 128;
                buffer[offset++] = c1 & 63 | 128;
            } else {
                buffer[offset++] = c1 >> 12 | 224;
                buffer[offset++] = c1 >> 6 & 63 | 128;
                buffer[offset++] = c1 & 63 | 128;
            }
        }
        return offset - start;
    }

    function writeStringOfVarInt32(v: string) {
        const isLatin1 = detectIsLatin1(v);
        const len = isLatin1 ? v.length : Buffer.byteLength(v);
        // type and len
        reserves(len);
        // write type
        dataView.setUint8(move(1), isLatin1 ? LATIN1 : UTF8);
        writeVarInt32(len);
        if (isLatin1) {
            if (len < 40) {
                fastWriteStringLatin1(v, arrayBuffer, move(len));
            } else {
                arrayBuffer.write(v, move(len), 'latin1');
            }
        } else {
            if (len < 40) {
                fastWriteStringUtf8(v, arrayBuffer, move(len));
            } else {
                (arrayBuffer as any).utf8Write(v, move(len));
            }
        }
    }

    function writeVarInt32(value: number) {
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

    return { skip, reset, reserves, writeUInt16, writeInt8, dump, writeUInt8, writeInt16, writeVarInt32, writeStringOfVarInt32, writeUtf8StringOfInt16, writeUInt64, writeBuffer, writeDouble, writeFloat, writeInt64, writeUInt32, writeInt32 }
}

export const BinaryReader = () => {
    let cursor = 0;
    let dataView!: DataView;
    let buffer!: Buffer;
    let bufferAsLatin1String: string;


    function reset(u8Array: Uint8Array) {
        dataView = new DataView(u8Array.buffer);
        buffer = Buffer.from(u8Array);
        bufferAsLatin1String = (buffer as any).latin1Slice(0, buffer.byteLength);
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
        const result = (buffer as any).utf8Slice(cursor, cursor + len);
        cursor += len;
        return result;
    }


    function readStringLatin1(len: number) {
        const result = bufferAsLatin1String.substring(cursor, cursor + len);
        cursor += len;
        return result;
    }

    function readBuffer(len: number) {
        return new Uint8Array(dataView.buffer.slice(cursor, cursor + len))
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