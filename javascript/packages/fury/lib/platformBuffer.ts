import { isNodeEnv } from "./util";

export interface PlatformBuffer extends Uint8Array {
    latin1Slice(start: number, end: number): string,
    utf8Slice(start: number, end: number): string,
    latin1Write(v: string, offset: number): void,
    utf8Write(v: string, offset: number): void,
    copy(target: Uint8Array, targetStart?: number, sourceStart?: number, sourceEnd?: number): Uint8Array
}


class BrowserBuffer extends Uint8Array {
    static alloc(size: number) {
        return new BrowserBuffer(new Uint8Array(size));
    }

    latin1Slice(start: number, end: number) {
        if (end - start < 1) {
            return "";
        }
        let str = "";
        for (let i = start; i < end;) {
            str += String.fromCharCode(this[i++]);
        }
        return str;
    }

    utf8Slice(start: number, end: number) {
        if (end - start < 1) {
            return "";
        }
        let str = "";
        for (let i = start; i < end;) {
            const t = this[i++];
            if (t <= 0x7F) {
                str += String.fromCharCode(t);
            } else if (t >= 0xC0 && t < 0xE0) {
                str += String.fromCharCode((t & 0x1F) << 6 | this[i++] & 0x3F);
            } else if (t >= 0xE0 && t < 0xF0) {
                str += String.fromCharCode((t & 0xF) << 12 | (this[i++] & 0x3F) << 6 | this[i++] & 0x3F);
            } else if (t >= 0xF0) {
                const t2 = ((t & 7) << 18 | (this[i++] & 0x3F) << 12 | (this[i++] & 0x3F) << 6 | this[i++] & 0x3F) - 0x10000;
                str += String.fromCharCode(0xD800 + (t2 >> 10));
                str += String.fromCharCode(0xDC00 + (t2 & 0x3FF));
            }
        }

        return str;
    }

    copy(target: Uint8Array, targetStart?: number, sourceStart?: number, sourceEnd?: number) {
        target.set(this.subarray(sourceStart, sourceEnd), targetStart);
    }
}

export function fromString(str: string) {
    if (isNodeEnv) {
        return Buffer.from(str);
    } else {
        return new BrowserBuffer(new TextEncoder().encode(str))
    }
}

export function fromUint8Array(ab: Buffer | Uint8Array): PlatformBuffer {
    if (isNodeEnv) {
        if (!Buffer.isBuffer(ab)) {
            return (Buffer.from(ab) as unknown as PlatformBuffer)
        } else {
            return ab as unknown as PlatformBuffer;
        }
    } else {
        const result = new BrowserBuffer(ab);
        return result as unknown as PlatformBuffer;
    }
}



export function alloc(size: number): PlatformBuffer {
    if (isNodeEnv) {
        return Buffer.allocUnsafe(size) as unknown as PlatformBuffer;
    } else {
        const result = BrowserBuffer.alloc(size);
        return result as PlatformBuffer;
    }
}


export function strByteLength(str: string): number {
    if (isNodeEnv) {
        return Buffer.byteLength(str);
    } else {
        let len = 0;
        let c = 0;
        for (let i = 0; i < str.length; ++i) {
            c = str.charCodeAt(i);
            if (c < 128)
                len += 1;
            else if (c < 2048)
                len += 2;
            else if ((c & 0xFC00) === 0xD800 && (str.charCodeAt(i + 1) & 0xFC00) === 0xDC00) {
                ++i;
                len += 4;
            } else
                len += 3;
        }
        return len;
    }
}
