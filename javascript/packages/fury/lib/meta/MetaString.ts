/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export class MetaString {
    static LOWER_SPECIAL: number = 5;
    static LOWER_UPPER_DIGIT_SPECIAL: number = 6;
    static UTF_8: number = 8;

    // Encode function that infers the bits per character
    static encode(str: string): Uint8Array {
        const bitsPerChar = MetaString.inferBitsPerChar(str);
        const totalBits = str.length * bitsPerChar + 8; // Adjusted for metadata bits
        const byteLength = Math.ceil(totalBits / 8);
        const bytes = new Uint8Array(byteLength);
        let currentBit = 8; // Start after the first 8 metadata bits

        for (const char of str) {
            const value = bitsPerChar === MetaString.LOWER_SPECIAL
                ? MetaString.charToValueLowerSpecial(char)
                : bitsPerChar === MetaString.LOWER_UPPER_DIGIT_SPECIAL
                ? MetaString.charToValueLowerUpperDigitSpecial(char)
                : MetaString.charToValueUTF8(char);

            for (let i = bitsPerChar - 1; i >= 0; i--) {
                if ((value & (1 << i)) !== 0) {
                    const bytePos = Math.floor(currentBit / 8);
                    const bitPos = currentBit % 8;

                    if (bytePos >= byteLength) {
                        throw new RangeError("Offset is outside the bounds of the DataView");
                    }
                    bytes[bytePos] |= (1 << (7 - bitPos));
                }
                currentBit++;
            }
        }

        // Store bitsPerChar in the first byte
        bytes[0] = bitsPerChar;

        return bytes;
    }

    // Decoding function that extracts bits per character from the first byte
    static decode(bytes: Uint8Array): string {
        const bitsPerChar = bytes[0] & 0x0F;
        const totalBits = (bytes.length * 8); // Adjusted for metadata bits
        const chars: string[] = [];
        let currentBit = 8; // Start after the first 8 metadata bits

        while (currentBit < totalBits) {
            let value = 0;
            for (let i = 0; i < bitsPerChar; i++) {
                const bytePos = Math.floor(currentBit / 8);
                const bitPos = currentBit % 8;

                if (bytePos >= bytes.length) {
                    throw new RangeError("Offset is outside the bounds of the DataView");
                }

                if (bytes[bytePos] & (1 << (7 - bitPos))) {
                    value |= (1 << (bitsPerChar - i - 1));
                }
                currentBit++;
            }

            chars.push(bitsPerChar === MetaString.LOWER_SPECIAL
                ? MetaString.valueToCharLowerSpecial(value)
                : bitsPerChar === MetaString.LOWER_UPPER_DIGIT_SPECIAL
                ? MetaString.valueToCharLowerUpperDigitSpecial(value)
                : MetaString.valueToCharUTF8(value));
        }

        return chars.join('');
    }

    // Infer bits per character based on the content of the string
    static inferBitsPerChar(str: string): number {
        if (/^[a-z._$|]+$/.test(str)) {
            return MetaString.LOWER_SPECIAL;
        } else if (/^[a-zA-Z0-9._]+$/.test(str)) {
            return MetaString.LOWER_UPPER_DIGIT_SPECIAL;
        }
        return MetaString.UTF_8; // Default to UTF-8
    }

    // Convert a character to its value for LOWER_SPECIAL encoding
    static charToValueLowerSpecial(char: string): number {
        if (char >= 'a' && char <= 'z') {
            return char.charCodeAt(0) - 'a'.charCodeAt(0);
        } else if (char === '.') {
            return 26;
        } else if (char === '_') {
            return 27;
        } else if (char === '$') {
            return 28;
        } else if (char === '|') {
            return 29;
        }
        throw new Error(`Invalid character for LOWER_SPECIAL: ${char}`);
    }

    static valueToCharLowerSpecial(value: number): string {
        if (value >= 0 && value <= 25) {
            return String.fromCharCode('a'.charCodeAt(0) + value);
        } else if (value === 26) {
            return '.';
        } else if (value === 27) {
            return '_';
        } else if (value === 28) {
            return '$';
        } else if (value === 29) {
            return '|';
        }
        throw new Error(`Invalid value for LOWER_SPECIAL: ${value}`);
    }

    static charToValueLowerUpperDigitSpecial(char: string): number {
        if (char >= 'a' && char <= 'z') {
            return char.charCodeAt(0) - 'a'.charCodeAt(0);
        } else if (char >= 'A' && char <= 'Z') {
            return char.charCodeAt(0) - 'A'.charCodeAt(0) + 26;
        } else if (char >= '0' && char <= '9') {
            return char.charCodeAt(0) - '0'.charCodeAt(0) + 52;
        } else if (char === '.') {
            return 62;
        } else if (char === '_') {
            return 63;
        }
        throw new Error(`Invalid character for LOWER_UPPER_DIGIT_SPECIAL: ${char}`);
    }

    static valueToCharLowerUpperDigitSpecial(value: number): string {
        if (value >= 0 && value <= 25) {
            return String.fromCharCode('a'.charCodeAt(0) + value);
        } else if (value >= 26 && value <= 51) {
            return String.fromCharCode('A'.charCodeAt(0) + value - 26);
        } else if (value >= 52 && value <= 61) {
            return String.fromCharCode('0'.charCodeAt(0) + value - 52);
        } else if (value === 62) {
            return '.';
        } else if (value === 63) {
            return '_';
        }
        throw new Error(`Invalid value for LOWER_UPPER_DIGIT_SPECIAL: ${value}`);
    }

    static charToValueUTF8(char: string): number {
        return char.charCodeAt(0);
    }

    static valueToCharUTF8(value: number): string {
        return String.fromCharCode(value);
    }
}
