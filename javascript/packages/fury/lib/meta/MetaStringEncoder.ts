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

class MetaStringEncoder {
    static LOWER_SPECIAL: number = 5;
    static LOWER_UPPER_DIGIT_SPECIAL: number = 6;

    static encode(input: string): MetaString {
        if (!input) {
            return MetaStringEncoder.createMetaString(input, 'UTF-8', '', '', new Uint8Array(0));
        }
        const encoding = MetaStringEncoder.computeEncoding(input);
        return MetaStringEncoder.encodeWithEncoding(input, encoding);
    }

    static createMetaString(input: string, encoding: string, specialChar1: string, specialChar2: string, bytes: Uint8Array): MetaString {
        return {
            input,
            encoding,
            specialChar1,
            specialChar2,
            bytes
        };
    }

    static computeEncoding(input: string): string {
        // Logic to determine the best encoding (LOWER_SPECIAL or LOWER_UPPER_DIGIT_SPECIAL)
        const lowerSpecialCount = [...input].filter(char => /[a-z._$|]/.test(char)).length;
        const lowerUpperDigitSpecialCount = [...input].filter(char => /[a-zA-Z0-9._]/.test(char)).length;

        if (lowerSpecialCount === input.length) {
            return 'LOWER_SPECIAL';
        } else if (lowerUpperDigitSpecialCount === input.length) {
            return 'LOWER_UPPER_DIGIT_SPECIAL';
        } else {
            return 'UTF-8';
        }
    }

    static encodeWithEncoding(input: string, encoding: string): MetaString {
        let bitsPerChar: number;
        if (encoding === 'LOWER_SPECIAL') {
            bitsPerChar = MetaStringEncoder.LOWER_SPECIAL;
        } else if (encoding === 'LOWER_UPPER_DIGIT_SPECIAL') {
            bitsPerChar = MetaStringEncoder.LOWER_UPPER_DIGIT_SPECIAL;
        } else {
            // For UTF-8, we can return the original string as the byte array
            return MetaStringEncoder.createMetaString(input, encoding, '', '', new TextEncoder().encode(input));
        }

        const totalBits = input.length * bitsPerChar + 1;
        const byteLength = Math.ceil(totalBits / 8);
        const bytes = new Uint8Array(byteLength);
        let currentBit = 1;

        for (let char of input) {
            const value = bitsPerChar === MetaStringEncoder.LOWER_SPECIAL
                ? MetaStringEncoder.charToValueLowerSpecial(char)
                : MetaStringEncoder.charToValueLowerUpperDigitSpecial(char);

            for (let i = bitsPerChar - 1; i >= 0; i--) {
                if ((value & (1 << i)) !== 0) {
                    const bytePos = Math.floor(currentBit / 8);
                    const bitPos = currentBit % 8;
                    bytes[bytePos] |= (1 << (7 - bitPos));
                }
                currentBit++;
            }
        }

        const stripLastChar = bytes.length * 8 >= totalBits + bitsPerChar;
        if (stripLastChar) {
            bytes[0] |= 0x80;
        }
        return MetaStringEncoder.createMetaString(input, encoding, '', '', bytes);
    }

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
        } else {
            throw new Error("Unsupported character for LOWER_SPECIAL encoding: " + char);
        }
    }

    static charToValueLowerUpperDigitSpecial(char: string): number {
        if (char >= 'a' && char <= 'z') {
            return char.charCodeAt(0) - 'a'.charCodeAt(0);
        } else if (char >= 'A' && char <= 'Z') {
            return 26 + (char.charCodeAt(0) - 'A'.charCodeAt(0));
        } else if (char >= '0' && char <= '9') {
            return 52 + (char.charCodeAt(0) - '0'.charCodeAt(0));
        } else if (char === '.' || char === '_') {
            return 62 + (char === '.' ? 0 : 1);
        } else {
            throw new Error("Unsupported character for LOWER_UPPER_DIGIT_SPECIAL encoding: " + char);
        }
    }
}

interface MetaString {
    input: string;
    encoding: string;
    specialChar1: string;
    specialChar2: string;
    bytes: Uint8Array;
}

// Example usage
const input = "helloWorld_123";
const encodedMSE = MetaStringEncoder.encode(input);
console.log(encodedMSE); // Output the encoded MetaString object
