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

class MetaStringDecoder {
    static LOWER_SPECIAL: number = 5;
    static LOWER_UPPER_DIGIT_SPECIAL: number = 6;

    // Decoding function from the encoded bytes
    static decode(bytes: Uint8Array, bitsPerChar: number): string {
        const totalBits = bytes.length * 8;
        const chars: string[] = [];
        let currentBit = 1;

        while (currentBit < totalBits) {
            let value = 0;
            for (let i = 0; i < bitsPerChar; i++) {
                const bytePos = Math.floor(currentBit / 8);
                const bitPos = currentBit % 8;
                if (bytes[bytePos] & (1 << (7 - bitPos))) {
                    value |= (1 << (bitsPerChar - i - 1));
                }
                currentBit++;
            }
            chars.push(bitsPerChar === MetaStringDecoder.LOWER_SPECIAL
                ? MetaStringDecoder.valueToCharLowerSpecial(value)
                : MetaStringDecoder.valueToCharLowerUpperDigitSpecial(value));
        }

        return chars.join('');
    }

    // Convert a value to its character for LOWER_SPECIAL encoding
    static valueToCharLowerSpecial(value: number): string {
        if (value >= 0 && value <= 25) {
            return String.fromCharCode(value + 'a'.charCodeAt(0));
        } else if (value === 26) {
            return '.';
        } else if (value === 27) {
            return '_';
        } else if (value === 28) {
            return '$';
        } else if (value === 29) {
            return '|';
        } else {
            throw new Error("Unsupported value for LOWER_SPECIAL decoding: " + value);
        }
    }

    // Convert a value to its character for LOWER_UPPER_DIGIT_SPECIAL encoding
    static valueToCharLowerUpperDigitSpecial(value: number): string {
        if (value >= 0 && value <= 25) {
            return String.fromCharCode(value + 'a'.charCodeAt(0));
        } else if (value >= 26 && value <= 51) {
            return String.fromCharCode(value - 26 + 'A'.charCodeAt(0));
        } else if (value >= 52 && value <= 61) {
            return String.fromCharCode(value - 52 + '0'.charCodeAt(0));
        } else if (value === 62) {
            return '.';
        } else if (value === 63) {
            return '_';
        } else {
            throw new Error("Unsupported value for LOWER_UPPER_DIGIT_SPECIAL decoding: " + value);
        }
    }
}

// Example usage
const encoded = new Uint8Array([/* some byte array here */]);
const decoded = MetaStringDecoder.decode(encoded, MetaStringDecoder.LOWER_UPPER_DIGIT_SPECIAL);
console.log(decoded); // Output the decoded string
