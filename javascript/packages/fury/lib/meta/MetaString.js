class MetaString {
    static LOWER_SPECIAL = 5;
    static LOWER_UPPER_DIGIT_SPECIAL = 6;

    // Encode function based on the specified bits per character
    static encode(str, bitsPerChar) {
        const totalBits = str.length * bitsPerChar + 1;
        const byteLength = Math.ceil(totalBits / 8);
        const bytes = new Uint8Array(byteLength);
        let currentBit = 1;

        for (let char of str) {
            const value = bitsPerChar === MetaString.LOWER_SPECIAL
                ? MetaString.charToValueLowerSpecial(char)
                : MetaString.charToValueLowerUpperDigitSpecial(char);

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
        return bytes;
    }

    // Decoding function from the encoded bytes
    static decode(bytes, bitsPerChar) {
        const totalBits = bytes.length * 8;
        const chars = [];
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
            chars.push(bitsPerChar === MetaString.LOWER_SPECIAL
                ? MetaString.valueToCharLowerSpecial(value)
                : MetaString.valueToCharLowerUpperDigitSpecial(value));
        }

        return chars.join('');
    }

    // Convert a character to its value for LOWER_SPECIAL encoding
    static charToValueLowerSpecial(char) {
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

    // Convert a character to its value for LOWER_UPPER_DIGIT_SPECIAL encoding
    static charToValueLowerUpperDigitSpecial(char) {
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

    // Convert a value to its character for LOWER_SPECIAL encoding
    static valueToCharLowerSpecial(value) {
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
    static valueToCharLowerUpperDigitSpecial(value) {
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
const encoded = MetaString.encode("helloWorld_123", MetaString.LOWER_UPPER_DIGIT_SPECIAL);
console.log(encoded);
const decoded = MetaString.decode(encoded, MetaString.LOWER_UPPER_DIGIT_SPECIAL);
console.log(decoded); // Output: "helloWorld_123"
