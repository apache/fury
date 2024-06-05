class MetaStringDecoder {
    static LOWER_SPECIAL = 5;
    static LOWER_UPPER_DIGIT_SPECIAL = 6;

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
            chars.push(bitsPerChar === MetaStringDecoder.LOWER_SPECIAL
                ? MetaStringDecoder.valueToCharLowerSpecial(value)
                : MetaStringDecoder.valueToCharLowerUpperDigitSpecial(value));
        }

        return chars.join('');
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
const encoded = new Uint8Array([/* some byte array here */]);
const decoded = MetaStringDecoder.decode(encoded, MetaStringDecoder.LOWER_UPPER_DIGIT_SPECIAL);
console.log(decoded); // Output the decoded string
