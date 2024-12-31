using System;

namespace Fury.Meta;

internal sealed class FirstToLowerSpecialEncoding(char specialChar1, char specialChar2)
    : AbstractLowerSpecialEncoding(specialChar1, specialChar2, MetaString.Encoding.FirstToLowerSpecial)
{
    public override int GetBytes(ReadOnlySpan<char> chars, Span<byte> bytes)
    {
        var (byteCount, stripLastChar) = GetByteAndStripLastChar(chars.Length);
        if (bytes.Length < byteCount)
        {
            ThrowHelper.ThrowArgumentException(nameof(bytes));
        }
        var currentBit = 1;
        if (chars.Length > 0)
        {
            var firstChar = chars[0];
            firstChar = char.ToLowerInvariant(firstChar);
            var v = EncodeCharToByte(firstChar);
            var byteIndex = currentBit / BitsOfByte;
            var bitOffset = currentBit % BitsOfByte;
            // bitOffset locations   write locations
            // _ x _ _ _ _ _ _       _ x x x x x _ _

            bytes[byteIndex] |= (byte)(v << (UnusedBitsPerChar - bitOffset));
            currentBit += BitsPerChar;
        }
        foreach (var c in chars)
        {
            var v = EncodeCharToByte(c);
            var byteIndex = currentBit / BitsOfByte;
            var bitOffset = currentBit % BitsOfByte;
            if (bitOffset <= UnusedBitsPerChar)
            {
                // bitOffset locations   write locations
                // x _ _ _ _ _ _ _       x x x x x _ _ _
                // _ x _ _ _ _ _ _       _ x x x x x _ _
                // _ _ x _ _ _ _ _       _ _ x x x x x _
                // _ _ _ x _ _ _ _       _ _ _ x x x x x

                bytes[byteIndex] |= (byte)(v << (UnusedBitsPerChar - bitOffset));
            }
            else
            {
                // bitOffset locations   write locations
                // _ _ _ _ x _ _ _       _ _ _ _ x x x x | x _ _ _ _ _ _ _
                // _ _ _ _ _ x _ _       _ _ _ _ _ x x x | x x _ _ _ _ _ _
                // _ _ _ _ _ _ x _       _ _ _ _ _ _ x x | x x x _ _ _ _ _
                // _ _ _ _ _ _ _ x       _ _ _ _ _ _ _ x | x x x x _ _ _ _

                bytes[byteIndex] |= (byte)(v >>> (bitOffset - UnusedBitsPerChar));
                bytes[byteIndex + 1] |= (byte)(v << (BitsOfByte + UnusedBitsPerChar - bitOffset));
            }
            currentBit += BitsPerChar;
        }

        if (stripLastChar)
        {
            bytes[0] |= 0x80;
        }

        return byteCount;
    }

    public override int GetChars(ReadOnlySpan<byte> bytes, Span<char> chars)
    {
        const byte bitMask = MaxRepresentableChar;

        var charCount = GetCharCount(bytes);
        if (chars.Length < charCount)
        {
            ThrowHelper.ThrowArgumentException(nameof(chars));
        }
        for (var i = 0; i < charCount; i++)
        {
            var currentBit = i * BitsPerChar + 1;
            var byteIndex = currentBit / BitsOfByte;
            var bitOffset = currentBit % BitsOfByte;

            byte charByte;
            if (bitOffset <= UnusedBitsPerChar)
            {
                // bitOffset locations   read locations
                // x _ _ _ _ _ _ _       x x x x x _ _ _
                // _ x _ _ _ _ _ _       _ x x x x x _ _
                // _ _ x _ _ _ _ _       _ _ x x x x x _
                // _ _ _ x _ _ _ _       _ _ _ x x x x x

                charByte = (byte)((bytes[byteIndex] >>> (UnusedBitsPerChar - bitOffset)) & bitMask);
            }
            else
            {
                // bitOffset locations   read locations
                // _ _ _ _ x _ _ _       _ _ _ _ x x x x | x _ _ _ _ _ _ _
                // _ _ _ _ _ x _ _       _ _ _ _ _ x x x | x x _ _ _ _ _ _
                // _ _ _ _ _ _ x _       _ _ _ _ _ _ x x | x x x _ _ _ _ _
                // _ _ _ _ _ _ _ x       _ _ _ _ _ _ _ x | x x x x _ _ _ _

                charByte = (byte)(
                    (
                        bytes[byteIndex] << (bitOffset - (UnusedBitsPerChar))
                        | bytes[byteIndex + 1] >>> (BitsOfByte + UnusedBitsPerChar - bitOffset)
                    ) & bitMask
                );
            }

            chars[i] = DecodeByteToChar(charByte);
        }

        if (chars.Length > 0)
        {
            chars[0] = char.ToUpperInvariant(chars[0]);
        }

        return charCount;
    }
}
