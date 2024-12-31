using System;

namespace Fury.Meta;

internal sealed class LowerUpperDigitSpecialEncoding(char specialChar1, char specialChar2)
    : MetaStringEncoding(specialChar1, specialChar2, MetaString.Encoding.LowerUpperDigitSpecial)
{
    private const int BitsPerChar = 6;
    private const int UnusedBitsPerChar = BitsOfByte - BitsPerChar;
    private const int MaxRepresentableChar = (1 << BitsPerChar) - 1;

    private static (int byteCount, bool stripLastChar) GetByteAndStripLastChar(int charCount)
    {
        var totalBits = charCount * BitsPerChar + 1;
        var byteLength = (totalBits + (BitsOfByte - 1)) / BitsOfByte;
        var stripLastChar = byteLength * BitsOfByte >= totalBits * BitsPerChar;
        return (byteLength, stripLastChar);
    }

    public override int GetMaxByteCount(int charCount) => GetByteAndStripLastChar(charCount).byteCount;

    public override int GetMaxCharCount(int byteCount) => (byteCount * BitsOfByte - 1) / BitsPerChar;

    public override int GetByteCount(ReadOnlySpan<char> chars) => GetMaxByteCount(chars.Length);

    public override int GetCharCount(ReadOnlySpan<byte> bytes)
    {
        var stripLastChar = (bytes[0] & 0x80) != 0;
        return GetMaxCharCount(bytes.Length) - (stripLastChar ? 1 : 0);
    }

    public override int GetBytes(ReadOnlySpan<char> chars, Span<byte> bytes)
    {
        var (byteCount, stripLastChar) = GetByteAndStripLastChar(chars.Length);
        if (bytes.Length < byteCount)
        {
            ThrowHelper.ThrowArgumentException(nameof(bytes));
        }
        var currentBit = 1;
        foreach (var c in chars)
        {
            var v = EncodeCharToByte(c);
            var byteIndex = currentBit / BitsOfByte;
            var bitOffset = currentBit % BitsOfByte;
            if (bitOffset <= UnusedBitsPerChar)
            {
                // bitOffset locations   write locations
                // x _ _ _ _ _ _ _       x x x x x x _ _
                // _ x _ _ _ _ _ _       _ x x x x x x _
                // _ _ x _ _ _ _ _       _ _ x x x x x x

                bytes[byteIndex] |= (byte)(v << (UnusedBitsPerChar - bitOffset));
            }
            else
            {
                // bitOffset locations   write locations
                // _ _ _ x _ _ _ _       _ _ _ x x x x x | x _ _ _ _ _ _ _
                // _ _ _ _ x _ _ _       _ _ _ _ x x x x | x x _ _ _ _ _ _
                // _ _ _ _ _ x _ _       _ _ _ _ _ x x x | x x x _ _ _ _ _
                // _ _ _ _ _ _ x _       _ _ _ _ _ _ x x | x x x x _ _ _ _
                // _ _ _ _ _ _ _ x       _ _ _ _ _ _ _ x | x x x x x _ _ _

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
                // x _ _ _ _ _ _ _       x x x x x x _ _
                // _ x _ _ _ _ _ _       _ x x x x x x _
                // _ _ x _ _ _ _ _       _ _ x x x x x x

                charByte = (byte)((bytes[byteIndex] >>> (UnusedBitsPerChar - bitOffset)) & bitMask);
            }
            else
            {
                // bitOffset locations   read locations
                // _ _ _ x _ _ _ _       _ _ _ x x x x x | x _ _ _ _ _ _ _
                // _ _ _ _ x _ _ _       _ _ _ _ x x x x | x x _ _ _ _ _ _
                // _ _ _ _ _ x _ _       _ _ _ _ _ x x x | x x x _ _ _ _ _
                // _ _ _ _ _ _ x _       _ _ _ _ _ _ x x | x x x x _ _ _ _
                // _ _ _ _ _ _ _ x       _ _ _ _ _ _ _ x | x x x x x _ _ _

                charByte = (byte)(
                    (
                        bytes[byteIndex] << (bitOffset - UnusedBitsPerChar)
                        | bytes[byteIndex + 1] >>> (BitsOfByte + UnusedBitsPerChar - bitOffset)
                    ) & bitMask
                );
            }

            chars[i] = DecodeByteToChar(charByte);
        }

        return charCount;
    }

    private byte EncodeCharToByte(char c)
    {
        byte v;
        if (c == SpecialChar1)
        {
            v = MaxRepresentableChar - 1;
        }
        else if (c == SpecialChar2)
        {
            v = MaxRepresentableChar;
        }
        else
        {
            v = c switch
            {
                >= 'a' and <= 'z' => (byte)(c - 'a'),
                >= 'A' and <= 'Z' => (byte)(c - 'A' + NumberOfEnglishLetters),
                >= '0' and <= '9' => (byte)(c - '0' + NumberOfEnglishLetters * 2),
                _ => ThrowHelper.ThrowArgumentException<byte>(nameof(c)),
            };
        }

        return v;
    }

    private char DecodeByteToChar(byte b)
    {
        var c = b switch
        {
            < NumberOfEnglishLetters => (char)(b + 'a'),
            < NumberOfEnglishLetters * 2 => (char)(b - NumberOfEnglishLetters + 'A'),
            < NumberOfEnglishLetters * 2 + 10 => (char)(b - NumberOfEnglishLetters * 2 + '0'),
            MaxRepresentableChar - 1 => SpecialChar1,
            MaxRepresentableChar => SpecialChar2,
            _ => ThrowHelper.ThrowArgumentException<char>(nameof(b)),
        };

        return c;
    }
}
