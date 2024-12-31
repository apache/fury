using System;

namespace Fury.Meta;

internal abstract class AbstractLowerSpecialEncoding(char specialChar1, char specialChar2, MetaString.Encoding encoding)
    : MetaStringEncoding(specialChar1, specialChar2, encoding)
{
    protected const int BitsPerChar = 5;
    protected const int UnusedBitsPerChar = BitsOfByte - BitsPerChar;
    protected const int MaxRepresentableChar = (1 << BitsPerChar) - 1;

    protected static (int byteCount, bool stripLastChar) GetByteAndStripLastChar(int charCount)
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

    protected static byte EncodeCharToByte(char c)
    {
        return c switch
        {
            >= 'a' and <= 'z' => (byte)(c - 'a'),
            '.' => NumberOfEnglishLetters,
            '_' => NumberOfEnglishLetters + 1,
            '$' => NumberOfEnglishLetters + 2,
            '|' => NumberOfEnglishLetters + 3,
            _ => ThrowHelper.ThrowArgumentException<byte>(nameof(c)),
        };
    }

    protected static char DecodeByteToChar(byte b)
    {
        return b switch
        {
            < NumberOfEnglishLetters => (char)(b + 'a'),
            NumberOfEnglishLetters => '.',
            NumberOfEnglishLetters + 1 => '_',
            NumberOfEnglishLetters + 2 => '$',
            NumberOfEnglishLetters + 3 => '|',
            _ => ThrowHelper.ThrowArgumentException<char>(nameof(b)),
        };
    }
}
