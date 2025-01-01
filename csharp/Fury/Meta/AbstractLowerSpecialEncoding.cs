using System;

namespace Fury.Meta;

internal abstract class AbstractLowerSpecialEncoding(MetaString.Encoding encoding)
    : MetaStringEncoding(encoding)
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

    protected static bool TryEncodeCharToByte(char c, out byte b)
    {
        (var success, b) = c switch
        {
            >= 'a' and <= 'z' => (true, (byte)(c - 'a')),
            '.' => (true, NumberOfEnglishLetters),
            '_' => (true, NumberOfEnglishLetters + 1),
            '$' => (true, NumberOfEnglishLetters + 2),
            '|' => (true, NumberOfEnglishLetters + 3),
            _ => (false, default)
        };
        return success;
    }

    protected static bool TryDecodeByteToChar(byte b, out char c)
    {
        (var success, c) = b switch
        {
            < NumberOfEnglishLetters => (true, (char)(b + 'a')),
            NumberOfEnglishLetters => (true, '.'),
            NumberOfEnglishLetters + 1 => (true, '_'),
            NumberOfEnglishLetters + 2 => (true, '$'),
            NumberOfEnglishLetters + 3 => (true, '|'),
            _ => (false, default)
        };
        return success;
    }
}
