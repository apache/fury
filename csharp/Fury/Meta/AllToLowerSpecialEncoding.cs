using System;

namespace Fury.Meta;

internal sealed class AllToLowerSpecialEncoding()
    : AbstractLowerSpecialEncoding(MetaString.Encoding.AllToLowerSpecial)
{
    public static readonly AllToLowerSpecialEncoding Instance = new();

    private const char UpperCaseFlag = '|';
    private static readonly byte EncodedUpperCaseFlag;

    static AllToLowerSpecialEncoding()
    {
        TryEncodeCharToByte(UpperCaseFlag, out EncodedUpperCaseFlag);
    }

    public override bool CanEncode(ReadOnlySpan<char> chars)
    {
        foreach (var t in chars)
        {
            var c = char.ToLowerInvariant(t);
            if (!TryEncodeCharToByte(c, out _))
            {
                return false;
            }
        }

        return true;
    }

    private static (int byteCount, bool stripLastChar) GetByteAndStripLastChar(ReadOnlySpan<char> chars)
    {
        var charCount = chars.Length;
        foreach (var c in chars)
        {
            if (char.IsUpper(c))
            {
                charCount++;
            }
        }

        return GetByteAndStripLastChar(charCount);
    }

    public override int GetMaxByteCount(int charCount)
    {
        return base.GetMaxByteCount(charCount * 2);
    }

    public override int GetByteCount(ReadOnlySpan<char> chars) => GetByteAndStripLastChar(chars).byteCount;

    public override int GetCharCount(ReadOnlySpan<byte> bytes)
    {
        var stripLastChar = (bytes[0] & 0x80) != 0;
        var countWithUpperCaseFlag = base.GetCharCount(bytes);
        var charCount = countWithUpperCaseFlag - (stripLastChar ? 1 : 0);
        var currentBit = 1;
        for (var i = 0; i < countWithUpperCaseFlag; i++)
        {
            var c = ReadChar(bytes, currentBit);
            if (c == UpperCaseFlag)
            {
                charCount--;
            }
            currentBit += BitsPerChar;
        }

        return charCount;
    }

    public override int GetBytes(ReadOnlySpan<char> chars, Span<byte> bytes)
    {
        var (byteCount, stripLastChar) = GetByteAndStripLastChar(chars);
        if (bytes.Length < byteCount)
        {
            ThrowHelper.ThrowArgumentException(nameof(bytes));
        }
        var currentBit = 1;
        foreach (var t in chars)
        {
            var c = t;
            if (char.IsUpper(c))
            {
                WriteBits(bytes, EncodedUpperCaseFlag, currentBit);
                currentBit += BitsPerChar;
                c = char.ToLowerInvariant(c);
            }
            if (!TryEncodeCharToByte(c, out var v))
            {
                ThrowHelper.ThrowArgumentOutOfRangeException(nameof(chars), chars.ToString());
            }
            WriteBits(bytes, v, currentBit);
            currentBit += BitsPerChar;
        }

        if (stripLastChar)
        {
            bytes[0] |= 0x80;
        }

        return byteCount;
    }

    private static void WriteBits(Span<byte> bytes, byte v, int currentBit)
    {
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
    }

    public override int GetChars(ReadOnlySpan<byte> bytes, Span<char> chars)
    {
        var countWithUpperCaseFlag = base.GetCharCount(bytes);
        if (chars.Length < countWithUpperCaseFlag)
        {
            ThrowHelper.ThrowArgumentException(nameof(chars));
        }

        for (var i = 0; i < countWithUpperCaseFlag; i++)
        {
            var currentBit = i * BitsPerChar + 1;
            var c = ReadChar(bytes, currentBit);
            if (c == UpperCaseFlag)
            {
                i++;
                c = ReadChar(bytes, currentBit + BitsPerChar);
                c = char.ToUpperInvariant(c);
            }
            chars[i] = c;
        }

        return countWithUpperCaseFlag;
    }

    private static char ReadChar(ReadOnlySpan<byte> bytes, int currentBit)
    {
        const byte bitMask = MaxRepresentableChar;
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

        if (!TryDecodeByteToChar(charByte, out var c))
        {
            ThrowHelper.ThrowArgumentOutOfRangeException(nameof(bytes));
        }

        return c;
    }
}
