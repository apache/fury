class FuryHeaderConst{
  static const int magicNumber = 0x62d4;

  static const int nullFlag = 1;
  static const int littleEndianFlag = 1 << 1;
  static const int crossLanguageFlag = 1 << 2;
  static const int outOfBandFlag = 1 << 3;
}