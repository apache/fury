final class StrStat{
  final int digitCount;
  final int upperCount;
  final bool canLUDS; // LowerUpperDigitSpecial
  final bool canLS; // LowerSpecial

  const StrStat(
    this.digitCount,
    this.upperCount,
    this.canLUDS,
    this.canLS,
  );
}