/// Language supported by fury.
enum Language{
  XLANG,
  JAVA,
  PYTHON,
  CPP,
  GO,
  JAVASCRIPT,
  RUST,
  DART;

  static int get peerLangBeginIndex => Language.JAVA.index;
  static int get peerLangEndIndex => Language.DART.index;
}