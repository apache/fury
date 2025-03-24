abstract class FuryException extends Error{
  FuryException();

  void giveExceptionMessage(StringBuffer buf){}

  @override
  String toString() {
    final buf = StringBuffer();
    giveExceptionMessage(buf);
    return buf.toString();
  }
}