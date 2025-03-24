class NamingStyleUtil{

  // The guarantee of executing this function is that str is a lowerCamelCase style string, so do not consider non-compliant cases of str
  static String lowerCamelToLowerUnderscore(String str) {
    // return str.replaceAllMapped(
    //   RegExp(r'([a-z])([A-Z])'),
    //   (Match m) => '${m[1]}_${m[2]!.toLowerCase()}',
    // ).toLowerCase();
    StringBuffer buf = StringBuffer();
    int len = str.length;
    int fromIndex = 0;
    for (int i = 0; i< len; ++i){
      int codeUnit = str.codeUnitAt(i);
      if (codeUnit >= 65 && codeUnit <= 90){
        // Uppercase letter
        buf.write(str.substring(fromIndex, i));
        buf.write("_");
        buf.write(str[i].toLowerCase());
        fromIndex = i + 1;
      }
    }
    if (fromIndex < len){
      buf.write(str.substring(fromIndex, len));
    }
    return buf.toString();
  }
}
