class NamingStyleUtil{

  // 执行这个函数的保证就是str是lowerCamelCase风格的字符串, 所以不要考虑哪些str的不合规情况
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
        // 大写字母
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