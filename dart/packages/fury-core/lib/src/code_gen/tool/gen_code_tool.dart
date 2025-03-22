class GenCodeTool{
  // static void writeIndentWithLevel(StringBuffer buf, int indentLevel){
  //   int totalIndent = indentLevel * 2;
  //   for (int i = 0; i < totalIndent; ++i){
  //     buf.write(' ');
  //   }
  // }
  static const String _2Spaces = '  ';
  static const String _4Spaces = _2Spaces + _2Spaces;
  static const String _6Spaces = _4Spaces + _2Spaces;
  static const String _8Spaces = _6Spaces + _2Spaces;
  static const String _10Spaces = _8Spaces + _2Spaces;
  static const String _12Spaces = _10Spaces + _2Spaces;
  static const String _14Spaces = _12Spaces + _2Spaces;
  static const String _16Spaces = _14Spaces + _2Spaces;
  static const String _18Spaces = _16Spaces + _2Spaces;
  
  static const List<String> _spaces = [
    _2Spaces,
    _4Spaces,
    _6Spaces,
    _8Spaces,
    _10Spaces,
    _12Spaces,
    _14Spaces,
    _16Spaces,
    _18Spaces,
  ];

  static const List<int> _spacesNum = [2, 4, 6, 8, 10, 12, 14, 16, 18];

  /// 不采用直接循环写入空格的方式，而是采用二分查找的方式，尽量减少写入的次数
  static void writeIndent(StringBuffer buf, int indent){
    int low = 0;
    int high = _spacesNum.length - 1;
    int result = -1;
    while (low <= high) {
      int mid = low + ((high - low) >> 1); // 避免溢出
      if (_spacesNum[mid] < indent) {
        // 当前元素小于目标，记录候选位置，继续向右找更接近的
        result = mid;
        low = mid + 1;
      } else if (_spacesNum[mid] > indent) {
        // 当前元素 >= 目标，向左缩小范围
        high = mid - 1;
      }else{
        // 当前元素 == 目标，直接返回
        buf.write(_spaces[mid]);
        return;
      }
    }
    if (result != -1) {
      buf.write(_spaces[result]);
    }
    indent -= (result == -1 ? 0 : _spacesNum[result]);
    for (int i = 0; i < indent; ++i){
      buf.write(' ');
    }
  }
}