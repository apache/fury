import 'package:fury_core/src/code_gen/excep/meta_spec/meta_spec_excep.dart';
import 'package:fury_core/src/const/meta_string_const.dart';

class ClassTagInvalidExcep extends MetaSpecExcep{

  final List<String>? _classesWithEmptyTag;
  final List<String>? _classesWithTooLongTag;
  final Map<String, List<String>>? _repeatedTags;

  ClassTagInvalidExcep(this._classesWithEmptyTag, this._classesWithTooLongTag, this._repeatedTags, [super._where]){
    assert(_classesWithEmptyTag != null || _repeatedTags != null || _classesWithTooLongTag != null);
  }

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    if (_classesWithEmptyTag != null) {
      buf.write('Classes with empty tag:');
      buf.writeAll(_classesWithEmptyTag, ', ');
      buf.write('\n');
    }

    if (_classesWithTooLongTag != null) {
      buf.write('Classes with too long tag (should be less than ');
      buf.write(MetaStringConst.metaStrMaxLen);
      buf.write('):');
      buf.writeAll(_classesWithTooLongTag, ', ');
      buf.write('\n');
    }

    if (_repeatedTags != null) {
      buf.write('Classes with repeated tags:');
      for (String c in _repeatedTags.keys) {
        buf.write(c);
        buf.write(': ');
        buf.writeAll(_repeatedTags[c]!, ', ');
        buf.write('\n');
      }
    }
  }


  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}