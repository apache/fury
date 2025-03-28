import 'package:fury/src/codegen/meta/impl/field_spec_immutable.dart';
import 'package:fury/src/util/string_util.dart';

class FieldsSorter{
  const FieldsSorter();

  void sortFieldsByName(List<FieldSpecImmutable> fields){
    for (int i = 0; i < fields.length; ++i){
      fields[i].transName ??= StringUtil.lowerCamelToLowerUnderscore(fields[i].name);
    }
    fields.sort(
      (a, b) {
        return a.transName!.compareTo(b.transName!);
      },
    );
  }
}