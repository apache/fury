import '../../../../util/naming_style_util.dart';
import '../../../meta/impl/field_spec_immutable.dart';

class FieldsSorter{

  const FieldsSorter();

  void sortFieldsByName(List<FieldSpecImmutable> fields){
    for (int i = 0; i < fields.length; ++i){
      fields[i].transName ??= NamingStyleUtil.lowerCamelToLowerUnderscore(fields[i].name);
    }
    fields.sort(
      (a, b) {
        return a.transName!.compareTo(b.transName!);
      },
    );
  }
}