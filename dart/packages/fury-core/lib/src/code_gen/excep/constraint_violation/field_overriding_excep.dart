import 'package:fury_core/src/code_gen/excep/field_excep.dart';
import 'package:fury_core/src/code_gen/rules/code_rules.dart';

class FieldOverridingException extends FieldExcep{
  FieldOverridingException(
    String libPath,
    String className,
    List<String> invalidFields,
    [String? where]
  ):
  super(libPath, className, invalidFields, CodeRules.unsupportFieldOverriding, where);
}