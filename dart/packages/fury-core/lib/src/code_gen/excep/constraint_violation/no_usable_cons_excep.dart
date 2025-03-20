import 'package:fury_core/src/code_gen/excep/fury_gen_excep.dart';

class NoUsableConsExcep extends FuryGenExcep {
  final String libPath;
  final String className;
  final reason;

  NoUsableConsExcep(this.libPath, this.className, this.reason)
      : super('$libPath@$className');
}