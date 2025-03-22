// import 'package:fury_core/fury_core.dart';
// import 'package:fury_core/src/code_gen/excep/single_field_excep.dart';
// import 'package:fury_core/src/const/dart_type.dart';
// import 'package:meta/meta.dart';
//
// class IncompatibleTypeMappingException extends SingleFieldExcep {
//   final FuryType specifiedType;
//   final List<DartTypeEnum> possibleDartTypes;
//   final bool toOrFrom;
//
//   IncompatibleTypeMappingException(
//     super._libPath,
//     super._className,
//     super._fieldName,
//     this.toOrFrom,
//     this.specifiedType,
//     this.possibleDartTypes, [
//     super.where,
//   ]);
//
//   @mustCallSuper
//   @override
//   void giveExcepMsg(StringBuffer buf) {
//     super.giveExcepMsg(buf);
//     buf.write('your specified type');
//     buf.write(toOrFrom ? '(serializeTo):' : '(deserializeFrom):');
//     buf.write(specifiedType);
//     buf.write('\n');
//     buf.write('possibleDartTypes: ');
//     buf.writeAll(
//       possibleDartTypes.map((d)=>d.fullSign), ', '
//     );
//     buf.write('\n');
//   }
//
//   @override
//   String toString() {
//     final buf = StringBuffer();
//     giveExcepMsg(buf);
//     return buf.toString();
//   }
// }