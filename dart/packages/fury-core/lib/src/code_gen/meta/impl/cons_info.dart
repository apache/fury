import 'package:fury_core/src/code_gen/entity/contructor_params.dart';

class ConsInfo {
  final bool flexibleOrUnnamedCons;
  final String? flexibleConsName;
  final ConstructorParams? unnamedConsParams;

  const ConsInfo.useFlexibleCons(
    this.flexibleConsName,
  ) : unnamedConsParams = null,
       flexibleOrUnnamedCons = true;

  const ConsInfo.useUnnamedCons(
    this.unnamedConsParams,
  ) : flexibleConsName = null,
       flexibleOrUnnamedCons = false;
}