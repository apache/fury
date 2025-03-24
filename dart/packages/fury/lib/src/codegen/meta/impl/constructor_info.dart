import 'package:fury/src/codegen/entity/contructor_params.dart';

class ConstructorInfo {
  final bool flexibleOrUnnamedCons;
  final String? flexibleConsName;
  final ConstructorParams? unnamedConsParams;

  const ConstructorInfo.useFlexibleCons(
    this.flexibleConsName,
  ) : unnamedConsParams = null,
       flexibleOrUnnamedCons = true;

  const ConstructorInfo.useUnnamedCons(
    this.unnamedConsParams,
  ) : flexibleConsName = null,
       flexibleOrUnnamedCons = false;
}