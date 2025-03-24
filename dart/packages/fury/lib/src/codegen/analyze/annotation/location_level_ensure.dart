import 'package:meta/meta_meta.dart';
import 'package:fury/src/codegen/const/location_level.dart';

@Target({TargetKind.parameter})
class LocationEnsure{
  final LocationLevel locationLevel;
  const LocationEnsure(this.locationLevel);
}