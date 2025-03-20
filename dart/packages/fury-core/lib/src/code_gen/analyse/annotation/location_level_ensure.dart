import 'package:fury_core/src/code_gen/const/location_level.dart';
import 'package:meta/meta_meta.dart';

@Target({TargetKind.parameter})
class LocationEnsure{
  final LocationLevel locationLevel;
  const LocationEnsure(this.locationLevel);
}