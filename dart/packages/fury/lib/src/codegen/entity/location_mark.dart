import 'package:fury/src/codegen/const/location_level.dart';
import 'package:meta/meta.dart';

@immutable
class LocationMark {
  final String libPath;
  final String clsName;
  final String? fieldName;
  final LocationLevel _level;
  
  LocationMark._(
    this.libPath,
    this.clsName,
    this.fieldName,
    this._level,
  );

  LocationMark.clsLevel(this.libPath, this.clsName)
      : fieldName = null,
        _level = LocationLevel.clsLevel;

  LocationMark.fieldLevel(this.libPath, this.clsName, this.fieldName)
      : _level = LocationLevel.fieldLevel;

  bool get ensureFieldLevel => _level.index >= LocationLevel.fieldLevel.index;
  bool get ensureClassLevel => _level.index >= LocationLevel.clsLevel.index;
  
  String get clsLocation => '$libPath@$clsName';
  
  LocationMark copyWithFieldName(String fieldName) {
    return LocationMark._(libPath, clsName, fieldName, LocationLevel.fieldLevel);
  }
}