import 'package:fury_core/src/config/config.dart';

class FuryConfig extends Config{
  final int  _configId;
  final bool _xlangMode;
  //final bool _isLittleEndian;
  final bool _refTracking;
  final bool _basicTypesRefIgnored;
  final bool _timeRefIgnored;
  final bool _stringRefIgnored;

  FuryConfig.onlyForManager(
    this._configId, {
    required bool xlangMode,
    bool isLittleEndian = true,
    bool refTracking = true,
    bool basicTypesRefIgnored = true,
    bool timeRefIgnored = true,
    // bool stringRefIgnored = true,
  })
  : _xlangMode = xlangMode,
    //_isLittleEndian = isLittleEndian,
    _refTracking = refTracking,
    _basicTypesRefIgnored = basicTypesRefIgnored,
    _timeRefIgnored = timeRefIgnored,
    _stringRefIgnored = false
  {
    // some checking works
    assert(_xlangMode == true, 'currently only support xlang mode');
    //assert(_isLittleEndian == true, 'Non-Little-Endian format detected. Only Little-Endian is supported.');
  }

  //getters
  bool get xlangMode => _xlangMode;
  //bool get isLittleEndian => _isLittleEndian;
  bool get refTracking => _refTracking;
  int get configId => _configId;
  bool get basicTypesRefIgnored => _basicTypesRefIgnored;
  bool get timeRefIgnored => _timeRefIgnored;
  bool get stringRefIgnored => _stringRefIgnored;
}