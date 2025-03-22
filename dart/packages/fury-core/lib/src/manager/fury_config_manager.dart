import 'package:fury_core/src/config/fury_config.dart';

class FuryConfigManager{
  // singleton
  static final FuryConfigManager _instance = FuryConfigManager._();
  static FuryConfigManager get inst => _instance;
  FuryConfigManager._();

  int configId = 0;
  int get nextConfigId => configId++;

  FuryConfig createConfig({
    required bool xlangMode,
    bool isLittleEndian = true,
    bool refTracking = true,
    bool basicTypesRefIgnored = true,
    bool timeRefIgnored = true,
    // bool stringRefIgnored = true,
  }) {
    return FuryConfig.onlyForManager(
      nextConfigId,
      xlangMode: xlangMode,
      isLittleEndian: isLittleEndian,
      refTracking: refTracking,
      basicTypesRefIgnored: basicTypesRefIgnored,
      timeRefIgnored: timeRefIgnored,
      // stringRefIgnored: stringRefIgnored,
    );
  }
}
