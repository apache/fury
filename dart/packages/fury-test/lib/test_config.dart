import 'dart:io';

import 'package:yaml/yaml.dart';

class TestConfig {
  static const String configName = 'test_config.yaml';
  static final TestConfig I = _loadConfig();
  final String pythonExecutable;

  TestConfig({required this.pythonExecutable});

  static TestConfig _loadConfig() {
    final String configPath = '${Directory.current.path}/$configName';
    final File configFile = File(configPath);
    if (!configFile.existsSync()) {
      throw ArgumentError('Config file not found: $configPath');
    }
    final config = loadYaml(configFile.readAsStringSync());
    return TestConfig(
      pythonExecutable: (config['python'] ? ['executable'] as String?) ?? 'python',
    );
  }
}
