import 'dart:io';

/// 执行命令行命令
///
/// [command] 要执行的命令列表
/// [waitTimeoutSeconds] 等待命令执行完成的超时时间（秒）
/// [env] 环境变量
///
/// 返回命令执行是否成功（退出码为0）
///
class TestProcessUtil {
  /// 同步执行命令行命令
  static bool executeCommandSync(List<String> command, int timeoutSec, Map<String, String> env) {
    try {
      print('Executing command synchronously: ${command.join(' ')}');
      // 使用同步API执行命令
      ProcessResult result = Process.runSync(
        command.first,
        command.skip(1).toList(),
        environment: env,
        runInShell: true,
      );

      // 输出结果
      stdout.write(result.stdout);
      stderr.write(result.stderr);
      print("exitCode: ${result.exitCode}");

      return result.exitCode == 0;
    } catch (e) {
      print("Error: $e");
      return false;
    }
  }
}