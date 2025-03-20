// @Skip()
library;


import 'dart:convert';
import 'dart:io';


Future<void> runBuild() async {
  print(Directory.current);
  // 启动命令行进程，指定你要运行的命令和参数
  final process = await Process.start(
    'dart', // 例如 'python', 'npm'
    ['run', 'build_runner', 'build'], // 命令行参数
    runInShell: true,
  );

  // 监听标准输出，当监测到特定提示时自动响应
  process.stdout
      .transform(utf8.decoder)
      .listen((output) {
    print(output);

    // 这里定义你的条件，什么情况下需要输入
    // 例如，当输出中包含 "请输入选项:" 或特定提示文本时
    if (output.contains('Delete these files?')) {
      print('automatically input: 1');
      // 自动提供输入并添加换行符
      process.stdin.writeln('1');
    }
  });

  // 监听标准错误
  process.stderr
      .transform(utf8.decoder)
      .listen((error) {
    print('error: $error');
  });

  // 等待进程完成
  final exitCode = await process.exitCode;
  print('exitCode: $exitCode');
}