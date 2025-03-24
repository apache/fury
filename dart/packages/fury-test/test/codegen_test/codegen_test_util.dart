// @Skip()
library;

import 'dart:convert';
import 'dart:io';

Future<void> runBuild() async {
  print(Directory.current);
  // Start a command line process, specifying the command and parameters you want to run
  final process = await Process.start(
    'dart', // e.g., 'python', 'npm'
    ['run', 'build_runner', 'build'], // Command line parameters
    runInShell: true,
  );

  // Listen to standard output and automatically respond when a specific prompt is detected
  process.stdout
      .transform(utf8.decoder)
      .listen((output) {
    print(output);

    // Define your conditions here, under what circumstances you need to input
    // For example, when the output contains "Delete these files?" or specific prompt text
    if (output.contains('Delete these files?')) {
      print('automatically input: 1');
      // Automatically provide input and add a newline character
      process.stdin.writeln('1');
    }
  });

  // Listen to standard error
  process.stderr
      .transform(utf8.decoder)
      .listen((error) {
    print('error: $error');
  });

  // Wait for the process to complete
  final exitCode = await process.exitCode;
  print('exitCode: $exitCode');
}
