class EnumBrief{
  // 这两个字段是当Type是Enum时，直接将Enum的Spec嵌入TypeSpec中，以减轻运行时开销
  final String importPrefix;
  final String enumName;

  const EnumBrief(
    this.importPrefix,
    this.enumName,
  );
}