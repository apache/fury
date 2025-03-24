class EnumBrief{
  // These two fields embed the Enum's Spec directly into TypeSpec when the Type is Enum, to reduce runtime overhead
  final String importPrefix;
  final String enumName;

  const EnumBrief(
    this.importPrefix,
    this.enumName,
  );
}
