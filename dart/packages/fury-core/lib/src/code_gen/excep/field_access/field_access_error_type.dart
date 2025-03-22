enum FieldAccessErrorType{
  // 没有途径赋值该字段
  noWayToAssign("This field needs to be assigned a value because it's includedFromFury, but it's not a constructor parameter and can't be assigned via a setter."),
  noWayToGet("This field needs to be read because it's includedFromFury, but it's not public and it can't be read via a getter."),
  // 承诺includeFromFury: false 但是构造函数需要
  notIncludedButConsDemand("This field is included in the constructor, but it's not includedFromFury. ");

  final String warning;

  const FieldAccessErrorType(this.warning);
}