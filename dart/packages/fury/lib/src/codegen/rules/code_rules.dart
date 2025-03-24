class CodeRules {
  // static const String mustHaveUnnamed = 'the class must have an unnamed constructor';

  static const String consParamsOnlySupportThisAndSuper = '''
  the constructor can only use parameters decorated by this and super'
  one example:
  class A extends B{
    final int _aa;
    final int ab;
    
    A(this._aa, super.bb, {required this.ab, super.ba});
  }
  ''';

  // field cant override
  static const String unsupportFieldOverriding = 'Classes in the inheritance chain cannot have members with the same name, meaning field overriding is not supported.';

  static const String circularReferenceIncapableRisk = "This class's fields (including those from the inheritance chain) are not all basic types, so it may have circular references. To handle this, the class must have a constructor without required parameters, but the constructor specified by @FuryCons does not meet this condition. If you're sure there will be no circular references, use @FuryClass(promiseAcyclic: true).";
}