class ConstructorParam{
  final String name;
  final bool optional;
  /// 意思是，这一个构造参数 对应 (排序好的fields list)的字段索引
  late final int _fieldIndex; // -1表示不需要

  ConstructorParam._( this.name, this.optional){
    assert(name.isNotEmpty);
  }

  ConstructorParam.withName(this.name, this.optional){
    assert(name.isNotEmpty);
  }

  void setFieldIndex(int index){
    assert (index >= 0);
    _fieldIndex = index;
  }

  void setNotInclude(){
    assert(optional);
    _fieldIndex = -1;
  }

  int get fieldIndex => _fieldIndex;

  ConstructorParam copyWithOptional( bool optional) => ConstructorParam._(name, optional);
}