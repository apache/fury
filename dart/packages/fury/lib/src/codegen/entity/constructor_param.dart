class ConstructorParam{
  final String name;
  final bool optional;
  /// This means that this constructor parameter corresponds to the field index of the (sorted fields list)
  late final int _fieldIndex; // -1 means not needed

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
