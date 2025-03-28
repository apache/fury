import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/type.dart';
import 'package:fury/src/codegen/collection/key/type_3string_key.dart';

class AnalysisTypeIdentifier{

  static bool _objectTypeSet = false;
  static late final InterfaceType _objectType;
  static bool get objectTypeSet => _objectTypeSet;
  static set setObjectType(InterfaceType type){
    _objectTypeSet = true;
    _objectType = type;
  }
  static InterfaceType get objectType => _objectType;

  static int get dartCoreLibId => objectType.element.library.id;


  static final List<int?> _ids = [null,null,null,null];
  static final List<Type3StringKey> _keys = [
    Type3StringKey(
      'FuryClass',
      'package',
      'fury/src/annotation/fury_class.dart',
    ),
    Type3StringKey(
      'FuryKey',
      'package',
      'fury/src/annotation/fury_key.dart',
    ),
    Type3StringKey(
      'FuryCons',
      'package',
      'fury/src/annotation/fury_constructor.dart',
    ),
    Type3StringKey(
      'FuryEnum',
      'package',
      'fury/src/annotation/fury_enum.dart',
    ),
  ];

  static bool _check(ClassElement element, int index){
    if (_ids[index] != null){
      return element.id == _ids[index];
    }
    Uri uri = element.librarySource.uri;
    Type3StringKey key = Type3StringKey(
      element.name,
      uri.scheme,
      uri.path,
    );
    if (key.hashCode != _keys[index].hashCode || key != _keys[index]){
      return false;
    }
    _ids[index] = element.id;
    return true;
  }

  static bool isFuryClass(ClassElement element){
    return _check(element, 0);
  }

  static bool isFuryKey(ClassElement element){
    return _check(element, 1);
  }

  static bool isFuryCons(ClassElement element){
    return _check(element, 2);
  }

  static bool isFuryEnum(ClassElement element){
    return _check(element, 3);
  }

  static void giveFuryEnumId(int id){
    _ids[3] = id;
  }

  static void giveFuryClassId(int id){
    _ids[0] = id;
  }

}