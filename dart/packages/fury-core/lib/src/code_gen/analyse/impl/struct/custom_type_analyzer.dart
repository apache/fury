import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/code_gen/analyse/analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/entity/obj_type_res.dart';
import 'package:fury_core/src/code_gen/entity/either.dart';
import 'package:fury_core/src/code_gen/excep/annotation/unregistered_type_excep.dart';
import '../../../../const/dart_type.dart';

class CustomTypeAnalyzer {

  const CustomTypeAnalyzer();

  /*
   这里要解释一下
   为什么当此类是自定义类型时，
   这里只是简单的获取，并不检查合法性，也不顺道将分析结果缓存，以方便之后可能的操作
   首先，如果这个类有注解，那么自然会触发专门对它的分析，那是写法如果有错，自然会报告，何不让那里来处理
   另外，现在来看， 注解很简单，好像可以顺道全部分析完成，但是之后可能字段逐步增多，这里再去顺道处理，
   就是越俎代庖了， 所以现在也不缓存。
   */
  // 这里either的left是目前没有发现禁止而返回的结果， 右边则是禁止的类型
  Either<ObjTypeRes, DartTypeEnum> analyseType(InterfaceElement element){
    String name = element.name;
    Uri libLoc = element.library.source.uri;
    String scheme = libLoc.scheme;
    String path = libLoc.path;

    DartTypeEnum? dartTypeEnum = DartTypeEnum.find(name, scheme, path);

    if (dartTypeEnum != null) {
      if (dartTypeEnum.objType == null) {
        return Either.right(dartTypeEnum);
      }
      return Either.left(
        ObjTypeRes(dartTypeEnum.objType!, dartTypeEnum.certainForSer)
      );
    }
    // 在内置类型中没有找到，认为是自定义类型
    if (element is EnumElement){
      if (!Analyzer.furyEnumAnnotationAnalyzer.hasFuryEnumAnnotation(element.metadata)){
        throw UnregisteredTypeException(path, name, 'FuryEnum');
      }
      return Either.left(ObjTypeRes.namedEnum);
    }
    assert(element is ClassElement);
    if (Analyzer.furyClassAnnotationAnalyzer.hasFuryClassAnnotation(element.metadata)){
      return Either.left(ObjTypeRes.namedStruct);
    }
    return Either.left(ObjTypeRes.unknownStruct);
  }

}