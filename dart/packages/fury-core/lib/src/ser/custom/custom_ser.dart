import 'package:fury_core/src/ser/ser.dart' show Ser;

abstract base class CustomSer<T> extends Ser<T>{
  const CustomSer(super.objType, super.writeRef);
}