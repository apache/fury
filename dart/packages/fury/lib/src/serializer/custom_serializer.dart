import 'package:fury/src/serializer/serializer.dart';

abstract base class CustomSerializer<T> extends Serializer<T>{
  const CustomSerializer(super.objType, super.writeRef);
}