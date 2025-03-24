import 'package:meta/meta.dart';

@immutable
class LibImportPack{
  final String? dartCorePrefix;
  final Map<int,String> _libIdToPrefix;

  const LibImportPack(this._libIdToPrefix, this.dartCorePrefix);

  String? getPrefixByLibId(int libId){
    return _libIdToPrefix[libId];
  }

  bool get noPrefix =>_libIdToPrefix.isEmpty;
}