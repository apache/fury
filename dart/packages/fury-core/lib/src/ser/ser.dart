import 'package:fury_core/src/deser_pack.dart';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/memory/byte_writer.dart';

import '../ser_pack.dart';
//
// abstract interface class _SerCacheable{
//   SerCache get cache;
// }


/// 准备设计成只能操纵非空类型的序列化器，至于空值的序列化，设计成在外部进行处理
abstract base class Ser<T> {

  final ObjType objType;
  // final bool forceNoRefWrite; //不受fury的Config控制，对于此类型不进行引用写入的标志
  final bool writeRef; // 是否进行引用写入的标志

  // // 因为用户可能建立多个fury实例，每个实例会对应不同的config,因为对于refTracking的设置不尽相同，
  // // 这里index是furyConfig对应的id, bool是是否进行refWrite
  // // writeRef数组由SerPool完全控制
  // final List<bool> writeRef = List.filled(5, true); // 这里出事元素必须是true，SerPool 需要这一条件

  const Ser(
    this.objType,
    this.writeRef,
    // [this.forceNoRefWrite = false]
  );

  // bool needWriteRef(int furyId) => writeRef[furyId];

  // Ser<T> get I;
  T read(ByteReader br, int refId, DeserPack pack);

  void write(ByteWriter bw, T v, SerPack pack);

  String get tag => throw UnimplementedError('tag is not implemented');
}