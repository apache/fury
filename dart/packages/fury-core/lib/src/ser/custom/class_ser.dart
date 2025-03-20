import 'package:fury_core/src/code_gen/entity/struct_hash_pair.dart';
import 'package:fury_core/src/excep/fury_mismatch_excep.dart';
import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/memory/byte_writer.dart';
import 'package:fury_core/src/meta/specs/class_spec.dart';
import 'package:fury_core/src/resolver/struct_hash_resolver.dart';
import 'package:fury_core/src/ser/custom/custom_ser.dart';

import '../../config/fury_config.dart';
import '../../const/obj_type.dart';
import '../../meta/spec_wraps/type_spec_wrap.dart';
import '../../meta/specs/field_spec.dart';
import '../../deser_pack.dart';
import '../../ser_pack.dart';
import '../ser.dart';
import '../ser_cache.dart';

final class ClassSerCache extends SerCache{
  const ClassSerCache();

  @override
  ClassSer getSerWithSpec(FuryConfig conf, covariant ClassSpec spec, Type dartType){
    List<TypeSpecWrap> typeWraps = TypeSpecWrap.ofList(spec.fields);
    return ClassSer(
      spec.fields,
      spec.construct,
      spec.noArgConstruct,
      typeWraps,
      conf.refTracking,
    );
  }
}


final class ClassSer extends CustomSer<Object>{

  static const ClassSerCache cache = ClassSerCache();

  final List<FieldSpec> _fields;
  final HasArgsCons? _construct;
  final NoArgsCons? _noArgConstruct;
  final List<TypeSpecWrap> _fieldTypeWraps;

  late final int _fromFuryHash;
  late final int _toFuryHash;

  bool _hashComputed = false;
  bool _fieldsSersComputed = false;

  ClassSer(
    this._fields,
    this._construct,
    this._noArgConstruct,
    this._fieldTypeWraps,
    bool refWrite,
  ): super(ObjType.NAMED_STRUCT, refWrite,);


  StructHashPair getHashPairForTest(StructHashResolver structHashResolver, String Function(Type type) getTagByDartType){
    return structHashResolver.computeHash(_fields, getTagByDartType);
  }

  @override
  Object read(ByteReader br, int refId, DeserPack pack) {
    if (!_fieldsSersComputed){
      pack.xtypeResolver.setSersForTypeWrap(_fieldTypeWraps);
      _fieldsSersComputed = true;
    }
    if (!_hashComputed){
      var pair = pack.structHashResolver.computeHash(_fields, pack.getTagByDartType);
      _fromFuryHash = pair.fromFuryHash;
      _toFuryHash = pair.toFuryHash;
      _hashComputed = true;
    }
    int readFHash = br.readInt32();
    if (readFHash != _fromFuryHash){
      throw FuryMismatchException(
        readFHash,
        _fromFuryHash,
        'The field hash read from bytes does not match the expected hash.',
      );
    }
    if (_noArgConstruct == null) {
      return _byParameterizedCons(br, refId, pack);
    }
    Object obj = _noArgConstruct();
    pack.refResolver.setRefTheLatestId(obj); // 需要立刻ref, 防止接下来的循环引用，也为了普通的引用追踪
    for (int i = 0; i < _fields.length; ++i) {
      FieldSpec fieldSpec = _fields[i];
      if (!fieldSpec.includeFromFury) continue;
      TypeSpecWrap typeWrap = _fieldTypeWraps[i];
      bool hasGenericsParam = typeWrap.hasGenericsParam;
      if (hasGenericsParam){
        pack.typeWrapStack.push(typeWrap);
      }
      late Object? fieldValue;
      Ser? ser = _fieldTypeWraps[i].ser;
      if (ser == null) {
        fieldValue = pack.furyDeser.xReadRefNoSer(br, pack);
      }else{
        fieldValue = pack.furyDeser.xReadRefWithSer(br, ser, pack);
      }
      assert(fieldSpec.setter != null);
      fieldSpec.setter!(obj, fieldValue);
      if (hasGenericsParam){
        pack.typeWrapStack.pop();
      }
    }
    return obj;
  }

  @override
  void write(ByteWriter bw, Object v, SerPack pack) {
    if (!_fieldsSersComputed){
      pack.xtypeResolver.setSersForTypeWrap(_fieldTypeWraps);
      _fieldsSersComputed = true;
    }
    if (!_hashComputed){
      var pair = pack.structHashResolver.computeHash(_fields, pack.getTagByDartType);
      _fromFuryHash = pair.fromFuryHash;
      _toFuryHash = pair.toFuryHash;
      _hashComputed = true;
    }
    bw.writeInt32(_toFuryHash);
    for (int i = 0; i < _fields.length; ++i) {
      FieldSpec fieldSpec = _fields[i];
      if (!fieldSpec.includeToFury) continue;
      TypeSpecWrap typeWrap = _fieldTypeWraps[i];
      bool hasGenericsParam = typeWrap.hasGenericsParam;
      if (hasGenericsParam){
        pack.typeWrapStack.push(typeWrap);
      }
      Object? fieldValue = fieldSpec.getter!(v);
      Ser? ser = typeWrap.ser;
      if (ser == null) {
        pack.furySer.xWriteRefNoSer(bw, fieldValue, pack);
      }else{
        pack.furySer.xWriteRefWithSer(bw, ser, fieldValue, pack);
      }
      if (hasGenericsParam){
        pack.typeWrapStack.pop();
      }
    }
  }

  Object _byParameterizedCons(ByteReader br, int refId, DeserPack pack){
    List<Object?> args = List.filled(_fields.length, null);
    for (int i = 0; i < _fields.length; ++i){
      FieldSpec fieldSpec = _fields[i];
      if (!fieldSpec.includeFromFury) continue;
      TypeSpecWrap typeWrap = _fieldTypeWraps[i];
      bool hasGenericsParam = typeWrap.hasGenericsParam;
      if (hasGenericsParam){
        pack.typeWrapStack.push(typeWrap);
      }
      Ser? ser = typeWrap.ser;
      if (ser == null) {
        args[i] = pack.furyDeser.xReadRefNoSer(br, pack);
      }else{
        args[i] = pack.furyDeser.xReadRefWithSer(br, ser, pack);
      }
      if (hasGenericsParam){
        pack.typeWrapStack.pop();
      }
    }
    // 这里是创建完成后再ref, 实际上是可能无法正确解析循环引用的，
    // 但是能到这里是因为用户通过promiseAcyclic保证了此类不会出现在循环引用中
    Object obj = _construct!(args);
    pack.refResolver.setRef(refId, obj);
    return obj;
  }
}