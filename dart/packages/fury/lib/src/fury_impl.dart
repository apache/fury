import 'dart:typed_data';
import 'package:fury/src/base_fury.dart';
import 'package:fury/src/codegen/entity/struct_hash_pair.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/dev_annotation/optimize.dart';
import 'package:fury/src/deserialize_coordinator.dart';
import 'package:fury/src/serialize_coordinator.dart';
import 'package:fury/src/manager/fury_config_manager.dart';
import 'package:fury/src/resolver/xtype_resolver.dart';
import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/meta/specs/custom_type_spec.dart';
import 'package:fury/src/serializer/serializer.dart';

final class Fury implements BaseFury{

  static final DeserializeCoordinator _deserDirector = DeserializeCoordinator.I;
  static final SerializeCoordinator _serDirector = SerializeCoordinator.I;

  final FuryConfig _conf;
  late final XtypeResolver _xtypeResolver;

  Fury({
    bool isLittleEndian = true,
    bool refTracking = true,
    bool basicTypesRefIgnored = true,
    bool timeRefIgnored = true,
    // bool stringRefIgnored = true,
  }) : _conf = FuryConfigManager.inst.createConfig(
    isLittleEndian: isLittleEndian,
    refTracking: refTracking,
    basicTypesRefIgnored: basicTypesRefIgnored,
    timeRefIgnored: timeRefIgnored,
    // stringRefIgnored: stringRefIgnored,
  ){
    _xtypeResolver = XtypeResolver.newOne(_conf);
  }

  @override
  @inline
  void register(CustomTypeSpec spec, [String? tag]) {
    _xtypeResolver.reg(spec, tag);
  }

  @inline
  @override
  void registerSerializer(Type type, Serializer ser) {
    _xtypeResolver.registerSerializer(type, ser);
  }

  @override
  @inline
  Object? fromFury(Uint8List bytes, [ByteReader? reader]) {
    return _deserDirector.deser(bytes, _conf, _xtypeResolver, reader);
  }

  @override
  @inline
  Uint8List toFury(Object? obj,) {
    return _serDirector.ser(obj, _conf, _xtypeResolver);
  }

  @override
  @inline
  void toFuryWithWriter(Object? obj, ByteWriter writer) {
    _serDirector.serWithWriter(obj, _conf, _xtypeResolver, writer);
  }

  // for test only
  StructHashPair getStructHashPair(Type type) {
    return _xtypeResolver.getHashPairForTest(type);
  }
}