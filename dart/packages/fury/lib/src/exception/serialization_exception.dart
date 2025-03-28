import 'package:fury/src/exception/fury_exception.dart';

import 'package:fury/src/const/obj_type.dart';

abstract class SerializationException extends FuryException {
  final String? _where;

  SerializationException([this._where]);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    if (_where != null) {
      buf.write('where: ');
      buf.writeln(_where);
    }
  }
}

class TypeIncompatibleException extends SerializationException {
  final ObjType _specified;
  final String _reason;

  TypeIncompatibleException(this._specified, this._reason, [super._where]);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('the specified type: ');
    buf.writeln(_specified);
    buf.write('while the reason: ');
    buf.writeln(_reason);
  }
}

class SerializationRangeException extends SerializationException {
  final ObjType _specified;
  final num _yourValue;

  SerializationRangeException(this._specified, this._yourValue, [super._where]);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('the specified type: ');
    buf.writeln(_specified);
    buf.write('while your value: ');
    buf.writeln(_yourValue);
  }
}

class SerializationConflictException extends SerializationException {
  final String _setting;
  final String _but;

  SerializationConflictException(this._setting, this._but, [super._where]);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('the setting: ');
    buf.writeln(_setting);
    buf.write('while: ');
    buf.writeln(_but);
  }
}