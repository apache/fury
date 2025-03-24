import 'package:fury/src/exception/fury_exception.dart';

class UnregisteredTagException extends FuryException {
  final String _tag;

  UnregisteredTagException(this._tag);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('Unregistered tag: ');
    buf.writeln(_tag);
  }
}

class UnregisteredTypeException extends FuryException {
  final Type _type;

  UnregisteredTypeException(this._type);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('Unregistered type: ');
    buf.writeln(_type);
  }
}

class DuplicatedTagRegistrationException extends FuryException {

  final String _tag;
  final Type _tagType;
  final Type _newType;

  DuplicatedTagRegistrationException(this._tag, this._tagType, this._newType);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('Duplicate registration for tag: ');
    buf.writeln(_tag);
    buf.write('\nThis tag is already registered for type: ');
    buf.writeln(_tagType);
    buf.write('\nBut you are now trying to register it for type: ');
    buf.writeln(_newType);
  }
}

class DuplicatedTypeRegistrationException extends FuryException {

  final Type _forType;
  final String _newTag;

  DuplicatedTypeRegistrationException(this._forType, this._newTag);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('Duplicate registration for type: ');
    buf.writeln(_forType);
    buf.write('\nBut you try to register another tag: ');
    buf.writeln(_newTag);
  }
}