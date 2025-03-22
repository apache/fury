// enum DevExcep{
//   magicNumberLack;
//
//   static final List<Function(List<Object> args)> _genMsg = [
//     (List<Object> args) => 'Magic number is lack, please check the magic number in the file ${args[0]}',
//   ];
//
//   const DevExcep(this._genMsg);
//
//   String getMsg(List<Object> args){
//     return
//   }
// }

class DevExceps{
  static final MagicNumberLack magicNumberLack = MagicNumberLack();
}

sealed class DevExcep extends Error {
}

// sealed class UnSupportNow

class MagicNumberLack extends DevExcep {
  String call(int should) {
    return "The fury xlang serialization must start with magic number $should. "
        "Please check whether the serialization is based on the xlang "
        "protocol and the data didn't corrupt.";
  }
}