import 'dart:typed_data';


extension Binary on int {
  int get b {
    return int.parse(toRadixString(10), radix: 2);
  }
}

int a = 1;

int func(){
  return a++;
}

class Som<T>{
  T f1;

  Som(this.f1);
}



void main(){
  var lis = Uint8List.fromList([1,2,3,4,5,6]);
  ByteData data = ByteData.sublistView(lis, 0, 6);
  var v = data.buffer.asUint8List(0,2);
  lis[0] = 1000;
  print(v);
}