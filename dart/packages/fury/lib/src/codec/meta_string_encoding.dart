enum MetaStrEncoding{
  utf8(0x00, -1),
  ls(0x01, 5),
  luds(0x02, 6),
  ftls(0x03, 5),
  atls(0x04, 5);

  final int id;
  final int bits;
  const MetaStrEncoding(this.id, this.bits);

  static MetaStrEncoding fromId(int id){
    for (var value in MetaStrEncoding.values){
      if (value.id == id){
        return value;
      }
    }
    throw ArgumentError('Invalid MetaStringEncoding id: $id');
  }
}