enum RefFlag{
  NULL(-3),
  TRACK_ALREADY(-2),
  UNTRACK_NOT_NULL(-1),
  TRACK_FIRST(0);

  final int id;

  const RefFlag(this.id);

  // static bool checkAllow(int id){
  //   return id >= NULL.id && id <= TRACK_FIRST.id;
  // }

  bool get noNeedToSer => (this == NULL || this == TRACK_ALREADY);
}