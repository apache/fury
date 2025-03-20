class Stack<E> {
  final _list = <E>[];

  void push(E value) => _list.add(value);

  E pop() => _list.removeLast();

  E? get peek {
    return _list.isNotEmpty ? _list.last : null;
  }

  void changeTop(E value) {
    if (_list.isNotEmpty) {
      _list[_list.length - 1] = value;
    } else {
      throw StateError('Stack is empty');
    }
  }

  bool get isEmpty => _list.isEmpty;
  bool get isNotEmpty => _list.isNotEmpty;

  @override
  String toString() => _list.toString();
}