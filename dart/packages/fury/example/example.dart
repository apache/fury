import 'dart:typed_data';

import 'package:fury/fury.dart';

part 'example.g.dart';

@furyClass
class Person with _$PersonFury{
  final String firstName, lastName;
  final int age;
  final LocalDate dateOfBirth;

  const Person(this.firstName, this.lastName, this.age, this.dateOfBirth);
}

void main(){
  Fury fury = Fury(
    refTracking: true,
  );
  fury.register($Person, "example.Person");
  Person obj = Person('Leo', 'Leo', 21, LocalDate(2004, 1, 1));

  // Serialize
  Uint8List bytes = fury.toFury(obj);

  // Deserialize
  obj = fury.fromFury(bytes) as Person;
}