# Fury Row Format

Fury row format is heavily inspired by spark tungsten row format, but with changes:

- Use arrow schema to describe meta.
- The implementation support java/C++/python/etc..
- String support latin/utf16/utf8 encoding.
- Decimal use arrow decimal format.
- Variable-size field can be inline in fixed-size region if small enough.
- Allow skip padding by generate Row using aot to put offsets in generated code.
- Support adding fields without breaking compatibility.

The initial fury java row data structure implementation is modified from spark unsafe row/writer.
