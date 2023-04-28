# Fury

Fury is a general jit-based multi-language serialization framework with blazing fast performance and extreme usability:
- Support languages such as Java/Python/C++/Golang/NodeJS
- Cross-language serialize any object automatically. IDL definition, schema compilation ande convert object to intermediate protocol are unnecessary.
- Support shared reference and circular reference, there won't be any duplicate data or recursion error.
- Zero-copy support: out-of-band serialization protocol and off-heap read/write.
- A highly-extensible JIT framework to generate serializer code in runtime in an async multi-thread way to speed serialization: reduce virtual method invocation, conditional branching, hash lookup, and metadata writing, providing more than 30-200 times the performance of other serialization frameworks.
- a cache-friendly binary random access row storage format, supports skipping serialization and partial serialization, and can automatically convert with column-oriented storage.

In addition to cross-language capabilities, Fury also has the following abilities:
- It seamlessly replaces Java serialization frameworks such as JDK/Kryo/Hessian without modifying any code, providing more than 30 times the performance of Kryo, more than100 times the performance of Hessian, and more than200 times the performance of JDK built-in serialization. It can greatly improve the efficiency of high-performance scene RPC calls and object persistence.
- It supports shared and circular reference Golang serialization frameworks.
- It supports object automatic serialization Golang serialization frameworks.

## RoadMap
- AOT Framework for c++/golang/rust
- C++/Rust object serialization support
- Golang/Rust/NodeJS row format support
- ProtoBuffer compatibility support
- New protocols for feature and knowledge graph serialization

## How to Contribute


## Getting involved
