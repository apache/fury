# Apache Fury™ Go

Fury is a blazingly fast multi-language serialization framework powered by just-in-time compilation and zero-copy.

Currently, Fury Go is implemented using reflection. In the future, we plan to implement a static code generator
to generate serializer code ahead to speed up serialization, or implement a JIT framework which generate ASM
instructions to speed up serialization.
