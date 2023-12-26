# Fury Go
Fury is a blazing fast multi-language serialization framework powered by jit(just-in-time compilation) and zero-copy.

Currently fury go are implemented using reflection. In future we plan to implement a static code generator 
to generate serializer code ahead to speed up serialization, or implement a jit framework which generate asm instructions
to speed up serialization.