# Apache Fury™ JavaScript

Node.js high-performance suite, ensuring that your Node.js version is 20 or later.

`hps` is use for detect the string type in v8. Fury support latin1 and utf8 string both, we should get the certain type of string before write it
in buffer. It is slow to detect the string is latin1 or utf8, but hps can detect it by a hack way, which is called FASTCALL in v8.
so it is not stable now.

Apache Fury(incubating) is an effort undergoing incubation at the Apache
Software Foundation (ASF), sponsored by the Apache Incubator PMC.

Incubation is required of all newly accepted projects until a further review
indicates that the infrastructure, communications, and decision making process
have stabilized in a manner consistent with other successful ASF projects.

While incubation status is not necessarily a reflection of the completeness
or stability of the code, it does indicate that the project has yet to be
fully endorsed by the ASF.
