<div align="center">
  <img width="77%" alt="" src="docs/images/logo/fury-logo.svg"><br>
</div>

# Fury

Fury is a blazing fast general multi-language native serialization framework powered by jit and zero-copy:

- Support languages such as Java/Python/C++/Golang/NodeJS
- Cross-language serialize any object automatically, no need for IDL definition, schema compilation ande object protocol
  conversion.
- Support shared reference and circular reference, no duplicate data or recursion error.
- Zero-copy support: cross-lagnauge out-of-band serialization protocol inspired
  by [pickle5](https://peps.python.org/pep-0574/) and off-heap read/write.
- A highly-extensible JIT framework to generate serializer code in runtime in an async multi-thread way to speed
  serialization, providing more than 30-200 times the performance compared to other serialization frameworks:
    - reduce memory access by inline variable in generated code.
    - reduce virtual method invocation.
    - reduce conditional branching.
    - reduce hash lookup.
- a cache-friendly binary random access row storage format, supports skipping serialization and partial serialization,
  and can convert to column-format automatically.

In addition to cross-language capabilities, Fury also has the following abilities:

- Drop-in replaces Java serialization frameworks such as JDK/Kryo/Hessian without modifying any code, providing more
  than 30 times the performance of Kryo, more than 100 times the performance of Hessian, and more than 200 times the
  performance of JDK built-in serialization. It can greatly improve the efficiency of high-performance RPC calls and
  object persistence.
- It supports shared and circular reference object serialization for golang.
- It supports automatic object serialization for golang.

## RoadMap

- AOT Framework for c++/golang/rust
- C++/Rust object serialization support
- Golang/Rust/NodeJS row format support
- ProtoBuffer compatibility support
- Protocols for feature and knowledge graph serialization
- Serialization infrastructure for any new protocols
    - JIT support
    - Zero-copy
    - Meta compression and sharing
    - Memory primitives

## How to Contribute

## Getting involved

| Platform                                                                                                                                                          | Purpose                                                                                                                                                                                                   | Estimated Response Time |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| [GitHub Issues](https://github.com/alipay/fury/issues)                                                                                                            | For reporting bugs and filing feature requests.                                                                                                                                                           | < 1 days                |
| [Slack](https://join.slack.com/t/fury-project/shared_invite/zt-1u8soj4qc-ieYEu7ciHOqA2mo47llS8A)                                                                  | For collaborating with other Fury users and latest announcements about Fury.                                                                                                                              | < 2 days                |
| [StackOverflow](https://stackoverflow.com/questions/tagged/fury)                                                                                                  | For asking questions about how to use Fury.                                                                                                                                                               | < 2 days                |
| [Zhihu](https://www.zhihu.com/column/c_1638859452651765760)  [Twitter](https://twitter.com/fury_community)  [Youtube](https://www.youtube.com/@FurySerialization) | Follow us for latest announcements about Fury.                                                                                                                                                            | < 2 days                |
| WeChat Official Account(微信公众号) / Dingding Group(钉钉群)                                                                                                              | <div style="text-align:center;"><img src="docs/images/fury_wechat_12.jpg" alt="WeChat Official Account " width="38%"/> <img src="docs/images/fury_dingding.jpg" alt="Dingding Group" width="30%"/> </div> | < 2 days                |
