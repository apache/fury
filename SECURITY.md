# Security Policy
## Reporting a Vulnerability
If you believe you have found any security (technical) vulnerability in Fury, you are welcomed to submit a vulnerability report to us at https://security.alipay.com . In case of reporting any security vulnerability, please be noted that you need to include following information (Qualified Reporting):
- The code and fury version
- A detailed description with necessary screenshots
- Steps to reproduce the vulnerability and your advice to fix it
- Other useful information

## Disclaimer
Dynamic serialization such as fury java/python native serialization supports deserialize unregistered types, which provides more dynamics and flexibility, but also introduce security risks.

For example, the deserialization may invoke `init` constructor or `equals`/`hashCode` method, if the method body of unknown types contains malicious code, the system will be at risks.

Fury provides a **class registration secure option** and **enabled by default** for such protocols, which allows only deserializing trusted registered types or built-in types.

**Do not disable class registration unless you can ensure your environment is indeed secure. We are not responsible for security risks if you disable this option**.

When this option is disabled, a **class blacklist** is used to mitigate security risks. The blacklist is **limited, incomplete, not actively updated, maintained by community**, and can't prevent all risks. We'd like to accept blacklist updates, but we do not assume any responsibility for this.
