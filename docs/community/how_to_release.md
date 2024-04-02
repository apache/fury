# Create a release

This document introduces how a release manager releases a new version of Apache Fury™ which complies with ASF licensing
policy.

## Introduction

`Source Release` is the key point which Apache releases, and necessary for an ASF release. As a convenience to users,
Apache Fury™ also provide binary packages. The binary packages should have same version as the source release.

Apache Incubator project cannot create official ASF
releases: [Incubator documentation](http://incubator.apache.org/guides/releasemanagement.html). The release manager need
to initiate a vote on fury community, i.e. dev@fury.apache.org. When the vote pass, start a vote on incubator community.

## Preparation

If you are a asf project release manager for the first time, it's suggested but not mandatory to read following
materials first:

- [Release Policy](https://www.apache.org/legal/release-policy.html)
- [Release distribution policy](https://infra.apache.org/release-distribution.html)
- [Releases from Incubating projects](http://incubator.apache.org/policy/incubation.html#releases)
- [Release creation process](https://infra.apache.org/release-publishing.html)
- [Apache Maven Jar repositories](https://infra.apache.org/repository-faq.html)
- [Signing releases](https://infra.apache.org/release-signing.html)
- How to [Use OpenPGP for key management](https://infra.apache.org/openpgp.html)
- [Release download pages](https://infra.apache.org/release-download-pages.html)

## Steps for publish a new release

DISCLAIMER file: https://incubator.apache.org/guides/branding.html#disclaimers
For releases, the text SHOULD be included in a separate DISCLAIMER file stored alongside the NOTICE and LICENSE files.

https://incubator.apache.org/policy/incubation.html#releases

Guide :: Distribution Guidelines: https://incubator.apache.org/guides/distribution.html

announce@apache.org
@TheASF Twitter account, or the official ASF Blog.

Community Vote:

```
Hello, Apache Fury(incubating) Community:

This is a call for vote to release Apache Fury(Incubating)
version release-0.5.0-rc1.

Apache Fury(incubating) - A blazing fast multi-language serialization
framework powered by JIT and zero-copy.

The change lists about this release:

https://github.com/apache/incubator-fury/compare/v0.4.1...v0.5.0-rc1

The release candidates:
https://dist.apache.org/repos/dist/dev/incubator/fury/0.5.0-rc1/

Git tag for the release:
https://github.com/apache/incubator-fury/releases/tag/v0.5.0-rc1

The artifacts signed with PGP key [5E580BA4], corresponding to
[chaokunyang@apache.org], that can be found in keys file:
https://downloads.apache.org/incubator/fury/KEYS

The vote will be open until the necessary
number of votes are reached.

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

To learn more about Fury, please see https://fury.apache.org/

*Valid check is a requirement for a vote. *Checklist for reference:

[ ] Download Fury is valid.
[ ] Checksums and PGP signatures are valid.
[ ] Source code distributions have correct names matching the current release.
[ ] LICENSE and NOTICE files are correct for each Fury repo.
[ ] All files have license headers if necessary.
[ ] No compiled archives bundled in source archive.
[ ] Can compile from source.

More detail checklist please refer:
https://cwiki.apache.org/confluence/display/INCUBATOR/Incubator+Release+Checklist

How to Build and Test, please refer to: https://github.com/apache/incubator-fury/blob/main/docs/guide/DEVELOPMENT.md

1.cd incubator-fury
2.sh ./build.sh

Thanks!

```
