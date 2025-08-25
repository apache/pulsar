# PIP-53: Contribute DotPulsar to Apache Pulsar

* **Status**: Proposed
* **Author**: Daniel Blankensteiner
* **Pull Request**: 
* **Mailing List discussion**: https://lists.apache.org/thread.html/8ebe35027d818e595eec322da26a3f392324ec3d86c4c1b12b1ff498%40%3Cdev.pulsar.apache.org%3E
* **Release**: 

At [Danske Commodities A/S](https://github.com/danske-commodities/dotpulsar/wiki#who-we-are) we have developed a Pulsar client library for .NET - DotPulsar.
The repository is here: https://github.com/danske-commodities/dotpulsar
and the NuGet package is here: https://www.nuget.org/packages/DotPulsar/
We would like to contribute the project back to the Pulsar community.

---

# Motivation

Currently, there are no official client libraries for .NET. Therefore we have written one entirely in C# (implementing the binary protocol), using the latest constructs like IAsyncDisposable, IAsyncEnumerable, ValueTask, nullable, pipelines and ReadOnlyMemory/ReadOnlySequence, for optimal developer experience and performance.

# Features

DotPulsar is by no means feature complete, but the basic use cases (for Danske Commodities A/S) are supported.

- Service discovery (LOOKUP)
- Automatic reconnect/retry
- [TLS connections](https://github.com/danske-commodities/dotpulsar/wiki/Client#tls-connection)
- [TLS Authentication](https://github.com/danske-commodities/dotpulsar/wiki/Client#tls-authentication)
- [JSON Web Token Authentication](https://github.com/danske-commodities/dotpulsar/wiki/Client#json-web-token-authentication)
- [Producing message with/without metadata](https://github.com/danske-commodities/dotpulsar/wiki/Producer)
- [Consuming messages using all subscription types and seeking](https://github.com/danske-commodities/dotpulsar/wiki/Consumer)
- [Reading messages](https://github.com/danske-commodities/dotpulsar/wiki/Reader)
- Read/Consume/Acknowledge batched messages (but currently not producing batched messages)

# Licenses

DotPulsar is under the Apache License Version 2.0 and only has two dependencies for the .NET Standard 2.1 version and four dependencies for the .NET Standard 2.0 version (which can be used from .NET Framework clients).

## System.IO.Pipelines, Microsoft.Bcl.AsyncInterfaces and Microsoft.Bcl.HashCode

Developed by Microsoft and under the MIT license.

## Protobuf-net

Developed by Marc Gravell and under the Apache License Version 2.0.

We are looking forward to any feedback.
