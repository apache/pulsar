# PIP-77: Contribute Supernova to Apache Pulsar

* Status: Proposed
* Author: Gabriel Volpe
* Pull Request:
* Mailing List discussion: https://lists.apache.org/thread.html/r5f1c3c9e973bb05540e9bbf968222ffe8eb2c5669f2f87292c11d6c9%40%3Cdev.pulsar.apache.org%3E
* Release: 

At [Chatroulette](https://about.chatroulette.com/) we have developed a Pulsar client library for Haskell - Supernova. The repository is here: https://github.com/cr-org/supernova and the library is here: https://hackage.haskell.org/package/supernova We would like to contribute the project back to the Pulsar community.

## Motivation

Currently, there are no official client libraries for Haskell. Therefore we have written one entirely in Haskell (implementing the binary protocol).

## Features

Supernova only implements some basic functionality.

- Service discovery (LOOKUP).
- Producing message with/without metadata.
- Consuming messages using all subscription types and seeking.

## Licenses

Supernova is licensed under the Apache License Version 2.0 and it has the following dependencies - listed on [Hackage](https://hackage.haskell.org/package/supernova).

- `async`, `base`, `bifunctor`, `binary`, `bytestring`, `crc32c`, `exceptions`, `lens-family-core`, `lens-family-th`, `managed`, `mtl`, `network`, `proto-lens` and `proto-lens-runtime` licensed under [BSD-3-Clause](https://opensource.org/licenses/BSD-3-Clause).
- `logging` licensed under [MIT](https://opensource.org/licenses/MIT).
- `text` licensed under [BSD-2-Clause](https://opensource.org/licenses/BSD-2-Clause).

We are looking forward to any feedback.
