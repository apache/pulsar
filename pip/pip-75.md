# PIP-75: Replace protobuf code generator

* **Status**: Proposal
* **Author**: Matteo Merli
* **Pull Request**:
* **Mailing List discussion**:
* **Release**:

## Motivation

In the Pulsar wire protocol, we are using Google Protobuf in order to perform
serialization/deserialization of the commands that are exchanged between
clients and brokers.

Because of the overhead involved with the regular Protobuf implementation, since
very early on, we have been using a modified version of Protobuf 2.4.1.
The modifications were done to ensure a more efficient serialization code that
used thread local caches for the objects used in the process.

There are few issues with the current approach:
 1. The patch to the Protobuf code generator is only based on version 2.4.1 and
    cannot be upgraded to newer Protobuf versions
 2. The new Protobuf version, 3.xx, do have the same performance issues as the
    2.x versions.
 3. The thread-local approach for reusing objects is not ideal. Thread-local
    access is not free and it would be better to instead cache the root objects
    only.

## Goal

Have an efficient and maintainable way to perform serialization/deserialization
of Pulsar protocol.

The current proposal is to switch from the patched Protobuf 2.4.1 and use a
different code generator, Splunk LightProto: https://github.com/splunk/lightproto.

This code generator has the following features/goals:

 1. Generate the fastest possible Java code for Protobuf SerDe
 2. 100% Compatible with proto2 definition and wire protocol
 3. Zero-copy deserialization using Netty ByteBuf
 4. Deserialize from direct memory
 5. Zero heap allocations in serialization / deserialization
 6. Lazy deserialization of strings and bytes
 7. Reusable mutable objects
 8. No runtime dependency library
 9. Java based code generator with Maven plugin

There is extensive testing to ensure the generated code serializes and parses
the same bytes in the same way as the Google Protobuf does.
