---
id: client-libraries
title: Pulsar client libraries
sidebar_label: "Overview"
---

Pulsar supports the following language specific client libraries:

| Language  | Documentation                                                        | Release note                                                                      | Code repo                                                                        |
| --------- | -------------------------------------------------------------------- | --------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Java      | [User doc](client-libraries-java.md)   <br/> [API doc](/api/client/) | [Bundled](/release-notes/)                                                        | [Bundled](https://github.com/apache/pulsar/tree/master/pulsar-client)            |
| C++       | [User doc](client-libraries-cpp.md)    <br/> [API doc](/api/cpp/)    | [Bundled](/release-notes/)                                                        | [Bundled](https://github.com/apache/pulsar/tree/master/pulsar-client-cpp)        |
| Python    | [User doc](client-libraries-python.md) <br/> [API doc](/api/python/) | [Bundled](/release-notes/)                                                        | [Bundled](https://github.com/apache/pulsar/tree/master/pulsar-client-cpp/python) |
| Go client | [User doc](client-libraries-go.md)                                   | [Standalone](https://github.com/apache/pulsar-client-go/releases)                 | [Standalone](https://github.com/apache/pulsar-client-go)                         |
| Node.js   | [User doc](client-libraries-node.md)                                 | [Standalone](https://github.com/apache/pulsar-client-node/releases)               | [Standalone](https://github.com/apache/pulsar-client-node)                       |
| C#        | [User doc](client-libraries-dotnet.md)                               | [Standalone](https://github.com/apache/pulsar-dotpulsar/blob/master/CHANGELOG.md) | [Standalone](https://github.com/apache/pulsar-dotpulsar)                         |

Pulsar supports the following language-agnostic client libraries:

| Interface | Documentation                             | Release note               | Code repo                                                                |
| --------- | ----------------------------------------- | -------------------------- | ------------------------------------------------------------------------ |
| REST      | [User doc](client-libraries-rest.md)      | [Bundled](/release-notes/) | [Bundled](https://github.com/apache/pulsar/tree/master/pulsar-broker)    |
| WebSocket | [User doc](client-libraries-websocket.md) | [Bundled](/release-notes/) | [Bundled](https://github.com/apache/pulsar/tree/master/pulsar-websocket) |

## Feature matrix

Pulsar client feature matrix for different languages is listed on [Pulsar Feature Matrix (Client and Function)](https://docs.google.com/spreadsheets/d/1YHYTkIXR8-Ql103u-IMI18TXLlGStK8uJjDsOOA0T20/edit#gid=1784579914) page.

## Third-party clients

Besides the official released clients, multiple projects on developing Pulsar clients are available in different languages.

> Want your repository listed here? Just submit a PR to the [pulsar repository](https://github.com/apache/pulsar/edit/master/site2/docs/client-libraries.md).

### .NET

| Project                                                                    | Description                                     | License                                    | Star                                                                                                          |
| -------------------------------------------------------------------------- | ----------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| [pulsar-client-dotnet](https://github.com/fsprojects/pulsar-client-dotnet) | Apache Pulsar native client for .NET (C#/F#/VB) | [MIT](https://opensource.org/licenses/MIT) | ![GitHub Repo Stars](https://img.shields.io/github/stars/fsprojects/pulsar-client-dotnet?style=for-the-badge) |

### Go

| Project                                                         | Description                              | License                                                   | Star                                                                                                   |
| --------------------------------------------------------------- | ---------------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| [pulsar-client-go](https://github.com/Comcast/pulsar-client-go) | A Go client library for Apache Pulsar    | [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) | ![GitHub Repo Stars](https://img.shields.io/github/stars/Comcast/pulsar-client-go?style=for-the-badge) |
| [go-pulsar](https://github.com/t2y/go-pulsar)                   | go-pulsar is a client library for Pulsar | [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) | ![GitHub Repo Stars](https://img.shields.io/github/stars/t2y/go-pulsar?style=for-the-badge)            |

### Haskell

| Project                                          | Description                      | License                                                   | Star                                                                                           |
| ------------------------------------------------ | -------------------------------- | --------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| [supernova](https://github.com/cr-org/supernova) | Apache Pulsar client for Haskell | [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) | ![GitHub Repo Stars](https://img.shields.io/github/stars/cr-org/supernova?style=for-the-badge) |

### Node.js

| Project                                                     | Description                                                                                   | License                                    | Star                                                                                                    |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| [pulsar-flex](https://github.com/ayeo-flex-org/pulsar-flex) | Pulsar Flex is a modern Apache Pulsar client for Node.js, developed to be independent of C++. | [MIT](https://opensource.org/licenses/MIT) | ![GitHub Repo Stars](https://img.shields.io/github/stars/ayeo-flex-org/pulsar-flex?style=for-the-badge) |

### PHP

| Project                                                             | Description                                 | License                                    | Star                                                                                                      |
| ------------------------------------------------------------------- | ------------------------------------------- | ------------------------------------------ | --------------------------------------------------------------------------------------------------------- |
| [pulsar-client-php](https://github.com/ikilobyte/pulsar-client-php) | PHP Native Client library for Apache Pulsar | [MIT](https://opensource.org/licenses/MIT) | ![GitHub Repo Stars](https://img.shields.io/github/stars/ikilobyte/pulsar-client-php?style=for-the-badge) |

### Rust

| Project                                                | Description                           | License                                                   | Star                                                                                                 |
| ------------------------------------------------------ | ------------------------------------- | --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| [pulsar-rs](https://github.com/streamnative/pulsar-rs) | Rust Client library for Apache Pulsar | [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) | ![GitHub Repo Stars](https://img.shields.io/github/stars/streamnative/pulsar-rs?style=for-the-badge) |

### Scala

| Project                                             | Description                                                          | License                                                   | Star                                                                                               |
| --------------------------------------------------- | -------------------------------------------------------------------- | --------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| [pulsar4s](https://github.com/CleverCloud/pulsar4s) | Idiomatic, typesafe, and reactive Scala client for Apache Pulsar     | [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) | ![GitHub Repo Stars](https://img.shields.io/github/stars/CleverCloud/pulsar4s?style=for-the-badge) |
| [neutron](https://github.com/cr-org/neutron)        | Purely functional Apache Pulsar client for Scala built on top of Fs2 | [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) | ![GitHub Repo Stars](https://img.shields.io/github/stars/cr-org/neutron?style=for-the-badge)       |
