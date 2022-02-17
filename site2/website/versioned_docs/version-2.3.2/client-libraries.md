---
id: client-libraries
title: Pulsar client libraries
sidebar_label: "Use Pulsar with client libraries"
original_id: client-libraries
---

Pulsar supports the following client libraries:

- [Java client](#java-client)
- [Go client](#go-client)
- [Python client](#python-client)
- [C++ client](#c-client)

## Java client

For instructions on how to use the Pulsar Java client to produce and consume messages, see [Pulsar Java client](client-libraries-java).

Two independent sets of Javadoc API docs are available.

Library | Purpose
:-------|:-------
[`org.apache.pulsar.client.api`](/api/client) | The [Pulsar Java client](client-libraries-java) is used to produce and consume messages on Pulsar topics.
[`org.apache.pulsar.client.admin`](/api/admin) | The Java client for the [Pulsar admin interface](admin-api-overview).


## Go client

For a tutorial on using the Pulsar Go client, see [Pulsar Go client](client-libraries-go).


## Python client

For a tutorial on using the Pulsar Python client, see [Pulsar Python client](client-libraries-python).

There are also [pdoc](https://github.com/BurntSushi/pdoc)-generated API docs for the Python client [here](/api/python).

## C++ client

For a tutorial on using the Pulsar C++ clent, see [Pulsar C++ client](client-libraries-cpp).

There are also [Doxygen](http://www.stack.nl/~dimitri/doxygen/)-generated API docs for the C++ client [here](/api/cpp).

## Feature Matrix
Pulsar client feature matrix for different languages is listed on [Client Features Matrix](https://github.com/apache/pulsar/wiki/Client-Features-Matrix) page.

## Thirdparty Clients

Besides the official released clients, there are also multiple projects on developing a Pulsar client in different languages.

> If you have developed a new Pulsar client, feel free to submit a pull request and add your client to the list below.

| Language | Project | Maintainer | License | Description |
|----------|---------|------------|---------|-------------|
| Go | [pulsar-client-go](https://github.com/Comcast/pulsar-client-go) | [Comcast](https://github.com/Comcast) | [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) | A native golang client |
| Go | [go-pulsar](https://github.com/t2y/go-pulsar) | [t2y](https://github.com/t2y) | [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) | |
| Scala | [pulsar4s](https://github.com/sksamuel/pulsar4s) | [sksamuel](https://github.com/sksamuel) | [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) | Idomatic, typesafe, and reactive Scala client for Apache Pulsar |
