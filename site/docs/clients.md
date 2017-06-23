---
title: Pulsar client libraries
layout: docs
---

Pulsar currently has client libraries available for three languages:

* [Java](#java-client)
* [C++](#c-client)
* [Python](#python-client)

## Java client

For a tutorial on using the Pulsar Java client to produce and consume messages, see [The Pulsar Java client](../{{ site.current_version }}/applications/JavaClient).

There are also two independent sets of Javadoc API docs available:

Library | Purpose
:-------|:-------
[`com.yahoo.pulsar.client.api`]({{ site.baseurl }}api/client) | The [Pulsar Java client](../{{ site.current_version }}/applications/JavaClient) for producing and consuming messages on Pulsar {% popover topics %}
[`com.yahoo.pulsar.client.admin`]({{ site.baseurl }}api/admin) | The Java interface for the [Pulsar admin interface](../{{ site.current_version }}/admin/AdminInterface)

<!-- * [`com.yahoo.pulsar.broker`]({{ site.baseurl }}/api/broker) -->

## C++ client

For a tutorial on using the Pulsar C++ clent, see [The Pulsar C++ client](../{{ site.current_version }}/applications/CppClient).

There are also [Doxygen](http://www.stack.nl/~dimitri/doxygen/)-generated API docs for the C++ client [here]({{ site.baseurl }}api/cpp).

## Python client

For a tutorial on using the Pulsar Python client, see [The Pulsar Python client](../{{ site.current_version }}/applications/PythonClient).

There are also [pdoc](https://github.com/BurntSushi/pdoc)-generated API docs for the Python client [here]({{ site.baseurl }}api/python).
