# PIP-46: Next-gen Proxy

* **Status**: Proposal
* **Author**: Jerry Peng
* **Pull Request**:
* **Mailing List discussion**:
* **Release**:

## Goals

Currently the proxy is used simply to forward messages to brokers but it has the potential to be much more.  I propose to develop a next-gen version of the proxy that allows the proxy to become more pluggable and able to support more ways to publish and consume message from Pulsar. The next-gen proxy will not serve its uses as a traditional proxy but also become a translation layer for Pulsar that allows different protocols to be able to interact with a Pulsar cluster. This will help attract more users to use Apache Pulsar as it could minimize users' migration costs. 

**Initial design goals:**

* Support different methods to publish and consume such as via HTTP/REST
* Support different wire protocol from other messaging/streaming systems.  For example, allowing users to use Apache Kafka producers and consumers to be able to produce and consume messages from an Apache Pulsar cluster. When can support a wide variety of wire protocols but I think we should prioritize the following ones:
  1. Apache Kafka
  2. MQTT

## Architecture

A variety of clients would be able to interact with a Pulsar cluster via the proxy.  More than one proxy can also be run for load balancing purposes.

![](https://www.lucidchart.com/publicSegments/view/38808e25-cc3e-4bb3-81fa-676ddff82fe6/image.jpeg)
