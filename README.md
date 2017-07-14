![logo](site/img/pulsar.png)

Pulsar is a distributed pub-sub messaging platform with a very
flexible messaging model and an intuitive client API.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.pulsar/pulsar/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.pulsar/pulsar)


## Main features
* Horizontally scalable (Millions of independent topics and millions
  of messages published per second)
* Strong ordering and consistency guarantees
* Low latency durable storage
* Topic and queue semantics
* Load balancer
* Designed for being deployed as a hosted service:
  * Multi-tenant
  * Authentication
  * Authorization
  * Quotas
  * Support mixing very different workloads
  * Optional hardware isolation
* Keeps track of consumer cursor position
* REST API for provisioning, admin and stats
* Geo replication
* Transparent handling of partitioned topics
* Transparent batching of messages

## Documentation

* [Getting Started](docs/GettingStarted.md)
* [Architecture](docs/Architecture.md)
* [Documentation Index](docs/Documentation.md)
* [Announcement post on Yahoo Eng Blog](https://yahooeng.tumblr.com/post/150078336821/open-sourcing-pulsar-pub-sub-messaging-at-scale)

## Contact
* [Pulsar-Dev](https://groups.google.com/d/forum/pulsar-dev) for
  development discussions
* [Pulsar-Users](https://groups.google.com/d/forum/pulsar-users) for
  users questions

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
