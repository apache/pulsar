![logo](site/img/pulsar.png)

Pulsar is a distributed pub-sub messaging platform with a very
flexible messaging model and an intuitive client API.

https://pulsar.incubator.apache.org

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

## Build Pulsar

Requirements:
 * Java JDK 1.8
 * Maven

Compile and install:

```bash
$ mvn install -DskipTests
```

Start standalone Pulsar service:

```bash
$ bin/pulsar standalone
```

Check https://pulsar.incubator.apache.org for documentation and examples.

## Contact

##### Mailing lists

| Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [users@pulsar.incubator.apache.org](mailto:users@pulsar.incubator.apache.org) | User-related discussions        | [Subscribe](mailto:users-subscribe@pulsar.incubator.apache.org) | [Unsubscribe](mailto:users-unsubscribe@pulsar.incubator.apache.org) | [Archives](http://mail-archives.apache.org/mod_mbox/incubator-pulsar-users/) |
| [dev@pulsar.incubator.apache.org](mailto:dev@pulsar.incubator.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@pulsar.incubator.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@pulsar.incubator.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/incubator-pulsar-dev/)   |

##### Slack

Pulsar slack channel at https://apache-pulsar.slack.com/

You can self-register at https://apache-pulsar.herokuapp.com/

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
