<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

![logo](site/img/pulsar.png)

Pulsar is a distributed pub-sub messaging platform with a very
flexible messaging model and an intuitive client API.

https://pulsar.apache.org

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

Check https://pulsar.apache.org for documentation and examples.

## Build Pulsar docs

Refer to the docs [README](site2/README.md).

## Contact

##### Mailing lists

| Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [users@pulsar.apache.org](mailto:users@pulsar.apache.org) | User-related discussions        | [Subscribe](mailto:users-subscribe@pulsar.apache.org) | [Unsubscribe](mailto:users-unsubscribe@pulsar.apache.org) | [Archives](http://mail-archives.apache.org/mod_mbox/pulsar-users/) |
| [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@pulsar.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@pulsar.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/pulsar-dev/)   |

##### Slack

Pulsar slack channel at https://apache-pulsar.slack.com/

You can self-register at https://apache-pulsar.herokuapp.com/

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Crypto Notice

This distribution includes cryptographic software. The country in which you currently reside may have restrictions on the import, possession, use, and/or re-export to another country, of encryption software. BEFORE using any encryption software, please check your country's laws, regulations and policies concerning the import, possession, or use, and re-export of encryption software, to see if this is permitted. See <http://www.wassenaar.org/> for more information.

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has classified this software as Export Commodity Control Number (ECCN) 5D002.C.1, which includes information security software using or performing cryptographic functions with asymmetric algorithms. The form and manner of this Apache Software Foundation distribution makes it eligible for export under the License Exception ENC Technology Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, Section 740.13) for both object code and source code.

The following provides more details on the included cryptographic software: Pulsar uses the SSL library from Bouncy Castle written by http://www.bouncycastle.org.

