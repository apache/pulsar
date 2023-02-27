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

![logo](https://pulsar.apache.org/img/pulsar.svg)

[![docker pull](https://img.shields.io/docker/pulls/apachepulsar/pulsar-all.svg)](https://hub.docker.com/r/apachepulsar/pulsar)
[![contributors](https://img.shields.io/github/contributors-anon/apache/pulsar)](https://github.com/apache/pulsar/graphs/contributors)
[![last commit](https://img.shields.io/github/last-commit/apache/pulsar)](https://github.com/apache/pulsar/commits/master)
[![release](https://img.shields.io/github/v/release/apache/pulsar?sort=semver)](https://pulsar.apache.org/download/)
[![downloads](https://img.shields.io/github/downloads/apache/pulsar/total)](https://pulsar.apache.org/download/)

Pulsar is a distributed pub-sub messaging platform with a very
flexible messaging model and an intuitive client API.

Learn more about Pulsar at https://pulsar.apache.org

## Main features

- Horizontally scalable (Millions of independent topics and millions
  of messages published per second)
- Strong ordering and consistency guarantees
- Low latency durable storage
- Topic and queue semantics
- Load balancer
- Designed for being deployed as a hosted service:
  - Multi-tenant
  - Authentication
  - Authorization
  - Quotas
  - Support mixing very different workloads
  - Optional hardware isolation
- Keeps track of consumer cursor position
- REST API for provisioning, admin and stats
- Geo replication
- Transparent handling of partitioned topics
- Transparent batching of messages

## Repositories

This repository is the main repository of Apache Pulsar. Pulsar PMC also maintains other repositories for
components in the Pulsar ecosystem, including connectors, adapters, and other language clients.

- [Pulsar Core](https://github.com/apache/pulsar)

### Helm Chart

- [Pulsar Helm Chart](https://github.com/apache/pulsar-helm-chart)

### Ecosystem

- [Pulsar Adapters](https://github.com/apache/pulsar-adapters)

### Clients

- [.NET/C# Client](https://github.com/apache/pulsar-dotpulsar)
- [C++ Client](https://github.com/apache/pulsar-client-cpp)
- [Go Client](https://github.com/apache/pulsar-client-go)
- [NodeJS Client](https://github.com/apache/pulsar-client-node)
- [Python Client](https://github.com/apache/pulsar-client-python)
- [Reactive Java Client](https://github.com/apache/pulsar-client-reactive)

### Dashboard & Management Tools

- [Pulsar Manager](https://github.com/apache/pulsar-manager)

### Website

- [Pulsar Site](https://github.com/apache/pulsar-site)

### CI/CD

- [Pulsar CI](https://github.com/apache/pulsar-test-infra)

### Archived/Halted

- [Pulsar Connectors](https://github.com/apache/pulsar-connectors) (bundled as [pulsar-io](pulsar-io))
- [Pulsar Translation](https://github.com/apache/pulsar-translation)
- [Pulsar SQL (Pulsar Presto Connector)](https://github.com/apache/pulsar-presto) (bundled as [pulsar-sql](pulsar-sql))
- [Ruby Client](https://github.com/apache/pulsar-client-ruby)

## Pulsar Runtime Java Version Recommendation

- pulsar ver > 2.10 and master branch

| Components     | Java Version  |
|----------------|:-------------:|
| Broker         |      17       |
| Functions / IO |      17       |
| CLI            |      17       |
| Java Client    | 8 or 11 or 17 |

- 2.8 <= pulsar ver <= 2.10

| Components     | Java Version |
|----------------|:------------:|
| Broker         |      11      |
| Functions / IO |      11      |
| CLI            |   8 or 11    |
| Java Client    |   8 or 11    |

- pulsar ver < 2.8

| Components | Java Version |
|------------|:------------:|
| All        |   8 or 11    |

## Build Pulsar

### Requirements

- JDK

    | Pulsar Version    |                    JDK Version                    |
    |-------------------|:-------------------------------------------------:|
    | master and 2.11 + | [JDK 17](https://adoptium.net/?variant=openjdk17) |
    | 2.8 / 2.9 / 2.10  | [JDK 11](https://adoptium.net/?variant=openjdk11) |
    | 2.7 -             |  [JDK 8](https://adoptium.net/?variant=openjdk8)  |

- Maven 3.6.1+
- zip

> **Note**:
>
> This project includes a [Maven Wrapper](https://maven.apache.org/wrapper/) that can be used instead of a system-installed Maven.
> Use it by replacing `mvn` by `./mvnw` on Linux and `mvnw.cmd` on Windows in the commands below.    
>
> It's better to use CMD rather than Powershell on Windows. Because maven will activate the `windows` profile which runs `rename-netty-native-libs.cmd`.

### Build

Compile and install:

```bash
$ mvn install -DskipTests
```

Compile and install individual module

```bash
$ mvn -pl module-name (e.g: pulsar-broker) install -DskipTests
```

### Minimal build (This skips most of external connectors and tiered storage handlers)

```bash
mvn install -Pcore-modules,-main -DskipTests
```

Run Unit Tests:

```bash
$ mvn test
```

Run Individual Unit Test:

```bash
$ mvn -pl module-name (e.g: pulsar-client) test -Dtest=unit-test-name (e.g: ConsumerBuilderImplTest)
```

Run Selected Test packages:

```bash
$ mvn test -pl module-name (for example, pulsar-broker) -Dinclude=org/apache/pulsar/**/*.java
```

Start standalone Pulsar service:

```bash
$ bin/pulsar standalone
```

Check https://pulsar.apache.org for documentation and examples.

## Build custom docker images

* Docker images must be built with Java 8 for `branch-2.7` or previous branches because of [ISSUE-8445](https://github.com/apache/pulsar/issues/8445).
* Java 11 is the recommended JDK version in `branch-2.8`, `branch-2.9` and `branch-2.10`.
* Java 17 is the recommended JDK version in `master`.

The following command builds the docker images `apachepulsar/pulsar-all:latest` and `apachepulsar/pulsar:latest`:

```bash
mvn clean install -DskipTests
mvn package -Pdocker,-main -am -pl docker/pulsar-all -DskipTests
```

After the images are built, they can be tagged and pushed to your custom repository. Here's an example of a bash script that tags the docker images with the current version and git revision and pushes them to `localhost:32000/apachepulsar`.

```bash
image_repo_and_project=localhost:32000/apachepulsar
pulsar_version=$(mvn initialize help:evaluate -Dexpression=project.version -pl . -q -DforceStdout)
gitrev=$(git rev-parse HEAD | colrm 10)
tag="${pulsar_version}-${gitrev}"
echo "Using tag $tag"
docker tag apachepulsar/pulsar-all:latest ${image_repo_and_project}/pulsar-all:$tag
docker push ${image_repo_and_project}/pulsar-all:$tag
docker tag apachepulsar/pulsar:latest ${image_repo_and_project}/pulsar:$tag
docker push ${image_repo_and_project}/pulsar:$tag
```

## Setting up your IDE

Read https://pulsar.apache.org/contribute/setup-ide for setting up IntelliJ IDEA or Eclipse for developing Pulsar.

## Documentation

> **Note**:
>
> For how to make contributions to Pulsar documentation, see [Pulsar Documentation Contribution Guide](https://pulsar.apache.org/contribute/document-intro/).

## Contact

##### Mailing lists

| Name                                                      | Scope                           | Subscribe                                             | Unsubscribe                                               | Archives                                                           |
|:----------------------------------------------------------|:--------------------------------|:------------------------------------------------------|:----------------------------------------------------------|:-------------------------------------------------------------------|
| [users@pulsar.apache.org](mailto:users@pulsar.apache.org) | User-related discussions        | [Subscribe](mailto:users-subscribe@pulsar.apache.org) | [Unsubscribe](mailto:users-unsubscribe@pulsar.apache.org) | [Archives](http://mail-archives.apache.org/mod_mbox/pulsar-users/) |
| [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@pulsar.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@pulsar.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/pulsar-dev/)   |

##### Slack

Pulsar slack channel at https://apache-pulsar.slack.com/

You can self-register at https://communityinviter.com/apps/apache-pulsar/apache-pulsar

##### Report a security vulnerability

To report a vulnerability for Pulsar, contact the [Apache Security Team](https://www.apache.org/security/). When reporting a vulnerability to [security@apache.org](mailto:security@apache.org), you can copy your email to [private@pulsar.apache.org](mailto:private@pulsar.apache.org) to send your report to the Apache Pulsar Project Management Committee. This is a private mailing list.

https://github.com/apache/pulsar/security/policy contains more details.

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Crypto Notice

This distribution includes cryptographic software. The country in which you currently reside may have restrictions on the import, possession, use, and/or re-export to another country, of encryption software. BEFORE using any encryption software, please check your country's laws, regulations and policies concerning the import, possession, or use, and re-export of encryption software, to see if this is permitted. See <http://www.wassenaar.org/> for more information.

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has classified this software as Export Commodity Control Number (ECCN) 5D002.C.1, which includes information security software using or performing cryptographic functions with asymmetric algorithms. The form and manner of this Apache Software Foundation distribution makes it eligible for export under the License Exception ENC Technology Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, Section 740.13) for both object code and source code.

The following provides more details on the included cryptographic software: Pulsar uses the SSL library from Bouncy Castle written by http://www.bouncycastle.org.
