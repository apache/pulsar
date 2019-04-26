---
id: standalone
title: Setting up a local standalone cluster
sidebar_label: Run Pulsar locally
---

For local development and testing, you can run Pulsar in standalone mode on your own machine. Standalone mode includes a Pulsar broker, the necessary ZooKeeper and BookKeeper components running inside of a single Java Virtual Machine (JVM) process.

> #### Pulsar in production? 
> If you're looking to run a full production Pulsar installation, see the [Deploying a Pulsar instance](deploy-bare-metal.md) guide.

## Run Pulsar Standalone Manually

### System requirements

Pulsar is currently available for **MacOS** and **Linux**. In order to use Pulsar, you need to install [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).


### Installing Pulsar

To get started with Pulsar, download a binary tarball release in one of the following ways:

* by clicking the link below and downloading the release from an Apache mirror:

  * <a href="pulsar:binary_release_url" download>Pulsar {{pulsar:version}} binary release</a>

* from the Pulsar [downloads page](pulsar:download_page_url)
* from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  $ wget pulsar:binary_release_url
  ```

After you download the tarball, untar it and use the `cd` command to navigate to the resulting directory:

```bash
$ tar xvfz apache-pulsar-{{pulsar:version}}-bin.tar.gz
$ cd apache-pulsar-{{pulsar:version}}
```

### What your package contains

The Pulsar binary package initially contains the following directories:

Directory | Contains
:---------|:--------
`bin` | Pulsar's command-line tools, such as [`pulsar`](reference-cli-tools.md#pulsar) and [`pulsar-admin`](reference-pulsar-admin.md).
`conf` | Configuration files for Pulsar, including [broker configuration](reference-configuration.md#broker), [ZooKeeper configuration](reference-configuration.md#zookeeper), and more.
`examples` | A Java JAR file containing [Pulsar Functions](functions-overview.md) example.
`lib` | The [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) files used by Pulsar.
`licenses` | License files, in the`.txt` form, for various components of the Pulsar [codebase](https://github.com/apache/pulsar).

These directories are created once you begin running Pulsar.

Directory | Contains
:---------|:--------
`data` | The data storage directory used by ZooKeeper and BookKeeper.
`instances` | Artifacts created for [Pulsar Functions](functions-overview.md).
`logs` | Logs created by the installation.


### Installing Builtin Connectors (optional)

Since `2.1.0-incubating` release, Pulsar releases a separate binary distribution, containing all the `builtin` connectors.
To enable those `builtin` connectors, you can download the connectors tarball release in one of the following ways:

* by clicking the link below and downloading the release from an Apache mirror:

  * <a href="pulsar:connector_release_url" download>Pulsar IO Connectors {{pulsar:version}} release</a>

* from the Pulsar [downloads page](pulsar:download_page_url)
* from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  $ wget pulsar:connector_release_url/{connector}-{{pulsar:version}}.nar
  ```

After you download the nar file, copy the file to the `connectors` directory in the pulsar directory. 
For example, if you download the `pulsar-io-aerospike-{{pulsar:version}}.nar` connector file, enter the following commands:

```bash
$ mkdir connectors
$ mv pulsar-io-aerospike-{{pulsar:version}}.nar connectors

$ ls connectors
pulsar-io-aerospike-{{pulsar:version}}.nar
...
```

> #### NOTES
>
> If you are running Pulsar in a bare metal cluster, make sure `connectors` tarball is unzipped in every pulsar directory of the broker
> (or in every pulsar directory of function-worker if you are running a separate worker cluster for Pulsar Functions).
> 
> If you are [running Pulsar in Docker](getting-started-docker.md) or deploying Pulsar using a docker image (e.g. [K8S](deploy-kubernetes.md) or [DCOS](deploy-dcos.md)),
> you can use the `apachepulsar/pulsar-all` image instead of the `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled [all builtin connectors](io-overview.md#working-with-connectors).

## Installing Tiered Storage Offloaders (optional)

> Since `2.2.0` release, Pulsar releases a separate binary distribution, containing the tiered storage offloaders.
> To enable tiered storage feature, follow the instructions below; otherwise skip this section.

To get started with [tiered storage offloaders](concepts-tiered-storage.md), you need to download the offloaders tarball release on every broker node in
one of the following ways:

* by clicking the link below and downloading the release from an Apache mirror:

  * <a href="pulsar:offloader_release_url" download>Pulsar Tiered Storage Offloaders {{pulsar:version}} release</a>

* from the Pulsar [downloads page](pulsar:download_page_url)
* from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  $ wget pulsar:offloader_release_url
  ```

After you download the tarball, untar the offloaders package and copy the offloaders as `offloaders`
in the pulsar directory:

```bash
$ tar xvfz apache-pulsar-offloaders-{{pulsar:version}}-bin.tar.gz

// you will find a directory named `apache-pulsar-offloaders-{{pulsar:version}}` in the pulsar directory
// then copy the offloaders

$ mv apache-pulsar-offloaders-{{pulsar:version}}/offloaders offloaders

$ ls offloaders
tiered-storage-jcloud-{{pulsar:version}}.nar
```

For more details on how to configure tiered storage feature, refer to [Tiered storage cookbook](cookbooks-tiered-storage.md).

> #### NOTES
>
> If you are running Pulsar in a bare metal cluster, make sure that `offloaders` tarball is unzipped in every broker's  pulsar directory.
> 
> If you are [running Pulsar in Docker](getting-started-docker.md) or deploying Pulsar using a docker image (e.g. [K8S](deploy-kubernetes.md) or [DCOS](deploy-dcos.md)),
> you can use the `apachepulsar/pulsar-all` image instead of the `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled tiered storage offloaders.


### Starting the cluster

Once you have an up-to-date local copy of the release, you can start a local cluster using the [`pulsar`](reference-cli-tools.md#pulsar) command, which is stored in the `bin` directory, and specifying that you want to start Pulsar in standalone mode.

```bash
$ bin/pulsar standalone
```

If you have started Pulsar successfully, you will see `INFO`-level log messages like this:

```bash
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@95] - Configuration Store cache started
2017-06-01 14:46:29,192 - INFO  - [main:AuthenticationService@61] - Authentication is disabled
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@108] - Pulsar WebSocket Service started
```

> #### Automatically created namespace
> When you start a local standalone cluster, a `public/default` [namespace](concepts-messaging.md#namespaces) is created automatically. The namespace is used for development purposes. All Pulsar topics are managed within namespaces. For more information, see [Topics](concepts-messaging.md#topics).

## Run Pulsar Standalone in Docker

Alternatively, you can run a Pulsar standalone locally in docker.

```bash
docker run -it -p 80:80 -p 8080:8080 -p 6650:6650 apachepulsar/pulsar-standalone
```

The command forwards the following port to localhost:

- 80: the port for Pulsar dashboard
- 8080: the HTTP service URL for Pulsar service
- 6650: the binary protocol service URL for Pulsar service

After the docker container is running, you can access the dashboard at http://localhost.

## Testing your cluster setup

Pulsar provides a CLI tool called [`pulsar-client`](reference-cli-tools.md#pulsar-client). The pulsar-client enables you to send messages to a Pulsar topic in a running cluster. The following command sends a message saying `hello-pulsar` to the `my-topic` topic:

```bash
$ bin/pulsar-client produce my-topic --messages "hello-pulsar"
```

If the message has been successfully published to the topic, you will see a confirmation like this in the `pulsar-client` logs:

```
13:09:39.356 [main] INFO  org.apache.pulsar.client.cli.PulsarClientTool - 1 messages successfully produced
```


> #### No need to create new topics explicitly
> As you have noticed that we do not explicitly create the `my-topic` topic, to which we sent the `hello-pulsar` message. When you write a message to a topic that does not yet exist, Pulsar creates that topic for you automatically.

## Using Pulsar clients locally

Pulsar currently offers client libraries for [Java](client-libraries-java.md),  [Go](client-libraries-go.md), [Python](client-libraries-python.md) and [C++](client-libraries-cpp.md). If you run a local standalone cluster, you can use one of these root URLs to interact with your cluster:

* `http://localhost:8080`
* `pulsar://localhost:6650`

The following is an example of producer for a Pulsar topic using the [Java](client-libraries-java.md) client:

```java
String localClusterUrl = "pulsar://localhost:6650";

PulsarClient client = PulsarClient.builder().serviceUrl(localClusterUrl).build();
Producer<byte[]> producer = client.newProducer().topic("my-topic").create();
```

The following is an example of [Python](client-libraries-python.md) producer:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('my-topic')
```

The following is an example of [C++](client-libraries-cpp.md) producer:

```cpp
Client client("pulsar://localhost:6650");
Producer producer;
Result result = client.createProducer("my-topic", producer);
if (result != ResultOk) {
    LOG_ERROR("Error creating producer: " << result);
    return -1;
}
```