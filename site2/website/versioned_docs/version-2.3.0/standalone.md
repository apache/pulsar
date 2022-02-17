---
slug: /
id: standalone
title: Setting up a local standalone cluster
sidebar_label: "Run Pulsar locally"
original_id: standalone
---

For the purposes of local development and testing, you can run Pulsar in standalone mode on your own machine. Standalone mode includes a Pulsar broker as well as the necessary ZooKeeper and BookKeeper components running inside of a single Java Virtual Machine (JVM) process.

> #### Pulsar in production? 
> If you're looking to run a full production Pulsar installation, see the [Deploying a Pulsar instance](deploy-bare-metal) guide.

## Run Pulsar Standalone Manually

### System requirements

Currently, Pulsar is available for 64-bit **macOS**, **Linux**, and **Windows**. To use Pulsar, you need to install 64-bit JRE/JDK 8 or later versions.


### Installing Pulsar

To get started running Pulsar, download a binary tarball release in one of the following ways:

* by clicking the link below and downloading the release from an Apache mirror:

  * <a href="pulsar:binary_release_url" download>Pulsar @pulsar:version@ binary release</a>

* from the Pulsar [downloads page](pulsar:download_page_url)
* from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  
  $ wget pulsar:binary_release_url
  
  ```

Once the tarball is downloaded, untar it and `cd` into the resulting directory:

```bash

$ tar xvfz apache-pulsar-@pulsar:version@-bin.tar.gz
$ cd apache-pulsar-@pulsar:version@

```

### What your package contains

The Pulsar binary package initially contains the following directories:

Directory | Contains
:---------|:--------
`bin` | Pulsar's command-line tools, such as [`pulsar`](reference-cli-tools.md#pulsar) and [`pulsar-admin`](reference-pulsar-admin)
`conf` | Configuration files for Pulsar, including for [broker configuration](reference-configuration.md#broker), [ZooKeeper configuration](reference-configuration.md#zookeeper), and more
`examples` | A Java JAR file containing example [Pulsar Functions](functions-overview)
`lib` | The [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) files used by Pulsar
`licenses` | License files, in `.txt` form, for various components of the Pulsar [codebase](https://github.com/apache/pulsar)

These directories will be created once you begin running Pulsar:

Directory | Contains
:---------|:--------
`data` | The data storage directory used by ZooKeeper and BookKeeper
`instances` | Artifacts created for [Pulsar Functions](functions-overview)
`logs` | Logs created by the installation


### Installing Builtin Connectors (optional)

Since release `2.1.0-incubating`, Pulsar releases a separate binary distribution, containing all the `builtin` connectors.
If you would like to enable those `builtin` connectors, you can download the connectors tarball release in one of the following ways:

* by clicking the link below and downloading the release from an Apache mirror:

  * <a href="pulsar:connector_release_url" download>Pulsar IO Connectors @pulsar:version@ release</a>

* from the Pulsar [downloads page](pulsar:download_page_url)
* from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  
  $ wget pulsar:connector_release_url/{connector}-@pulsar:version@.nar
  
  ```

Once the nar file is downloaded, copy the file to directory `connectors` in the pulsar directory, 
for example, if the connector file `pulsar-io-aerospike-@pulsar:version@.nar` is downloaded:

```bash

$ mkdir connectors
$ mv pulsar-io-aerospike-@pulsar:version@.nar connectors

$ ls connectors
pulsar-io-aerospike-@pulsar:version@.nar
...

```

> #### NOTES
>
> If you are running Pulsar in a bare metal cluster, you need to make sure `connectors` tarball is unzipped in every broker's pulsar directory
> (or in every function-worker's pulsar directory if you are running a separate worker cluster for Pulsar functions).
> 
> If you are [running Pulsar in Docker](getting-started-docker.md) or deploying Pulsar using a docker image (e.g. [K8S](deploy-kubernetes.md) or [DCOS](deploy-dcos)),
> you can use `apachepulsar/pulsar-all` image instead of `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled [all builtin connectors](io-overview.md#working-with-connectors).

## Installing Tiered Storage Offloaders (optional)

> Since release `2.2.0`, Pulsar releases a separate binary distribution, containing the tiered storage offloaders.
> If you would like to enable tiered storage feature, you can follow the instructions as below; otherwise you can
> skip this section for now.

To get started using [tiered storage offloaders](concepts-tiered-storage), you'll need to download the offloaders tarball release on every broker node in
one of the following ways:

* by clicking the link below and downloading the release from an Apache mirror:

  * <a href="pulsar:offloader_release_url" download>Pulsar Tiered Storage Offloaders @pulsar:version@ release</a>

* from the Pulsar [downloads page](pulsar:download_page_url)
* from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  
  $ wget pulsar:offloader_release_url
  
  ```

Once the tarball is downloaded, in the pulsar directory, untar the offloaders package and copy the offloaders as `offloaders`
in the pulsar directory:

```bash

$ tar xvfz apache-pulsar-offloaders-@pulsar:version@-bin.tar.gz

// you will find a directory named `apache-pulsar-offloaders-@pulsar:version@` in the pulsar directory
// then copy the offloaders

$ mv apache-pulsar-offloaders-@pulsar:version@/offloaders offloaders

$ ls offloaders
tiered-storage-jcloud-@pulsar:version@.nar

```

For more details of how to configure tiered storage feature, you could reference this [Tiered storage cookbook](cookbooks-tiered-storage)

> #### NOTES
>
> If you are running Pulsar in a bare metal cluster, you need to make sure `offloaders` tarball is unzipped in every broker's pulsar directory
> 
> If you are [running Pulsar in Docker](getting-started-docker.md) or deploying Pulsar using a docker image (e.g. [K8S](deploy-kubernetes.md) or [DCOS](deploy-dcos)),
> you can use `apachepulsar/pulsar-all` image instead of `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled tiered storage offloaders.


### Starting the cluster

Once you have an up-to-date local copy of the release, you can start up a local cluster using the [`pulsar`](reference-cli-tools.md#pulsar) command, which is stored in the `bin` directory, and specifying that you want to start up Pulsar in standalone mode:

```bash

$ bin/pulsar standalone

```

If Pulsar has been successfully started, you should see `INFO`-level log messages like this:

```bash

2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@95] - Configuration Store cache started
2017-06-01 14:46:29,192 - INFO  - [main:AuthenticationService@61] - Authentication is disabled
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@108] - Pulsar WebSocket Service started

```

> #### Automatically created namespace
> When you start a local standalone cluster, Pulsar will automatically create a `public/default` [namespace](concepts-messaging.md#namespaces) that you can use for development purposes. All Pulsar topics are managed within namespaces. For more info, see [Topics](concepts-messaging.md#topics).

## Run Pulsar Standalone in Docker

Alternatively, you can run pulsar standalone locally in docker.

```bash

docker run -it -p 80:80 -p 8080:8080 -p 6650:6650 apachepulsar/pulsar-standalone

```

The command forwards following port to localhost:

- 80: the port for pulsar dashboard
- 8080: the http service url for pulsar service
- 6650: the binary protocol service url for pulsar service

After the docker container is running, you can access the dashboard under http://localhost .

## Testing your cluster setup

Pulsar provides a CLI tool called [`pulsar-client`](reference-cli-tools.md#pulsar-client) that enables you to do things like send messages to a Pulsar topic in a running cluster. This command will send a simple message saying `hello-pulsar` to the `my-topic` topic:

```bash

$ bin/pulsar-client produce my-topic --messages "hello-pulsar"

```

If the message has been successfully published to the topic, you should see a confirmation like this in the `pulsar-client` logs:

```

13:09:39.356 [main] INFO  org.apache.pulsar.client.cli.PulsarClientTool - 1 messages successfully produced

```

> #### No need to explicitly create new topics
> You may have noticed that we did not explicitly create the `my-topic` topic to which we sent the `hello-pulsar` message. If you attempt to write a message to a topic that does not yet exist, Pulsar will automatically create that topic for you.

## Using CLI pulsar clients 

Pulsar provides a CLI tool called [`pulsar-client`](reference-cli-tools.md#pulsar-client) that enables you to do things like receive messages from a Pulsar topic in a running cluster. This command will receive a simple message saying `hello-pulsar` to the `my-topic` topic:

```bash

$ bin/pulsar-client consume my-topic -t Shared -s demo-sub -n 0

```

If the message has been successfully published to the topic as above, you should see a confirmation like this in the `pulsar-client` logs:

```

----- got message -----
hello-pulsar

```

## Using Pulsar clients locally

Pulsar currently offers client libraries for [Java](client-libraries-java.md),  [Go](client-libraries-go.md), [Python](client-libraries-python.md) and [C++](client-libraries-cpp). If you're running a local standalone cluster, you can use one of these root URLs for interacting with your cluster:

* `http://localhost:8080`
* `pulsar://localhost:6650`

Here's an example producer for a Pulsar topic using the [Java](client-libraries-java) client:

```java

String localClusterUrl = "pulsar://localhost:6650";

PulsarClient client = PulsarClient.builder().serviceUrl(localClusterUrl).build();
Producer<byte[]> producer = client.newProducer().topic("my-topic").create();

```

Here's an example [Python](client-libraries-python) producer:

```python

import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('my-topic')

```

Finally, here's an example [C++](client-libraries-cpp) producer:

```cpp

Client client("pulsar://localhost:6650");
Producer producer;
Result result = client.createProducer("my-topic", producer);
if (result != ResultOk) {
    LOG_ERROR("Error creating producer: " << result);
    return -1;
}

```

