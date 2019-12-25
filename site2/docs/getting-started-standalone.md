---
id: standalone
title: Set Up a Standalone Pulsar Locally
sidebar_label: Run Pulsar Locally
---

For local development and test, you can run Pulsar in standalone mode on your machine. The standalone mode includes a Pulsar broker and the necessary components (ZooKeeper and BookKeeper) running in a single Java Virtual Machine (JVM) process.
This section explains how to **install**, **start**, **use** and **stop** Pulsar in standalone mode。

> #### Deploy a Pulsar Cluster? 
> If you're looking to deploy a Pulsar cluster, see [Deploy a Pulsar Cluster](deploy-bare-metal.md).

## Install Pulsar

### System Requirements

Pulsar is currently available for **MacOS** and **Linux**. 
Before installing Pulsar, you need to download and install Java 8 from [Oracle Download Center](http://www.oracle.com/).

> #### Tip
> By default, Pulsar allocates 2G JVM heap memory at startup. The volume is an extra option passed into JVM and it can be changed in `conf/pulsar_env.sh` file under `PULSAR_MEM`.

### Installation Steps

1. Download a binary tarball release of Pulsar in one of the following ways:

    + Apache mirror  (<a href="pulsar:binary_release_url" download>Pulsar {{pulsar:version}} binary release</a>)
    + Pulsar [download page](pulsar:download_page_url)  
    + Pulsar [release page](https://github.com/apache/pulsar/releases/latest)
    + Use [Wget](https://www.gnu.org/software/wget):
        ```shell
        $ wget pulsar:binary_release_url
        ```
2. Unzip the tarball and use the `cd` command to navigate to the resulting directory:
    ```bash
    $ tar xvfz apache-pulsar-{{pulsar:version}}-bin.tar.gz
    $ cd apache-pulsar-{{pulsar:version}}
    ```
    > #### Package Directories
    > The Pulsar binary package initially contains the following directories:
    > 
    > Directory | Content
    > :---------|:--------
    > `bin` | Pulsar's command-line tools, such as  [`pulsar`](reference-cli-tools.md#pulsar) and [`pulsar-admin`](reference-pulsar-admin.md).
    > `conf` | Configuration files for Pulsar, including [broker configuration](reference-configuration.md#broker), [ZooKeeper configuration](reference-configuration.md#zookeeper), and more.
    > `examples` | A Java JAR file containing [Pulsar functions](functions-overview.md) examples.
    > `lib` | The [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) files used by Pulsar.
    > `licenses` | License files in `.txt` form, for various components of Pulsar [codebase](https://github.com/apache/pulsar).
    > 
    > These directories are created once you begin running Pulsar.
    > 
    > Directory | Content
    > :---------|:--------
    > `data` | The data storage directory of ZooKeeper and BookKeeper.
    > `instances` | Artifacts created for [Pulsar functions](functions-overview.md).
    > `logs` | Installation logs.

3. (**optional**) If you need to use built-in connectors, you can install it as follows. Otherwise, skip this step and perform the next step [Start Pulsar](#start-pulsar-standalone).

    Since `2.1.0-incubating` release, Pulsar releases a separate binary distribution, containing all the built-in connectors. To enable those connectors, you can download the tarball release in one of the following ways:
     * Apache mirror <a href="pulsar:connector_release_url" download>Pulsar IO Connectors {{pulsar:version}} Release</a> 
     * Pulsar [download page](pulsar:download_page_url)  
     * Pulsar [release page](https://github.com/apache/pulsar/releases/latest)   
     * Use [Wget](https://www.gnu.org/software/wget):
     
       ```shell
       $ wget pulsar:connector_release_url/{connector}-{{pulsar:version}}.nar
       ```
    After downloading the `nar` file, copy it to the `connectors` directory under the Pulsar directory. For example, if you download the `pulsar-io-aerospike-{{pulsar:version}}.nar` connector file, enter following commands:
    ```bash
    $ mkdir connectors
    $ mv pulsar-io-aerospike-{{pulsar:version}}.nar connectors
    
    $ ls connectors
    pulsar-io-aerospike-{{pulsar:version}}.nar
    ```
    > #### Note
    >
    > * If you are running Pulsar in a bare metal cluster, make sure to unzip connectors tarball in every Pulsar directory of the broker 
    > (or in every Pulsar directory of function-worker if you are running a separate worker cluster for Pulsar functions).
    > 
    > * If you are [running Pulsar in Docker](getting-started-docker.md) or deploying Pulsar using a docker image (e.g. [K8S](deploy-kubernetes.md) or [DCOS](deploy-dcos.md)),
    > you can use `apachepulsar/pulsar-all` image instead of `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has bundled all [built-in connectors](io-overview.md#working-with-connectors).

4. (**optional**) If you need to use tiered storage offloaders, you can install it as follows. Otherwise, skip this step and perform the next step [Start Pulsar](#start-pulsar-standalone). 

    Since 2.2.0 release, Pulsar releases a separate binary distribution, containing the tiered storage offloaders. To enable tiered storage feature, follow the instructions below. To get started with [tiered storage offloaders](concepts-tiered-storage.md), you need to download the offloaders tarball release on every broker node in one of the following ways:
    * Apache mirror <a href="pulsar:offloader_release_url" download>Pulsar Tiered Storage Offloaders {{pulsar:version}} Release</a>
    * Pulsar [download page](pulsar:download_page_url)  
    * Pulsar [release page](https://github.com/apache/pulsar/releases/latest)
    * Use [Wget](https://www.gnu.org/software/wget):

         ```shell
         $ wget pulsar:offloader_release_url
         ```

    After downloading the tarball, unzip the offloaders package and copy the offloaders as `offloaders` in the pulsar directory:

    ```bash
    $ tar xvfz apache-pulsar-offloaders-{{pulsar:version}}-bin.tar.gz
    
    // you will find a directory named `apache-pulsar-offloaders-{{pulsar:version}}` in the Pulsar directory
    // then copy the offloaders

    $ mv apache-pulsar-offloaders-{{pulsar:version}}/offloaders offloaders
    
    $ ls offloaders
    tiered-storage-jcloud-{{pulsar:version}}.nar
    ```

    For more information on how to configure tiered storage, see [Tiered storage cookbook](cookbooks-tiered-storage.md).
    
    > #### Note
    >
    > * If you are running Pulsar in a bare metal cluster, make sure that `offloaders` tarball is unzipped in every Pulsar directory of the broker.
    > 
    > * If you are [running Pulsar in Docker](getting-started-docker.md) or deploying Pulsar using a docker image (e.g. [K8S](deploy-kubernetes.md) or [DCOS](deploy-dcos.md)),
    > you can use `apachepulsar/pulsar-all` image instead of `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has bundled tiered storage offloaders.

## Start Pulsar

Once you have an up-to-date local copy of Pulsar release, you can start a local cluster using [`pulsar`](reference-cli-tools.md#pulsar) command stored in the `bin` directory and specify to start Pulsar in standalone mode.

```bash
$ bin/pulsar standalone
```

If you have started Pulsar successfully, you will see `INFO`-level log messages like this:

```bash
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@95] - Configuration Store cache started
2017-06-01 14:46:29,192 - INFO  - [main:AuthenticationService@61] - Authentication is disabled
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@108] - Pulsar WebSocket Service started
```

> #### Tip
> 
> * If you need to run other commands, please open a new terminal window.
> * You can run Pulsar service as a background process through the `pulsar-daemon start standalone` command. See [pulsar-daemon](https://pulsar.apache.org/docs/en/reference-cli-tools/#pulsar-daemon) for more information.
> * By default, no encryption, authentication or authorization is configured. You can ascess Apache Pulsar from remote server without any authorization. Please do check [Security Overview](security-overview.md) to secure your deployment.
> * A `public/default` [namespace](concepts-messaging.md#namespaces) namespace is created automatically when starting a local standalone cluster. The namespace is used to manage topics. See [Topics](concepts-messaging.md#topics) for more information.

## Use Pulsar

[`pulsar-client`](reference-cli-tools.md#pulsar-client) is a CLI tool provided by Pulsar. With [`pulsar-client`](reference-cli-tools.md#pulsar-client), users can consume and produce messages to a Pulsar topic in a running cluster. 

### Consume a Message

The following command consumes a message with the subscription name `first-subscription` to the `my-topic` topic:

```bash
$ bin/pulsar-client consume my-topic -s "first-subscription"
```

If the message has been successfully consumed, you will see a confirmation as below in `pulsar-client` logs:

```
09:56:55.566 [pulsar-client-io-1-1] INFO  org.apache.pulsar.client.impl.MultiTopicsConsumerImpl - [TopicsConsumerFakeTopicNamee2df9] [first-subscription] Success subscribe new topic my-topic in topics consumer, partitions: 4, allTopicPartitionsNumber: 4
```

> #### Tip
>  
> As you have noticed that we do not explicitly create `my-topic` topic, to which we consume the message. When consuming or producing a message to a topic that does not yet exist, Pulsar will creates that topic automatically.

### Produce a Message

The following command produces a message saying `hello-pulsar` to the `my-topic` topic:

```bash
$ bin/pulsar-client produce my-topic --messages "hello-pulsar"
```

If the message has been successfully published to the topic, you will see a confirmation as below in `pulsar-client` logs:

```
13:09:39.356 [main] INFO  org.apache.pulsar.client.cli.PulsarClientTool - 1 messages successfully produced
```

## Stop Pulsar

Press `Ctrl+C` to stop a local standalone Pulsar.

> #### Tip
> If Pulsar service runs as a background process through pulsar-daemon start standalone command, please use `pulsar-daemon stop standalone`  command to stop Pulsar service. See [pulsar-daemon](https://pulsar.apache.org/docs/en/reference-cli-tools/#pulsar-daemon) for more information.


