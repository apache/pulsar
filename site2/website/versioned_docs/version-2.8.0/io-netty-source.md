---
id: version-2.8.0-io-netty-source
title: Netty source connector
sidebar_label: Netty source connector
original_id: io-netty-source
---

The Netty source connector opens a port that accepts incoming data via the configured network protocol 
and publish it to user-defined Pulsar topics.

This connector can be used in a containerized (for example, k8s) deployment. Otherwise, if the connector is running in process or thread mode, the instance may be conflicting on listening to ports.

## Configuration

The configuration of the Netty source connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `type` |String| true |tcp | The network protocol over which data is transmitted to netty. <br><br>Below are the available options:<br><li>tcp<li>http<li>udp |
| `host` | String|true | 127.0.0.1 | The host name or address on which the source instance listen. |
| `port` | int|true | 10999 | The port on which the source instance listen. |
| `numberOfThreads` |int| true |1 | The number of threads of Netty TCP server to accept incoming connections and handle the traffic of accepted connections. |


### Example

Before using the Netty source connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
        "type": "tcp",
        "host": "127.0.0.1",
        "port": "10911",
        "numberOfThreads": "1"
    }
    ```

* YAML

    ```yaml
    configs:
        type: "tcp"
        host: "127.0.0.1"
        port: 10999
        numberOfThreads: 1
    ```


## Usage 

The following examples show how to use the Netty source connector with TCP and HTTP.

### TCP 

1. Start Pulsar standalone.

    ```bash
    $ docker pull apachepulsar/pulsar:{version}

    $ docker run -d -it -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data --name pulsar-netty-standalone apachepulsar/pulsar:{version} bin/pulsar standalone
    ```

2. Create a configuration file _netty-source-config.yaml_.
   
    ```yaml
    configs:
        type: "tcp"
        host: "127.0.0.1"
        port: 10999
        numberOfThreads: 1
    ```

3. Copy the configuration file _netty-source-config.yaml_ to Pulsar server.

    ```bash
    $ docker cp netty-source-config.yaml pulsar-netty-standalone:/pulsar/conf/
    ```

4. Download the Netty source connector.

    ```bash
    $ docker exec -it pulsar-netty-standalone /bin/bash
    curl -O http://mirror-hk.koddos.net/apache/pulsar/pulsar-{version}/connectors/pulsar-io-netty-{version}.nar
    ```
    
5. Start the Netty source connector.
   
   ```bash
   $ ./bin/pulsar-admin sources localrun \
   --archive pulsar-io-{{pulsar:version}}.nar \
   --tenant public \
   --namespace default \
   --name netty \
   --destination-topic-name netty-topic \
   --source-config-file netty-source-config.yaml \
   --parallelism 1
   ```

6. Consume data.

    ```bash
    $ docker exec -it pulsar-netty-standalone /bin/bash
    
    $ ./bin/pulsar-client consume -t Exclusive -s netty-sub netty-topic -n 0
    ```

7. Open another terminal window to send data to the Netty source.

    ```bash
    $ docker exec -it pulsar-netty-standalone /bin/bash
    
    $ apt-get update
    
    $ apt-get -y install telnet

    $ root@1d19327b2c67:/pulsar# telnet 127.0.0.1 10999
    Trying 127.0.0.1...
    Connected to 127.0.0.1.
    Escape character is '^]'.
    hello
    world
    ```

8. The following information appears on the consumer terminal window.

    ```bash
    ----- got message -----
    hello

    ----- got message -----
    world
    ```

### HTTP 

1. Start Pulsar standalone.

    ```bash
    $ docker pull apachepulsar/pulsar:{version}

    $ docker run -d -it -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data --name pulsar-netty-standalone apachepulsar/pulsar:{version} bin/pulsar standalone
    ```

2. Create a configuration file _netty-source-config.yaml_.
   
    ```yaml
    configs:
        type: "http"
        host: "127.0.0.1"
        port: 10999
        numberOfThreads: 1
    ```

3. Copy the configuration file _netty-source-config.yaml_ to Pulsar server.
   
    ```bash
    $ docker cp netty-source-config.yaml pulsar-netty-standalone:/pulsar/conf/
    ```

4. Download the Netty source connector.

    ```bash
    $ docker exec -it pulsar-netty-standalone /bin/bash
    curl -O http://mirror-hk.koddos.net/apache/pulsar/pulsar-{version}/connectors/pulsar-io-netty-{version}.nar
    ```
    
5. Start the Netty source connector.
   
   ```bash
   $ ./bin/pulsar-admin sources localrun \
   --archive pulsar-io-{{pulsar:version}}.nar \
   --tenant public \
   --namespace default \
   --name netty \
   --destination-topic-name netty-topic \
   --source-config-file netty-source-config.yaml \
   --parallelism 1
   ```

6. Consume data.

    ```bash
    $ docker exec -it pulsar-netty-standalone /bin/bash
    
    $ ./bin/pulsar-client consume -t Exclusive -s netty-sub netty-topic -n 0
    ```

7. Open another terminal window to send data to the Netty source.

    ```bash
    $ docker exec -it pulsar-netty-standalone /bin/bash
    
    $ curl -X POST --data 'hello, world!' http://127.0.0.1:10999/
    ```

8. The following information appears on the consumer terminal window.

    ```bash
    ----- got message -----
    hello, world!
    ```
