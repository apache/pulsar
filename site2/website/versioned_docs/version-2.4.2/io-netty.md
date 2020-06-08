---
id: version-2.4.2-io-netty
title: Netty Tcp or Udp Connector
sidebar_label: Netty Tcp or Udp Connector
original_id: io-netty
---

## Source

The Netty Source connector opens a port that accept incoming data via the configured network protocol and publish it to a user-defined Pulsar topic.
Also, this connector is suggested to be used in a containerized (e.g. k8s) deployment.
Otherwise, if the connector is running in process or thread mode, the instances may be conflicting on listening to ports.

### Source Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `type` | `false` | `tcp` | The network protocol over which data is trasmitted to netty. Valid values include HTTP, TCP, and UDP |
| `host` | `false` | `127.0.0.1` | The host name or address that the source instance to listen on. |
| `port` | `false` | `10999` | The port that the source instance to listen on. |
| `numberOfThreads` | `false` | `1` | The number of threads of Netty Tcp Server to accept incoming connections and handle the traffic of the accepted connections. |


### Configuration Example

Here is a configuration Json example:

```$json
{
    "type": "tcp",
    "host": "127.0.0.1",
    "port": "10911",
    "numberOfThreads": "5"
}
```
Here is a configuration Yaml example:

```$yaml
configs:
    type: "tcp"
    host: "127.0.0.1"
    port: 10999
    numberOfThreads: 1
```

### Usage example


- Start pulsar standalone

```$bash
docker pull apachepulsar/pulsar:2.4.0
docker run -d -it -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data --name pulsar-netty-standalone apachepulsar/pulsar:2.4.0 bin/pulsar standalone
```

- Start pulsar-io in standalone

#### Tcp example

- Config file netty-source-config.yaml

```$yaml
configs:
    type: "tcp"
    host: "127.0.0.1"
    port: 10999
    numberOfThreads: 1
```

- Copy configuration file to pulsar server

```$bash
docker cp netty-source-config.yaml pulsar-netty-standalone:/pulsar/conf/
```

- Download netty connector and start netty connector

```$bash
docker exec -it pulsar-netty-standalone /bin/bash
curl -O http://mirror-hk.koddos.net/apache/pulsar/pulsar-2.4.0/connectors/pulsar-io-netty-2.4.0.nar
./bin/pulsar-admin sources localrun --archive pulsar-io-{{pulsar:version}}.nar --tenant public --namespace default --name netty --destination-topic-name netty-topic --source-config-file netty-source-config.yaml --parallelism 1
```

- Consume data 

```$bash
docker exec -it pulsar-netty-standalone /bin/bash
./bin/pulsar-client consume -t Exclusive -s netty-sub netty-topic -n 0
```

- Open another window for send data to netty source

```$bash
docker exec -it pulsar-netty-standalone /bin/bash
apt-get update
apt-get -y install telnet
root@1d19327b2c67:/pulsar# telnet 127.0.0.1 10999
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
hello
world
```

- Verification results

In the consumer window just opened, you can see the following data

```bash
----- got message -----
hello

----- got message -----
world
```

#### Http example

- Config file netty-source-config.yaml

```$yaml
configs:
    type: "http"
    host: "127.0.0.1"
    port: 10999
    numberOfThreads: 1
```
- Start netty source

```$bash
docker exec -it pulsar-netty-standalone /bin/bash
./bin/pulsar-admin sources localrun --archive pulsar-io-{{pulsar:version}}.nar --tenant public --namespace default --name netty --destination-topic-name netty-topic --source-config-file netty-source-config.yaml --parallelism 1
```

- Verification results

```bash
curl -X POST --data 'hello, world!' http://127.0.0.1:10999/
```

- Verification results

In the consumer window just opened, you can see the following data

```bash
----- got message -----
hello, world!
```
