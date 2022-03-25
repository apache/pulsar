---
id: version-2.6.0-reference-cli-tools
title: Pulsar command-line tools
sidebar_label: Pulsar CLI tools
original_id: reference-cli-tools
---

Pulsar offers several command-line tools that you can use for managing Pulsar installations, performance testing, using command-line producers and consumers, and more.

All Pulsar command-line tools can be run from the `bin` directory of your [installed Pulsar package](getting-started-standalone.md). The following tools are currently documented:

* [`pulsar`](#pulsar)
* [`pulsar-client`](#pulsar-client)
* [`pulsar-daemon`](#pulsar-daemon)
* [`pulsar-perf`](#pulsar-perf)
* [`bookkeeper`](#bookkeeper)
* [`broker-tool`](#broker-tool)

> ### Getting help
> You can get help for any CLI tool, command, or subcommand using the `--help` flag, or `-h` for short. Here's an example:
> ```shell
> $ bin/pulsar broker --help
> ```

## `pulsar`

The pulsar tool is used to start Pulsar components, such as bookies and ZooKeeper, in the foreground.

These processes can also be started in the background, using nohup, using the pulsar-daemon tool, which has the same command interface as pulsar.

Usage:
```bash
$ pulsar command
```
Commands:
* `bookie`
* `broker`
* `compact-topic`
* `discovery`
* `configuration-store`
* `initialize-cluster-metadata`
* `proxy`
* `standalone`
* `websocket`
* `zookeeper`
* `zookeeper-shell`

Example:
```bash
$ PULSAR_BROKER_CONF=/path/to/broker.conf pulsar broker
```

The table below lists the environment variables that you can use to configure the `pulsar` tool.

|Variable|Description|Default|
|---|---|---|
|`PULSAR_LOG_CONF`|Log4j configuration file|`conf/log4j2.yaml`|
|`PULSAR_BROKER_CONF`|Configuration file for broker|`conf/broker.conf`|
|`PULSAR_BOOKKEEPER_CONF`|description: Configuration file for bookie|`conf/bookkeeper.conf`|
|`PULSAR_ZK_CONF`|Configuration file for zookeeper|`conf/zookeeper.conf`|
|`PULSAR_CONFIGURATION_STORE_CONF`|Configuration file for the configuration store|`conf/global_zookeeper.conf`|
|`PULSAR_DISCOVERY_CONF`|Configuration file for discovery service|`conf/discovery.conf`|
|`PULSAR_WEBSOCKET_CONF`|Configuration file for websocket proxy|`conf/websocket.conf`|
|`PULSAR_STANDALONE_CONF`|Configuration file for standalone|`conf/standalone.conf`|
|`PULSAR_EXTRA_OPTS`|Extra options to be passed to the jvm||
|`PULSAR_EXTRA_CLASSPATH`|Extra paths for Pulsar's classpath||
|`PULSAR_PID_DIR`|Folder where the pulsar server PID file should be stored||
|`PULSAR_STOP_TIMEOUT`|Wait time before forcefully killing the Bookie server instance if attempts to stop it are not successful||



### `bookie`

Starts up a bookie server

Usage:
```bash
$ pulsar bookie options
```

Options

|Option|Description|Default|
|---|---|---|
|`-readOnly`|Force start a read-only bookie server|false|
|`-withAutoRecovery`|Start auto-recover service bookie server|false|


Example
```bash
$ PULSAR_BOOKKEEPER_CONF=/path/to/bookkeeper.conf pulsar bookie \
  -readOnly \
  -withAutoRecovery
```

### `broker`

Starts up a Pulsar broker

Usage
```bash
$ pulsar broker options
```

Options

|Option|Description|Default|
|---|---|---|
|`-bc` , `--bookie-conf`|Configuration file for BookKeeper||
|`-rb` , `--run-bookie`|Run a BookKeeper bookie on the same host as the Pulsar broker|false|
|`-ra` , `--run-bookie-autorecovery`|Run a BookKeeper autorecovery daemon on the same host as the Pulsar broker|false|

Example
```bash
$ PULSAR_BROKER_CONF=/path/to/broker.conf pulsar broker
```

### `compact-topic`

Run compaction against a Pulsar topic (in a new process)

Usage
```bash
$ pulsar compact-topic options
```
Options

|Flag|Description|Default|
|---|---|---|
|`-t` , `--topic`|The Pulsar topic that you would like to compact||

Example
```bash
$ pulsar compact-topic --topic topic-to-compact
```

### `discovery`

Run a discovery server

Usage
```bash
$ pulsar discovery
```

Example
```bash
$ PULSAR_DISCOVERY_CONF=/path/to/discovery.conf pulsar discovery
```

### `configuration-store`

Starts up the Pulsar configuration store

Usage
```bash
$ pulsar configuration-store
```

Example
```bash
$ PULSAR_CONFIGURATION_STORE_CONF=/path/to/configuration_store.conf pulsar configuration-store
```

### `initialize-cluster-metadata`

One-time cluster metadata initialization

Usage
```bash
$ pulsar initialize-cluster-metadata options
```

Options

|Flag|Description|Default|
|---|---|---|
|`-ub` , `--broker-service-url`|The broker service URL for the new cluster||
|`-tb` , `--broker-service-url-tls`|The broker service URL for the new cluster with TLS encryption||
|`-c` , `--cluster`|Cluster name||
|`--configuration-store`|The configuration store quorum connection string||
|`-uw` , `--web-service-url`|The web service URL for the new cluster||
|`-tw` , `--web-service-url-tls`|The web service URL for the new cluster with TLS encryption||
|`-zk` , `--zookeeper`|The local ZooKeeper quorum connection string||


### `proxy`

Manages the Pulsar proxy

Usage
```bash
$ pulsar proxy options
```

Options

|Flag|Description|Default|
|---|---|---|
|`--configuration-store`|Configuration store connection string||
|`-zk` , `--zookeeper-servers`|Local ZooKeeper connection string||

Example
```bash
$ PULSAR_PROXY_CONF=/path/to/proxy.conf pulsar proxy \
  --zookeeper-servers zk-0,zk-1,zk2 \
  --configuration-store zk-0,zk-1,zk-2
```

### `standalone`

Run a broker service with local bookies and local ZooKeeper

Usage
```bash
$ pulsar standalone options
```

Options

|Flag|Description|Default|
|---|---|---|
|`-a` , `--advertised-address`|The standalone broker advertised address||
|`--bookkeeper-dir`|Local bookies’ base data directory|data/standalone/bookkeeper|
|`--bookkeeper-port`|Local bookies’ base port|3181|
|`--no-broker`|Only start ZooKeeper and BookKeeper services, not the broker|false|
|`--num-bookies`|The number of local bookies|1|
|`--only-broker`|Only start the Pulsar broker service (not ZooKeeper or BookKeeper)||
|`--wipe-data`|Clean up previous ZooKeeper/BookKeeper data||
|`--zookeeper-dir`|Local ZooKeeper’s data directory|data/standalone/zookeeper|
|`--zookeeper-port` |Local ZooKeeper’s port|2181|

Example
```bash
$ PULSAR_STANDALONE_CONF=/path/to/standalone.conf pulsar standalone
```

### `websocket`

Usage
```bash
$ pulsar websocket
```

Example
```bash
$ PULSAR_WEBSOCKET_CONF=/path/to/websocket.conf pulsar websocket
```

### `zookeeper`

Starts up a ZooKeeper cluster

Usage
```bash
$ pulsar zookeeper
```

Example
```bash
$ PULSAR_ZK_CONF=/path/to/zookeeper.conf pulsar zookeeper
```


### `zookeeper-shell`

Connects to a running ZooKeeper cluster using the ZooKeeper shell

Usage
```bash
$ pulsar zookeeper-shell options
```

Options

|Flag|Description|Default|
|---|---|---|
|`-c`, `--conf`|Configuration file for ZooKeeper||
|`-server`|Configuration zk address, eg: `127.0.0.1:2181`||



## `pulsar-client`

The pulsar-client tool

Usage
```bash
$ pulsar-client command
```

Commands
* `produce`
* `consume`


Options

|Flag|Description|Default|
|---|---|---|
|`--auth-params`|Authentication parameters, whose format is determined by the implementation of method `configure` in authentication plugin class, for example "key1:val1,key2:val2" or "{\"key1\":\"val1\",\"key2\":\"val2\"}"|{"saslJaasClientSectionName":"PulsarClient", "serverType":"broker"}|
|`--auth-plugin`|Authentication plugin class name|org.apache.pulsar.client.impl.auth.AuthenticationSasl|
|`--proxy-protocol`|Proxy protocol to select type of routing at proxy||
|`--proxy-url`|Proxy-server URL to which to connect||
|`--url`|Broker URL to which to connect|pulsar://localhost:6650/ </br> ws://localhost:8080 |
|`-h`, `--help`|Show this help


### `produce`
Send a message or messages to a specific broker and topic

Usage
```bash
$ pulsar-client produce topic options
```

Options

|Flag|Description|Default|
|---|---|---|
|`-f`, `--files`|Comma-separated file paths to send; either -m or -f must be specified|[]|
|`-m`, `--messages`|Comma-separated string of messages to send; either -m or -f must be specified|[]|
|`-n`, `--num-produce`|The number of times to send the message(s); the count of messages/files * num-produce should be below 1000|1|
|`-r`, `--rate`|Rate (in messages per second) at which to produce; a value 0 means to produce messages as fast as possible|0.0|
|`-c`, `--chunking`|Split the message and publish in chunks if the message size is larger than the allowed max size|false|
|`-s`, `--separator`|Character to split messages string with.|","|
|`-k`, `--key`|Message key to add|key=value string, like k1=v1,k2=v2.|
|`-p`, `--properties`|Properties to add. If you want to add multiple properties, use the comma as the separator, e.g. `k1=v1,k2=v2`.| |


### `consume`
Consume messages from a specific broker and topic

Usage
```bash
$ pulsar-client consume topic options
```

Options

|Flag|Description|Default|
|---|---|---|
|`--hex`|Display binary messages in hexadecimal format.|false|
|`-n`, `--num-messages`|Number of messages to consume, 0 means to consume forever.|1|
|`-r`, `--rate`|Rate (in messages per second) at which to consume; a value 0 means to consume messages as fast as possible|0.0|
|`--regex`|Indicate the topic name is a regex pattern|false|
|`-s`, `--subscription-name`|Subscription name||
|`-t`, `--subscription-type`|The type of the subscription. Possible values: Exclusive, Shared, Failover, Key_Shared.|Exclusive|
|`-p`, `--subscription-position`|The position of the subscription. Possible values: Latest, Earliest.|Latest|
|`-q`, `--queue-size`|The size of consumer's receiver queue.|0|
|`-mc`, `--max_chunked_msg`|Max pending chunk messages.|0|
|`-ac`, `--auto_ack_chunk_q_full`|Auto ack for the oldest message in consumer's receiver queue if the queue full.|false|



## `pulsar-daemon`
A wrapper around the pulsar tool that’s used to start and stop processes, such as ZooKeeper, bookies, and Pulsar brokers, in the background using nohup.

pulsar-daemon has a similar interface to the pulsar command but adds start and stop commands for various services. For a listing of those services, run pulsar-daemon to see the help output or see the documentation for the pulsar command.

Usage
```bash
$ pulsar-daemon command
```

Commands
* `start`
* `stop`


### `start`
Start a service in the background using nohup.

Usage
```bash
$ pulsar-daemon start service
```

### `stop`
Stop a service that’s already been started using start.

Usage
```bash
$ pulsar-daemon stop service options
```

Options

|Flag|Description|Default|
|---|---|---|
|-force|Stop the service forcefully if not stopped by normal shutdown.|false|



## `pulsar-perf`
A tool for performance testing a Pulsar broker.

Usage
```bash
$ pulsar-perf command
```

Commands
* `consume`
* `produce`
* `read`
* `websocket-producer`
* `managed-ledger`
* `monitor-brokers`
* `simulation-client`
* `simulation-controller`
* `help`

Environment variables

The table below lists the environment variables that you can use to configure the pulsar-perf tool.

|Variable|Description|Default|
|---|---|---|
|`PULSAR_LOG_CONF`|Log4j configuration file|conf/log4j2.yaml|
|`PULSAR_CLIENT_CONF`|Configuration file for the client|conf/client.conf|
|`PULSAR_EXTRA_OPTS`|Extra options to be passed to the JVM||
|`PULSAR_EXTRA_CLASSPATH`|Extra paths for Pulsar's classpath||


### `consume`
Run a consumer

Usage
```
$ pulsar-perf consume options
```

Options

|Flag|Description|Default|
|---|---|---|
|`--auth-params`|Authentication parameters, whose format is determined by the implementation of method `configure` in authentication plugin class. For example, `key1:val1,key2:val2` or `{"key1":"val1","key2":"val2"}`.||
|`--auth_plugin`|Authentication plugin class name||
|`-ac`, `--auto_ack_chunk_q_full`|Auto ack for the oldest message in consumer's receiver queue if the queue full|false|
|`--acks-delay-millis`|Acknowlegments grouping delay in millis|100|
|`-k`, `--encryption-key-name`|The private key name to decrypt payload||
|`-v`, `--encryption-key-value-file`|The file which contains the private key to decrypt payload||
|`-h`, `--help`|Help message|false|
|`--conf-file`|Configuration file||
|`-e`, `--expire_time_incomplete_chunked_messages`|The expiration time for incomplete chunk messages (in milliseconds)|0|
|`-c`, `--max-connections`|Max number of TCP connections to a single broker|100|
|`-mc`, `--max_chunked_msg`|Max pending chunk messages|0|
|`-n`, `--num-consumers`|Number of consumers (per topic)|1|
|`-t`, `--num-topic`|The number of topics|1|
|`-r`, `--rate`|Simulate a slow message consumer (rate in msg/s)|0|
|`-q`, `--receiver-queue-size`|Size of the receiver queue|1000|
|`--replicated`|Whether the subscription status should be replicated|false|
|`-u`, `--service-url`|Pulsar service URL||
|`-i`, `--stats-interval-seconds`|Statistics interval seconds. If 0, statistics will be disabled|0|
|`-s`, `--subscriber-name`|Subscriber name prefix|sub|
|`-st`, `--subscription-type`|Subscriber type. Possible values are Exclusive, Shared, Failover, Key_Shared.|Exclusive|
|`-sp`, `--subscription-position`|Subscriber position. Possible values are Latest, Earliest.|Latest|
|`-time`, `--test-duration`|Test duration (in seconds). If the value is 0 or smaller than 0, it keeps consuming messages|0|
|`--trust-cert-file`|Path for the trusted TLS certificate file||


### `produce`
Run a producer

Usage
```bash
$ pulsar-perf produce options
```

Options

|Flag|Description|Default|
|---|---|---|
|`--auth-params`|Authentication parameters, whose format is determined by the implementation of method `configure` in authentication plugin class. For example, `key1:val1,key2:val2` or `{"key1":"val1","key2":"val2"}`.||
|`--auth_plugin`|Authentication plugin class name||
|`-b`, `--batch-time-window`|Batch messages in a window of the specified number of milliseconds|1|
|`-bb`, `--batch-max-bytes`|Maximum number of bytes per batch|4194304|
|`-bm`, `--batch-max-messages`|Maximum number of messages per batch|1000|
|`-ch`, `--chunking`|Split the message and publish in chunks if the message size is larger than allowed max size|false|
|`-d`, `--delay`|Mark messages with a given delay in seconds|0s|
|`-z`, `--compression`|Compress messages’ payload. Possible values are NONE, LZ4, ZLIB, ZSTD or SNAPPY.||
|`--conf-file`|Configuration file||
|`-k`, `--encryption-key-name`|The public key name to encrypt payload||
|`-v`, `--encryption-key-value-file`|The file which contains the public key to encrypt payload||
|`-ef`, `--exit-on-failure`|Exit from the process on publish failure|false|
|`-h`, `--help`|Help message|false|
|`-c`, `--max-connections`|Max number of TCP connections to a single broker|100|
|`-o`, `--max-outstanding`|Max number of outstanding messages|1000|
|`-p`, `--max-outstanding-across-partitions`|Max number of outstanding messages across partitions|50000|
|`-m`, `--num-messages`|Number of messages to publish in total. If the value is 0 or smaller than 0, it keeps publishing messages.|0|
|`-n`, `--num-producers`|The number of producers (per topic)|1|
|`-threads`, `--num-test-threads`|Number of test threads|1|
|`-t`, `--num-topic`|The number of topics|1|
|`-f`, `--payload-file`|Use payload from an UTF-8 encoded text file and a payload will be randomly selected when publishing messages||
|`-e`, `--payload-delimiter`|The delimiter used to split lines when using payload from a file|\n|
|`-r`, `--rate`|Publish rate msg/s across topics|100|
|`-u`, `--service-url`|Pulsar service URL||
|`-s`, `--size`|Message size (in bytes)|1024|
|`-i`, `--stats-interval-seconds`|Statistics interval seconds. If 0, statistics will be disabled.|0|
|`-time`, `--test-duration`|Test duration (in seconds). If the value is 0 or smaller than 0, it keeps publishing messages.|0|
|`--trust-cert-file`|Path for the trusted TLS certificate file||
|`--warmup-time`|Warm-up time in seconds|1|


### `read`
Run a topic reader

Usage
```bash
$ pulsar-perf read options
```

Options

|Flag|Description|Default|
|---|---|---|
|`--auth-params`|Authentication parameters, whose format is determined by the implementation of method `configure` in authentication plugin class. For example, `key1:val1,key2:val2` or `{"key1":"val1","key2":"val2"}`.||
|`--auth_plugin`|Authentication plugin class name||
|`--conf-file`|Configuration file||
|`-h`, `--help`|Help message|false|
|`-c`, `--max-connections`|Max number of TCP connections to a single broker|100|
|`-t`, `--num-topic`|The number of topics|1|
|`-r`, `--rate`|Simulate a slow message reader (rate in msg/s)|0|
|`-q`, `--receiver-queue-size`|Size of the receiver queue|1000|
|`-u`, `--service-url`|Pulsar service URL||
|`-m`, `--start-message-id`|Start message id. This can be either 'earliest', 'latest' or a specific message id by using 'lid:eid'|earliest|
|`-i`, `--stats-interval-seconds`|Statistics interval seconds. If 0, statistics will be disabled.|0|
|`-time`, `--test-duration`|Test duration (in seconds). If the value is 0 or smaller than 0, it keeps consuming messages.|0|
|`--trust-cert-file`|Path for the trusted TLS certificate file||
|`--use-tls`|Use TLS encryption on the connection|false|


### `websocket-producer`
Run a websocket producer

Usage
```bash
$ pulsar-perf websocket-producer options
```

Options

|Flag|Description|Default|
|---|---|---|
|`--auth-params`|Authentication parameters, whose format is determined by the implementation of method `configure` in authentication plugin class. For example, `key1:val1,key2:val2` or `{"key1":"val1","key2":"val2"}`.||
|`--auth_plugin`|Authentication plugin class name||
|`--conf-file`|Configuration file||
|`-h`, `--help`|Help message|false|
|`-m`, `--num-messages`|Number of messages to publish in total. If the value is 0 or smaller than 0, it keeps publishing messages|0|
|`-t`, `--num-topic`|The number of topics|1|
|`-f`, `--payload-file`|Use payload from a file instead of empty buffer||
|`-u`, `--proxy-url`|Pulsar Proxy URL, e.g., "ws://localhost:8080/"||
|`-r`, `--rate`|Publish rate msg/s across topics|100|
|`-s`, `--size`|Message size in byte|1024|
|`-time`, `--test-duration`|Test duration (in seconds). If the value is 0 or smaller than 0, it keeps publishing messages|0|


### `managed-ledger`
Write directly on managed-ledgers

Usage
```bash
$ pulsar-perf managed-ledger options
```

Options

|Flag|Description|Default|
|---|---|---|
|`-a`, `--ack-quorum`|Ledger ack quorum|1|
|`-dt`, `--digest-type`|BookKeeper digest type. Possible Values: [CRC32, MAC, CRC32C, DUMMY]|CRC32C|
|`-e`, `--ensemble-size`|Ledger ensemble size|1|
|`-h`, `--help`|Help message|false|
|`-c`, `--max-connections`|Max number of TCP connections to a single bookie|1|
|`-o`, `--max-outstanding`|Max number of outstanding requests|1000|
|`-m`, `--num-messages`|Number of messages to publish in total. If the value is 0 or smaller than 0, it keeps publishing messages|0|
|`-t`, `--num-topic`|Number of managed ledgers|1|
|`-r`, `--rate`|Write rate msg/s across managed ledgers|100|
|`-s`, `--size`|Message size in byte|1024|
|`-time`, `--test-duration`|Test duration (in seconds). If the value is 0 or smaller than 0, it keeps publishing messages|0|
|`--threads`|Number of threads writing|1|
|`-w`, `--write-quorum`|Ledger write quorum|1|
|`-zk`, `--zookeeperServers`|ZooKeeper connection string||


### `monitor-brokers`
Continuously receive broker data and/or load reports

Usage
```bash
$ pulsar-perf monitor-brokers options
```

Options

|Flag|Description|Default|
|---|---|---|
|`--connect-string`|A connection string for one or more ZooKeeper servers||
|`-h`, `--help`|Help message|false|


### `simulation-client`
Run a simulation server acting as a Pulsar client. Uses the client configuration specified in `conf/client.conf`.

Usage
```bash
$ pulsar-perf simulation-client options
```

Options

|Flag|Description|Default|
|---|---|---|
|`--port`|Port to listen on for controller|0|
|`--service-url`|Pulsar Service URL||
|`-h`, `--help`|Help message|false|

### `simulation-controller`
Run a simulation controller to give commands to servers

Usage
```bash
$ pulsar-perf simulation-controller options
```

Options

|Flag|Description|Default|
|---|---|---|
|`--client-port`|The port that the clients are listening on|0|
|`--clients`|Comma-separated list of client hostnames||
|`--cluster`|The cluster to test on||
|`-h`, `--help`|Help message|false|


### `help`
This help message

Usage
```bash
$ pulsar-perf help
```


## `bookkeeper`
A tool for managing BookKeeper.

Usage
```bash
$ bookkeeper command
```

Commands
* `autorecovery`
* `bookie`
* `localbookie`
* `upgrade`
* `shell`


Environment variables

The table below lists the environment variables that you can use to configure the bookkeeper tool.

|Variable|Description|Default|
|---|---|---|
|BOOKIE_LOG_CONF|Log4j configuration file|conf/log4j2.yaml|
|BOOKIE_CONF|BookKeeper configuration file|conf/bk_server.conf|
|BOOKIE_EXTRA_OPTS|Extra options to be passed to the JVM||
|BOOKIE_EXTRA_CLASSPATH|Extra paths for BookKeeper's classpath||
|ENTRY_FORMATTER_CLASS|The Java class used to format entries||
|BOOKIE_PID_DIR|Folder where the BookKeeper server PID file should be stored||
|BOOKIE_STOP_TIMEOUT|Wait time before forcefully killing the Bookie server instance if attempts to stop it are not successful||


### `auto-recovery`
Runs an auto-recovery service

Usage
```bash
$ bookkeeper autorecovery options
```

Options

|Flag|Description|Default|
|---|---|---|
|`-c`, `--conf`|Configuration for the auto-recovery||


### `bookie`
Starts up a BookKeeper server (aka bookie)

Usage
```bash
$ bookkeeper bookie options
```

Options

|Flag|Description|Default|
|---|---|---|
|`-c`, `--conf`|Configuration for the auto-recovery||
|-readOnly|Force start a read-only bookie server|false|
|-withAutoRecovery|Start auto-recovery service bookie server|false|


### `localbookie`
Runs a test ensemble of N bookies locally

Usage
```bash
$ bookkeeper localbookie N
```

### `upgrade`
Upgrade the bookie’s filesystem

Usage
```bash
$ bookkeeper upgrade options
```

Options

|Flag|Description|Default|
|---|---|---|
|`-c`, `--conf`|Configuration for the auto-recovery||
|`-u`, `--upgrade`|Upgrade the bookie’s directories||


### `shell`
Run shell for admin commands. To see a full listing of those commands, run bookkeeper shell without an argument.

Usage
```bash
$ bookkeeper shell
```

Example
```bash
$ bookkeeper shell bookiesanity
```

## `broker-tool`

The `broker- tool` is used for operations on a specific broker.

Usage
```bash
$ broker-tool command
```
Commands
* `load-report`
* `help`

Example
Two ways to get more information about a command as below:

```bash
$ broker-tool help command
$ broker-tool command --help
```

### `load-report`

Collect the load report of a specific broker. 
The command is run on a broker, and used for troubleshooting why broker can’t collect right load report.

Options

|Flag|Description|Default|
|---|---|---|
|`-i`, `--interval`| Interval to collect load report, in milliseconds ||
|`-h`, `--help`| Display help information ||

