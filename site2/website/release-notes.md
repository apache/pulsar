
## Apache 

### 2.2.0 &mdash; 2018-10-17 <a id="2.2.0"></a>

This is the first release of Pulsar as an Apache Top Level Project 

This is a fetaure release, including several new features, improvements and fixes for  issues reported for 2.1.1-incubating.
 
* [Pulsar Java Client Interceptors](https://github.com/apache/pulsar/pull/2627) 

* [Integration of functions and io with schema registry](https://github.com/apache/pulsar/pull/2266) 

* [Dead Letter Topic](https://github.com/apache/pulsar/wiki/PIP-22%3A-Pulsar-Dead-Letter-Topic) 

* [Flink Source connector](https://github.com/apache/pulsar/pull/2555) 

* [JDBC Sink Connector](https://github.com/apache/pulsar/issues/2313) 

* [HDFS Sink Connector](https://github.com/apache/pulsar/pull/2409) 

* [Google Cloud Storage Offloader](https://github.com/apache/pulsar/issues/2067) 

* [Pulsar SQL](https://github.com/apache/pulsar/wiki/PIP-19:-Pulsar-SQL) 


For a complete list of issues fixed, see 

https://github.com/apache/pulsar/milestone/16?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v2.2.0





## Apache incubator

### 2.1.1-incubating &mdash; 2018-09-17 <a id="2.1.1-incubating"></a>

This release fixes issues reported for 2.1.0-incubating.

 * [#2473](https://github.com/apache/incubator-pulsar/pull/2473) - Downgrading ZK to stable version 3.4.13
 * [#2219](https://github.com/apache/incubator-pulsar/pull/2219) - Cpp client: add PatternMultiTopicsConsumerImpl to support regex subscribe
 * [#2387](https://github.com/apache/incubator-pulsar/pull/2387) - Fixed race condition during expansion of concurrent open hash maps
 * [#2348](https://github.com/apache/incubator-pulsar/pull/2348) - Fix NPE when splitting and unloading bundle
 * [#2223](https://github.com/apache/incubator-pulsar/pull/2223) - fix bug in FunctionRuntimeManager involving not cleaning up old invalid assignments
 * [#2367](https://github.com/apache/incubator-pulsar/pull/2367) - [compaction] make topic compaction works with partitioned topic
 * [#2203](https://github.com/apache/incubator-pulsar/pull/2203) - Make sure schema is initialized before the topic is loaded

The complete list of changes can be found at:
https://github.com/apache/incubator-pulsar/milestone/17?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v2.1.1-incubating

### 2.1.0-incubating &mdash; 2018-08-02 <a id="2.1.0-incubating"></a>

This is the seventh release of Apache Pulsar since entering the ASF incubator.

It is a feature release, including several new features and major improvements:

- [Pulsar IO](/docs/en/io-overview/): A connector framework for moving data in and out of Apache Pulsar leveraging [Pulsar Functions](/docs/en/functions-overview/) runtime.
- A list of [Builtin Connectors](/docs/en/io-connectors):
  - [Aerospike Connector](/docs/en/io-aerospike/)
  - [Cassandra Connector](/docs/en/io-cassandra/)
  - [Kafka Connector](/docs/en/io-kafka/)
  - [Kinesis Connector](/docs/en/io-kinesis/)
  - [RabbitMQ Connector](/docs/en/io-rabbitmq/)
  - [Twitter Firehose Connector](/docs/en/io-twitter/) 
- [Tiered Storage](/docs/en/concepts-tiered-storage/): An extension in Pulsar segment store to offload older segments into long term storage (e.g. HDFS, S3).
  S3 support is supported in 2.1 release.
- [Stateful function](/docs/en/functions-state/): Pulsar Functions is able to use [State API](/docs/en/functions-state#api) for storing state within Pulsar.
- Pulsar [Go Client](/docs/en/client-libraries-go/)
- [Avro](https://github.com/apache/incubator-pulsar/blob/v2.1.0-incubating/pulsar-client-schema/src/main/java/org/apache/pulsar/client/impl/schema/AvroSchema.java) and
  [Protobuf](https://github.com/apache/incubator-pulsar/blob/v2.1.0-incubating/pulsar-client-schema/src/main/java/org/apache/pulsar/client/impl/schema/ProtobufSchema.java) Schema support

The complete list of changes can be found at: https://github.com/apache/incubator-pulsar/milestone/13?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v2.1.0-incubating

### 2.0.1-incubating &mdash; 2018-06-18 <a id="2.0.1-incubating"></a>

This release fixes issues reported for 2.0.0-rc1-incubating.

 * [#1893](https://github.com/apache/incubator-pulsar/pull/1893) - Fixed issues with Python packages on PyPI
 * [#1797](https://github.com/apache/incubator-pulsar/issues/1797) - Proxy doesn't strip the request
    URL for admin requests correctly
 * [#1862](https://github.com/apache/incubator-pulsar/pull/1862) - Fix REST APIs provided by Pulsar proxy

The complete list of changes can be found at:
https://github.com/apache/incubator-pulsar/milestone/14?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v2.0.1-incubating

### 1.22.1-incubating &mdash; 2018-06-18 <a id="1.22.1-incubating"></a>

This is the sixth release of Apache Pulsar since entering the ASF incubator.

This release addresses issues reported in 1.22.0-incubating version.

 * [#1660](https://github.com/apache/incubator-pulsar/pull/1660) Deadlock while closing non persistent topic
 * [#1591](https://github.com/apache/incubator-pulsar/pull/1591) Deadlock while closing non shared consumer
 * [#1554](https://github.com/apache/incubator-pulsar/pull/1554) Handle invalid mark delete position at managed cursor
 * [#1262](https://github.com/apache/incubator-pulsar/pull/1262) Broker should not start replicator for root partitioned topic
 * [#1662](https://github.com/apache/incubator-pulsar/pull/1662) NPE when cursor failed to close empty subscription
 * [#1370](https://github.com/apache/incubator-pulsar/pull/1370) Relocate service files for shading pulsar-client-admin module
 * [#1265](https://github.com/apache/incubator-pulsar/pull/1265) Fixed lookup redirect logic on Proxyside
 * [#1428](https://github.com/apache/incubator-pulsar/pull/1428) Handle Race condition in concurrent bundle split
 * [#1817](https://github.com/apache/incubator-pulsar/pull/1817) Fixed mem leak when acknowledging while disconnected from broke
 * [#1851](https://github.com/apache/incubator-pulsar/pull/1851) Fixing resource leak due to open file descriptors in SecurityUtility.

The complete list of changes can be found at:
https://github.com/apache/incubator-pulsar/milestone/15?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v1.22.1-incubating

### 2.0.0-rc1-incubating &mdash; 2018-05-29 <a id="2.0.0-rc1-incubating"></a>

This is the fifth release of Apache Pulsar since entering the ASF incubator and the
first time we increase major release number.

There are several new features and major improvements:

 * [Pulsar functions](http://pulsar.apache.org/docs/latest/functions/overview/): Lightweight
   compute framework
 * New type-safe [Java API](http://pulsar.apache.org/docs/latest/clients/Java/) for producer/consumers
 * [Schema registry](http://pulsar.apache.org/docs/v2.0.0-rc1-incubating/getting-started/ConceptsAndArchitecture/#Schemaregistry-ll008b) &mdash; Enforce schema on topics
 * Topic compaction &mdash; Out of band compaction of messages to allow consumer to fetch a
   snapshot with last published message for each message key.
 * Upgraded to [Apache BookKeeper](https://bookkeeper.apache.org/) 4.7.0
 * Performance improvements &mdash; Up to 3x throughput improvements compared to Pulsar-1.22 and
   99.9 Pct publish latencies <10ms
 * [Simplified terminology](http://pulsar.apache.org/docs/v2.0.0-rc1-incubating/getting-started/Pulsar-2.0/#Propertiesversustenants-gh1amh) and admin tools
   - Renamed "property" into "tenants"
   - Short topic names: `my-topic`
   - Topics independent of cluster names: `my-tenant/my-namespace/my-topic`

The complete list of changes can be found at:
https://github.com/apache/incubator-pulsar/milestone/12?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v2.0.0-rc1-incubating


### 1.22.0-incubating &mdash; 2018-03-06 <a id="1.22.0-incubating"></a>

This is the fourth of Apache Pulsar since entering the ASF incubator.

Major changes in this release include:

#### Features
 * [#896](https://github.com/apache/incubator-pulsar/pull/896) PIP-7 Introduce Failure-domain and Anti-affinity-namespace group
 * [#1031](https://github.com/apache/incubator-pulsar/pull/1031) Add optional key/value metadata to producers/consumers
 * [#1129](https://github.com/apache/incubator-pulsar/pull/1129) Added end to end encryption in C++ client
 * [#1151](https://github.com/apache/incubator-pulsar/pull/1151) Added REST handler to create a subscription on a topic
 * [#1087](https://github.com/apache/incubator-pulsar/pull/1087) Add basic authentication plugin
 * [#1200](https://github.com/apache/incubator-pulsar/pull/1200) Add pluggable authorization mechanism
 * [#1208](https://github.com/apache/incubator-pulsar/pull/1208) Add hostname-verification at client tls connection
 * [#950](https://github.com/apache/incubator-pulsar/pull/950) Provided an DCOS Universe package for pulsar
 * [#1046](https://github.com/apache/incubator-pulsar/pull/1046) Introduce config to skip non-recoverable data-ledger
 * [#899](https://github.com/apache/incubator-pulsar/pull/899) Add subscription auth mode by prefix
 * [#1135](https://github.com/apache/incubator-pulsar/pull/1135) Added infinite time retention configuration option

#### Enhancements

 * [#1094](https://github.com/apache/incubator-pulsar/pull/1094) Include BoringSSL native implementation for faster TLS
 * [#1204](https://github.com/apache/incubator-pulsar/pull/1204) Reduce size of buffer used to assemble batches
 * [#930](https://github.com/apache/incubator-pulsar/pull/930) Perform async DNS resolution
 * [#1124](https://github.com/apache/incubator-pulsar/pull/1124) Support Pulsar proxy from C++/Python client library
 * [#1012](https://github.com/apache/incubator-pulsar/pull/1012) Made load shedding for load manager Dynamically configurable
 * [#962](https://github.com/apache/incubator-pulsar/pull/962) Raw Reader for Pulsar Topics
 * [#941](https://github.com/apache/incubator-pulsar/pull/941) Upgraded Jackson version
 * [#1002](https://github.com/apache/incubator-pulsar/pull/1002), [#1169](https://github.com/apache/incubator-pulsar/pull/1169), [#1168](https://github.com/apache/incubator-pulsar/pull/1168) Making Pulsar Proxy more secure
 * [#1029](https://github.com/apache/incubator-pulsar/pull/1029) Fix MessageRouter hash inconsistent on C++/Java client

#### Fixes

 * [#1153](https://github.com/apache/incubator-pulsar/pull/1153) Fixed increase partitions on a partitioned topic
 * [#1195](https://github.com/apache/incubator-pulsar/pull/1195) Ensure the checksum is not stripped after validation in the broker
 * [#1203](https://github.com/apache/incubator-pulsar/pull/1203) Use duplicates when writing from ByteBuf pair to avoid multiple threads issues
 * [#1210](https://github.com/apache/incubator-pulsar/pull/1210) Cancel keep-alive timer task after the proxy switch to TCP proxy
 * [#1170](https://github.com/apache/incubator-pulsar/pull/1170) Upgrade BK version: BK-4.3.1.91-yahoo (fix: stats + DoubleByteBuf)
 * [#875](https://github.com/apache/incubator-pulsar/pull/875) Bug fixes for Websocket proxy

The complete list of changes can be found at:
https://github.com/apache/incubator-pulsar/milestone/11?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v1.22.0-incubating

### 1.21.0-incubating &mdash; 2017-12-17 <a id="1.21.0-incubating"></a>

This is the third of Apache Pulsar since entering the ASF incubator.

Major changes in this release include:

 * [#689](https://github.com/apache/incubator-pulsar/pull/689) Upgrade to Netty 4.1
 * [#846](https://github.com/apache/incubator-pulsar/pull/846) Publish the shaded pulsar-client as the default dependency
 * [#832](https://github.com/apache/incubator-pulsar/pull/832) [#833](https://github.com/apache/incubator-pulsar/pull/833) [#849](https://github.com/apache/incubator-pulsar/pull/849) [#852](https://github.com/apache/incubator-pulsar/pull/852) Enhancements to Kafka API wrapper to have it work with Kafka's own benchmark tools
 * [#836](https://github.com/apache/incubator-pulsar/pull/836) Fix to C++ partitioned consumer client
 * [#822](https://github.com/apache/incubator-pulsar/pull/822) [#826](https://github.com/apache/incubator-pulsar/pull/826) Several fixes and improvements related to the namespace bundles
 * [#848](https://github.com/apache/incubator-pulsar/pull/848) Allow consumer to seek to message id from within Pulsar client
 * [#903](https://github.com/apache/incubator-pulsar/pull/903) PIP-8: Scale Pulsar beyond 1M topics
 * [#824](https://github.com/apache/incubator-pulsar/pull/824) Enable secure replication over TLS
 * [#923](https://github.com/apache/incubator-pulsar/pull/923) Upgrade to bk-4.3.1.83-yahoo to expose journalSyncData option
 * [#807](https://github.com/apache/incubator-pulsar/pull/807) Prevent message duplication when active consumer is changed

Complete list of changes can be found at:
https://github.com/apache/incubator-pulsar/milestone/10?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v1.21.0-incubating

### 1.20.0-incubating &mdash; 2017-08-08 <a id="1.20.0-incubating"></a>

This is the second of Apache Pulsar since entering the ASF incubator.

Major changes in this release include:

 * [#620](https://github.com/apache/incubator-pulsar/pull/620) [#717](https://github.com/apache/incubator-pulsar/pull/717) [#718](https://github.com/apache/incubator-pulsar/pull/718) Reader API support for C++ , Python & Websocket Proxy
 * [#634](https://github.com/apache/incubator-pulsar/pull/634) Added [Message dispatch throttling](https://github.com/apache/incubator-pulsar/wiki/PIP-3:-Message-dispatch-throttling)
 * [#731](https://github.com/apache/incubator-pulsar/pull/731) Added [End to End Encryption](https://github.com/apache/incubator-pulsar/wiki/PIP-4:-Pulsar-End-to-End-Encryption)
 * [#732](https://github.com/apache/incubator-pulsar/pull/732) Support for [Event Time](https://github.com/apache/incubator-pulsar/wiki/PIP-5:-Event-time) for messages
 * [#751](https://github.com/apache/incubator-pulsar/pull/751) Guaranteed [Deduplication of Messages](https://github.com/apache/incubator-pulsar/wiki/PIP-6:-Guaranteed-Message-Deduplication)
 * [#761](https://github.com/apache/incubator-pulsar/pull/761) Kafka API wrapper for Pulsar client library

Complete list of changes can be found at:
https://github.com/apache/incubator-pulsar/milestone/9?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v1.20.0-incubating

### 1.19.0-incubating &mdash; 2017-08-08 <a id="1.19.0-incubating"></a>

This is the first of Apache Pulsar since entering the ASF incubator.

Major changes included in this release are:

 * [#524](https://github.com/apache/incubator-pulsar/pull/524) Moved APIs from `com.yahoo.pulsar` to `org.apache.pulsar`
 * [#548](https://github.com/apache/incubator-pulsar/pull/548) Added stateless [Pulsar proxy](https://github.com/apache/incubator-pulsar/wiki/PIP-1:-Pulsar-Proxy)
 * [#538](https://github.com/apache/incubator-pulsar/pull/538) Support for [non-persistent topics](https://github.com/apache/incubator-pulsar/wiki/PIP-2:-Non-Persistent-topic)
 * [#587](https://github.com/apache/incubator-pulsar/pull/587) Upgraded RocksDB to comply with ASF policy
 * [#507](https://github.com/apache/incubator-pulsar/pull/507) Instrumentation of ZooKeeper client to expose metrics
 * Various fixes for TLS auth in WebSocket proxy

Complete list of changes can be found at:
https://github.com/apache/incubator-pulsar/milestone/8?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v1.19.0-incubating

## Pre-Apache

### 1.18 &mdash; 2017-06-17 <a id="1.18"></a>

Main changes:
 * [#325](https://github.com/apache/incubator-pulsar/pull/325) Add Modular load manager documentation
 * [#329](https://github.com/apache/incubator-pulsar/pull/329) Add api to get list of partitioned topics
 * [#296](https://github.com/apache/incubator-pulsar/pull/296) Added spark streaming custom receiver for pulsar
 * [#317](https://github.com/apache/incubator-pulsar/pull/317) HTTP lookups for c++ client lib
 * [#332](https://github.com/apache/incubator-pulsar/pull/332) Fix: Modular load manager bug fixes
 * [#352](https://github.com/apache/incubator-pulsar/pull/352) Fix: Delete local-policies and invalidate cache when namespace is dele
 * [#356](https://github.com/apache/incubator-pulsar/pull/356) Fix: WebSocket TLS connection bug
 * [#363](https://github.com/apache/incubator-pulsar/pull/363) Use binary protocol lookup for connection between WebSocket proxy and broker
 * [#375](https://github.com/apache/incubator-pulsar/pull/375) Fix: Bug fixes on deadlock while topic loading failure
 * [#376](https://github.com/apache/incubator-pulsar/pull/376) Fix: Avoid incrementing unack-msg count for non-shared sub and not show it on stats
 * [#329](https://github.com/apache/incubator-pulsar/pull/329) Fix: Handle zkCache failures
 * [#387](https://github.com/apache/incubator-pulsar/pull/387) Pass client library version to broker and show on stats
 * [#345](https://github.com/apache/incubator-pulsar/pull/345) Add load shedding strategy
 * [#393](https://github.com/apache/incubator-pulsar/pull/393) Change default mark-delete rate limit from 10s to 1s
 * [#392](https://github.com/apache/incubator-pulsar/pull/392) Upgrade to netty-4.0.46
 * [#366](https://github.com/apache/incubator-pulsar/pull/366) NonDurable cursor for managed ledger
 * [#371](https://github.com/apache/incubator-pulsar/pull/371) Introduce topic reader in client API
 * [#341](https://github.com/apache/incubator-pulsar/pull/341) Add stats and monitoring for websocket proxy
 * [#299](https://github.com/apache/incubator-pulsar/pull/299) Add api to increase partitions of existing non-global partitioned-topic
 * [#294](https://github.com/apache/incubator-pulsar/pull/294) Add endpoint to fetch stats for Prometheus
 * [#440](https://github.com/apache/incubator-pulsar/pull/440) Enable PulsarAdmin to trust multiple certificates
 * [#442](https://github.com/apache/incubator-pulsar/pull/442) Fix: Remove broker weights for ModularLoadManager
 * [#446](https://github.com/apache/incubator-pulsar/pull/446) Fix: Recover cursor with correct readPosition and replay unackedMessages
 * [#441](https://github.com/apache/incubator-pulsar/pull/441) Set Block If queue full to false by default
 * [#447](https://github.com/apache/incubator-pulsar/pull/447) Fix: DoubleByteBuf to send large size messages in TLS mode
 * [#443](https://github.com/apache/incubator-pulsar/pull/443) Add topic termination option
 * [#436](https://github.com/apache/incubator-pulsar/pull/436) Added ZooKeeper instrumentation for enhanced stats
 * [#448](https://github.com/apache/incubator-pulsar/pull/448) WebSocket proxy should not make a consumer/producer when authorization is failed
 * [#443](https://github.com/apache/incubator-pulsar/pull/443) Add Docker images definition and instruction to deploy on Kubernetes
 * [#474](https://github.com/apache/incubator-pulsar/pull/474) Fix: message rate out with batches to count messages/s
 * [#482](https://github.com/apache/incubator-pulsar/pull/482) Allow client(producer/consumer) to check topic stats
 * [#468](https://github.com/apache/incubator-pulsar/pull/468) Pulsar Python client library
 * [#386](https://github.com/apache/incubator-pulsar/pull/386) Increment bookkeeper version to 4.3.1.69-yahoo

Full list of changes: https://github.com/yahoo/pulsar/milestone/7?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v1.18

### 1.17.5 &mdash; 2017-05-02 <a id="1.17.5"></a>

 * [#343](https://github.com/apache/incubator-pulsar/pull/343) Fix ModularLoadManager to select broker from current available-broker list
 * [#384](https://github.com/apache/incubator-pulsar/pull/384) Fix Send replay entries read callback from background thread, to avoid recursive stack calls
 * [#390](https://github.com/apache/incubator-pulsar/pull/390) Fix Shaded AsyncHttpClient in pulsar client
 * [#374](https://github.com/apache/incubator-pulsar/pull/374) Fix Remove Exceptionally Completed Topic Futures

https://github.com/apache/incubator-pulsar/releases/tag/v1.17.5

### 1.17.4 &mdash; 2017-04-25 <a id="1.17.4"></a>

 * [#362](https://github.com/apache/incubator-pulsar/pull/362) Fix add timeout on blocking ZookeeperCache get call
 * [#375](https://github.com/apache/incubator-pulsar/pull/375) Fix possible deal lock on topic loading if broker fails to get MLConfiguration from zk
 * [#377](https://github.com/apache/incubator-pulsar/pull/377) Fix zkCache error handling and zk-callback processing on separate dedicated thread

https://github.com/apache/incubator-pulsar/releases/tag/v1.17.4

### 1.17.3 &mdash; 2017-04-20 <a id="1.17.3"></a>

 * [#367](https://github.com/apache/incubator-pulsar/pull/367) Fix dispatcher correctly finds available consumer from list of shared-subscription consumers

https://github.com/apache/incubator-pulsar/releases/tag/v1.17.3

### 1.17.2 &mdash; 2017-04-06 <a id="1.17.2"></a>

 * [#327](https://github.com/apache/incubator-pulsar/pull/327) Create znode for dynamic configuration if not present
 * [#336](https://github.com/apache/incubator-pulsar/pull/336) Fix prevent creation of topic when bundle is disable
 * [#338](https://github.com/apache/incubator-pulsar/pull/338) Fix deserialize load report based on load-manager

https://github.com/apache/incubator-pulsar/releases/tag/v1.17.2

### 1.17.1 &mdash; 2017-03-30 <a id="1.17.1"></a>

 * [#326](https://github.com/apache/incubator-pulsar/pull/326) Fix memory leak while duplicating entry data from existing entry

https://github.com/apache/incubator-pulsar/releases/tag/v1.17.1

### 1.17 &mdash; 2017-03-30 <a id="1.17"></a>

Main changes:

 * [#188](https://github.com/apache/incubator-pulsar/pull/188) Pulsar Dashboard
 * [#276](https://github.com/apache/incubator-pulsar/pull/276) Broker persist individually deleted messages
 * [#282](https://github.com/apache/incubator-pulsar/pull/282) Support binary format to persist managed-ledger info in ZK
 * [#292](https://github.com/apache/incubator-pulsar/pull/292) Added REST and CLI tool to expose ManagedLedger metadata
 * [#285](https://github.com/apache/incubator-pulsar/pull/285) Add documentation in japanese
 * [#178](https://github.com/apache/incubator-pulsar/pull/178) Add Athenz authentication plugin
 * [#186](https://github.com/apache/incubator-pulsar/pull/186) Update Broker service configuration dynamically
 * [#215](https://github.com/apache/incubator-pulsar/pull/215) Fix Broker disconnects unsupported batch-consumer on batch-message topic
 * [#165](https://github.com/apache/incubator-pulsar/pull/165) Message dispatching on consumer priority-level
 * [#303](https://github.com/apache/incubator-pulsar/pull/303) Introduce new load manager implementation
 * [#306](https://github.com/apache/incubator-pulsar/pull/306) Add topic loading throttling at broker
 * [#302](https://github.com/apache/incubator-pulsar/pull/302) Update BK version to 4.3.1.60-yahoo to include: 64bit ledger-ids, fix: memory leak on read-only bookie and datasketches concurrency issue
 * [#216](https://github.com/apache/incubator-pulsar/pull/216) Binary proto api to get consumer stats
 * [#225](https://github.com/apache/incubator-pulsar/pull/225) Server lookup throttling
 * [#182](https://github.com/apache/incubator-pulsar/pull/182) Client lookup request throttling and server-error handling
 * [#265](https://github.com/apache/incubator-pulsar/pull/265) Fix client handling on http server error
 * [#204](https://github.com/apache/incubator-pulsar/pull/204) Fix discovery service redirection
 * [#311](https://github.com/apache/incubator-pulsar/pull/311) Fix netty package conflict at binary distribution
 * [#221](https://github.com/apache/incubator-pulsar/pull/221) Fixed race condition on client reconnection logic
 * [#239](https://github.com/apache/incubator-pulsar/pull/239) Fix replicator handling on closed cursor
 * [#318](https://github.com/apache/incubator-pulsar/pull/318) GC improvements: Recyclable entry and reduce collection on stats generation

Full list of changes: https://github.com/apache/incubator-pulsar/milestone/3?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v1.17

### 1.16.5 &mdash; 2017-03-10 <a id="1.16.5"></a>

 * [#311](https://github.com/apache/incubator-pulsar/pull/311) Exclude netty individual jars from binary distribution. This issue was causing binary distribution to have conflicting netty dependencies.

https://github.com/apache/incubator-pulsar/releases/tag/v1.16.5

### 1.16.4 &mdash; 2017-03-10 <a id="1.16.4"></a>

 * [#265](https://github.com/apache/incubator-pulsar/pull/265) Fix client closes http-connection on internal-server error
 * [#283](https://github.com/apache/incubator-pulsar/pull/283) Fix recycle keep alive command-object properly
 * [#284](https://github.com/apache/incubator-pulsar/pull/284) Reduce usage of collections in managed-ledger metrics-generation to reduce GC impact

https://github.com/apache/incubator-pulsar/releases/tag/v1.16.4

### 1.16.3 &mdash; 2017-03-01 <a id="1.16.3"></a>

 * [#275](https://github.com/apache/incubator-pulsar/pull/275) Fix for Infinite loop in PersistentReplicator.startProducer()

https://github.com/apache/incubator-pulsar/releases/tag/v1.16.3

### 1.16.2 &mdash; 2017-02-24 <a id="1.16.2"></a>

 * [#250](https://github.com/apache/incubator-pulsar/pull/250) : Disconnect consumers without closing dispatcher on cursor-reset

https://github.com/apache/incubator-pulsar/releases/tag/v1.16.2

### 1.16.1 &mdash; 2017-02-24 <a id="1.16.1"></a>

 * [#221](https://github.com/apache/incubator-pulsar/pull/221) Fixed race condition while creating client connection
 * [#223](https://github.com/apache/incubator-pulsar/pull/223) Fixed broker's direct memory usage count
 * [#220](https://github.com/apache/incubator-pulsar/pull/220) Fixed stuck replicator producer on backlog quota exception
 * [#239](https://github.com/apache/incubator-pulsar/pull/239) Fixed replicator stop reading on already closed cursor

https://github.com/apache/incubator-pulsar/releases/tag/v1.16.1

### 1.16 &mdash; 2017-02-02 <a id="1.16"></a>

Main changes:
 * [#76](https://github.com/apache/incubator-pulsar/pull/76) Async Zookeeper cache implementation
 * [#105](https://github.com/apache/incubator-pulsar/pull/105) Support topic lookup using pulsar binary protocol
 * [#164](https://github.com/apache/incubator-pulsar/pull/164) Fixed handling failure of unloading namespace bundle
 * [#166](https://github.com/apache/incubator-pulsar/pull/166) Support websocket proxy deployment without passing globalZK
 * [#161](https://github.com/apache/incubator-pulsar/pull/161) Fixed avoiding creation of duplicate replicator
 * [#160](https://github.com/apache/incubator-pulsar/pull/160) Add support uri encoding on broker admin rest api
 * [#143](https://github.com/apache/incubator-pulsar/pull/143) Include DataSketches metrics provider for bookie stats
 * [#127](https://github.com/apache/incubator-pulsar/pull/127) Updated BK-4.3.1.45/47-yahoo to include bookie/bookkeeper-client bug-fixes and DataSketch metrics provider
 * [#124](https://github.com/apache/incubator-pulsar/pull/124) Consumer-stats: Add blockedConsumer flag
 * [#95](https://github.com/apache/incubator-pulsar/pull/95) Consumer-stats: Add message redelivery rate
 * [#123](https://github.com/apache/incubator-pulsar/pull/123) Fixed Batch message replication
 * [#106](https://github.com/apache/incubator-pulsar/pull/106) Fixed Partitioned consumer should avoid blocking call to fill shared queue
 * [#139](https://github.com/apache/incubator-pulsar/pull/139) Support online consumer cursor reset
 * [#187](https://github.com/apache/incubator-pulsar/pull/187) Support custom advertised address in pulsar standalone

Full list of changes: https://github.com/yahoo/pulsar/milestone/2?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v1.16

### 1.15.7 &mdash; 2017-01-25 <a id="1.15.7"></a>

 * [#174](https://github.com/apache/incubator-pulsar/pull/174) Handling bundle unloading failure

https://github.com/apache/incubator-pulsar/releases/tag/v1.15.7

### 1.15.6 &mdash; 2017-01-20 <a id="1.15.6"></a>

 * [#171](https://github.com/apache/incubator-pulsar/pull/171) Fix: Consumer redelivery should not wipeout availablePermits

https://github.com/apache/incubator-pulsar/releases/tag/v1.15.6

### 1.15.5 &mdash; 2017-01-03 <a id="1.15.5"></a>

 * [#159](https://github.com/apache/incubator-pulsar/pull/159) Fix: Replicator-cleanup while closing replicator at broker.
 * [#160](https://github.com/apache/incubator-pulsar/pull/160) Fix: Http lookup for topic with special character

https://github.com/apache/incubator-pulsar/releases/tag/v1.15.5

### 1.15.4 &mdash; 2016-12-14 <a id="1.15.4"></a>

 * [#146](https://github.com/apache/incubator-pulsar/pull/146) Fix: Partitioned consumer can consume messages with receiverQueueSize 1.

https://github.com/apache/incubator-pulsar/releases/tag/v1.15.4

### 1.15.3 &mdash; 2016-12-13 <a id="1.15.3"></a>

 * [#145](https://github.com/apache/incubator-pulsar/pull/145) Fixed issue Partitioned-consumer aggregate messages without blocking internal listener thread

https://github.com/apache/incubator-pulsar/releases/tag/v1.15.3

### 1.15.2 &mdash; 2016-11-03 <a id="1.15.2"></a>

 * [#102](https://github.com/apache/incubator-pulsar/pull/102) Fixed issue with message dispatching while message-replay at broker

https://github.com/apache/incubator-pulsar/releases/tag/v1.15.2

### 1.15.1 &mdash; 2016-10-27 <a id="1.15.1"></a>

 * [#89](https://github.com/apache/incubator-pulsar/pull/89) Fixed issue with replication in a mixed
   environment with 1.14 and 1.15 brokers

https://github.com/apache/incubator-pulsar/releases/tag/v1.15.1

### 1.15 &mdash; 2016-10-18 <a id="1.15"></a>

- [#39](https://github.com/apache/incubator-pulsar/pull/39) Updated BookKeeper version to 4.3.1.41-yahoo to include bookie storage improvements
- [#17](https://github.com/apache/incubator-pulsar/pull/17) Fixed memory leak in stats generation buffer rollover
- [#27](https://github.com/apache/incubator-pulsar/pull/27) Fixed issues with discovery service component when HTTPS is enabled
- [#43](https://github.com/apache/incubator-pulsar/pull/43) Add end-to-end crc32c checksum verification on message header and payload, rather than just payload. Support for intel hardware instructions to speed up computation.
- [#26](https://github.com/apache/incubator-pulsar/pull/26) Added ability to configure the address that the broker uses to advertise itself. Needed in cases where the public hostname/ip is different than the machine interface ip (eg: in AWS EC2 instances).
- [#38](https://github.com/apache/incubator-pulsar/pull/38) Prevent message-replay of already acknowledged messages
- [#51](https://github.com/apache/incubator-pulsar/pull/51) Per message unacknowledged redelivery. When ack-timeout is configured, only request redelivery of messages that effectively have the timeout expired, instead of all the messages dispatched to the consumer.
- [#48](https://github.com/apache/incubator-pulsar/pull/48) Add unacknowledged messages threshold to stop delivery to consumer that are not acknowledging messages
- [#59](https://github.com/apache/incubator-pulsar/pull/59) Added admin method to do a one-time messages time expiration for a given subscription (independently from the TTL configured at the namespace level)

Full list of changes: https://github.com/apache/incubator-pulsar/milestone/1?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v1.15

### 1.14 &mdash; 2016-08-31 <a id="1.14"></a>

First public release of Pulsar

https://github.com/apache/incubator-pulsar/releases/tag/v1.14
