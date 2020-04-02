
## Apache Pulsar Release Notes

### 2.5.0 &mdash; 2019-12-06 <a id="2.5.0"></a>

#### PIPs

- [PIP-41] Introduce Protocol Handler API [#5130](https://github.com/apache/pulsar/pull/5130) [#5743](https://github.com/apache/pulsar/pull/5473) 
- [PIP-45] Pluggable metadata interface [#5330](https://github.com/apache/pulsar/pull/5330) 
- [PIP-44] Separate schema compatibility checker for producer and consumer [#5227](https://github.com/apache/pulsar/pull/5227)
- [PIP-43] Producer send messages with different schema [#5141](https://github.com/apache/pulsar/issues/5141) [#5517](https://github.com/apache/pulsar/pull/5517) 
- [PIP-51] Introduce sticky consumer [#5388](https://github.com/apache/pulsar/pull/5388)
- [PIP-38] Support batch receive in java client. [#4621](https://github.com/apache/pulsar/pull/4621)
- [PIP-52] PIP-52: [pulsar-sever] Add support of dispatch throttling relative to publish-rate [#5797](https://github.com/apache/pulsar/pull/5797)

#### Fixes

- [Broker] Avoid retrying deleting namespace when topic is already deleted/fenced [#4665](https://github.com/apache/pulsar/pull/4665)
- [Broker] Fix expiry monitor to continue on non-recoverable error [#4818](https://github.com/apache/pulsar/pull/4818) 
- [Broker] fix ns-isolation api to fetch policy for specific broker [#5314](https://github.com/apache/pulsar/pull/5314)
- [Broker] external protocols not set to local broker data [#5749](https://github.com/apache/pulsar/pull/5479)
- [Broker] Add handle exception KeeperException.BadVersionException  [#5563](https://github.com/apache/pulsar/pull/5563)
- [Broker] Fix message deduplicate issue while using external sequence id with batch produce [#5491](https://github.com/apache/pulsar/pull/5491)
- [Broker] Remove cursor while remove non-durable subscription [#5719](https://github.com/apache/pulsar/pull/5719) 
- [Broker] Fix potential read 0 entries cause dispatcher stop dispatch [#5894](https://github.com/apache/pulsar/pull/5894)
- [Proxy] Proxy doesn't use the right ca certicate to connect to brokers [#5971](https://github.com/apache/pulsar/pull/5971)
- [Client] Add SentConnectFrame state check when running `handleError` [#5913](https://github.com/apache/pulsar/pull/5913)

#### Enhancements

- [Zookeeper] Bump zookeeper to version 3.5.6 [#5043](https://github.com/apache/pulsar/pull/5043)
- [BookKeeper] Upgrade bk version to 4.10.0 [#5607](https://github.com/apache/pulsar/pull/5607)
- [Broker] Process requests asynchronously on some REST APIs [4765](https://github.com/apache/pulsar/pull/4765) [4778](https://github.com/apache/pulsar/pull/4778) [4795](https://github.com/apache/pulsar/pull/4795) 
- [Broker] Fixes not owned bundles got selected when broker overloading [#5002](https://github.com/apache/pulsar/pull/5002)
- [Broker] Support update partition for global topic  [#5306](https://github.com/apache/pulsar/pull/5306)
- [Broker] Ensure the handling of PartitionMetadataRequest is async end-to-end [#5307](https://github.com/apache/pulsar/pull/5307)
- [Broker] Allow to automatically assign TCP ports when starting a broker  [#3555](https://github.com/apache/pulsar/pull/3555) 
- [Broker] Introduce publish rate-limiting on topic [#3986](https://github.com/apache/pulsar/pull/3986)
- [Broker] Add publish rate limit for each broker to avoid OOM [#5710](https://github.com/apache/pulsar/pull/5710)
- [Broker] Allow for namespace default of offload threshold [#5872](https://github.com/apache/pulsar/pull/5872)
- [Broker] Avoid unsafe split when validate hostname which might be ipv6 address [#5713](https://github.com/apache/pulsar/pull/5713)
- [Broker] Support batch authorization of partitioned topic [#5767](https://github.com/apache/pulsar/pull/5767)
- [Client][Java] Introduce `batchingMaxBytes` setting in pulsar producer [#5045](https://github.com/apache/pulsar/pull/5045)
- [Client][Java] Add epoch for connection handler to handle create producer timeout [#5571](https://github.com/apache/pulsar/pull/5571)
- [Performance] Reduce char[] creation on jvm heap [#5055](https://github.com/apache/pulsar/pull/5055)
- [CLI] Add a broker tool for operations of a specific broker [#5768](https://github.com/apache/pulsar/pull/5768)
- [CLI] Validate topic name before creating partition/non partition topic via admin cli [#5148](https://github.com/apache/pulsar/pull/5148)
- [CLI] Make PulsarClusterMetadataSetup idempotent [#5879](https://github.com/apache/pulsar/pull/5879)
- [CLI] Allow for topic deletions with regex consumers [#5230](https://github.com/apache/pulsar/pull/5230)

#### Stats & Monitoring 

- [Broker] Added delayed messages in Prometheus when using namespace-level metrics aggregation [#4691](https://github.com/apache/pulsar/pull/4691)
- [Dashboard] Increasing Dashboard consumerName field to 256 varchar [4716](https://github.com/apache/pulsar/pull/4716)
- [Dashboard] integrate peek into messages page [#4966](https://github.com/apache/pulsar/pull/4966)
- [Dashboard] Support parse batch entry [#4992](https://github.com/apache/pulsar/pull/4992)

#### Security

- [Broker] Add broker-bookie mTLS support [#5042](https://github.com/apache/pulsar/pull/5042)

#### Tiered Storage

- HDFS Offloader [#4403](https://github.com/apache/pulsar/pull/4403)
- Fix the problem of repeated storage of offload driver metadata [#5834](https://github.com/apache/pulsar/pull/5834)

#### Pulsar Schema

- [Broker] Pulsar schema api should respect to namespace level compatibility [#4821](https://github.com/apache/pulsar/issues/4821)
- [Client][Java] NPE is thrown when a consumer consumes a partitioned topic with struct schema
 [#4960](https://github.com/apache/pulsar/pull/4960)
- [Broker] Add compatibility check for primitive schema types [#5051](https://github.com/apache/pulsar/pull/5051)
- [Broker] Support uploading key/value schema using Pulsar admin [#5000](https://github.com/apache/pulsar/pull/5000)
- [Client][Java] Schema support encoding & encoding ByteBuf [#5123](https://github.com/apache/pulsar/pull/5123)

#### Pulsar IO

- [Broker] Support reload Source and Sink for Pulsar IO [5008](https://github.com/apache/pulsar/pull/5008)
- [Connector] Added Kinesis Source Connector [#3784](https://github.com/apache/pulsar/pull/3784)
- [Connector] Add a source connector for MongoDB [#5316](https://github.com/apache/pulsar/pull/5316)
- [Connector] Support CDC Connector for MongoDB [#5590](https://github.com/apache/pulsar/pull/5590)
- [Connector] Improve hbase sink performance [#5705](https://github.com/apache/pulsar/pull/5705)

#### Pulsar Functions

- [Function Worker] Allow resource overcommitting when running functions in Kubernetes [4829](https://github.com/apache/pulsar/pull/4829)
- [Function Worker] Make Function Authentication Provider pluggable [#5404](https://github.com/apache/pulsar/pull/5404)
- [Function Worker] Added deletion of state for Functions [#5469](https://github.com/apache/pulsar/pull/5469) 
- [Function Worker] Distribute the CA for KubernetesSecretsTokenAuthProvider [#5398](https://github.com/apache/pulsar/pull/5398)
- [Function Runtime] Function runtime pluggable [#5463](https://github.com/apache/pulsar/pull/5463)
- [Function Runtime] Allow functions to pass runtime specific options [#5400](https://github.com/apache/pulsar/pull/5400)

#### Pulsar SQL

- Support for other schema name separators in pulsar SQL [4732](https://github.com/apache/pulsar/issues/4732)
- Reuse ManagedLedgerFactory instances across SQL queries [4813](https://github.com/apache/pulsar/pull/4813) 
- Using pulsar SQL query messages will appear `NoSuchLedger` when zk root directory changed [#5001](https://github.com/apache/pulsar/pull/5001)

#### Java client

- Not allow use acknowledgeCumulative on Key_shared subscription [#5339](https://github.com/apache/pulsar/pull/5339)
- Fix bug that beforeConsume() of interceptor is not called when receiver queue size is 0 [#5777](https://github.com/apache/pulsar/pull/5777)

#### Go client

- Use buffered channels to avoid blocking on callback  [#5336](https://github.com/apache/pulsar/pull/5336)

#### C++ client

- Change state_ to closed when resultOk is returned [#5446](https://github.com/apache/pulsar/pull/5446)
- Expose redelivery count [#5677](https://github.com/apache/pulsar/pull/5677)

#### Adaptors

- Add support of pulsar-kafka-adapter for kafka-0.9 API [#4886](https://github.com/apache/pulsar/pull/4886)
- Add support of pulsar-kafka-adapter for kafka-0.8 API [#4797](https://github.com/apache/pulsar/pull/4797) 
- Make client keepalive interval configurable on pulsar-client-kafka [#5131](https://github.com/apache/pulsar/pull/5131)

#### Transaction

> The development of Pulsar Transaction is still ongoing

- [Buffer] Add new marker to show which message belongs to transaction [#4776](https://github.com/apache/pulsar/pull/4776)
- [Buffer] Add data ledger position in txn commit marker [#4826](https://github.com/apache/pulsar/pull/4826)
- [Buffer] Add basic operation of transaction [#4738](https://github.com/apache/pulsar/pull/4738)
- [Buffer] Add new commands for the transaction [#4866](https://github.com/apache/pulsar/pull/4866)
- [Protocol] Add default handler to handle transaction related commands [#4891](https://github.com/apache/pulsar/pull/4891)
- [Client] Introduce Transaction Client API [#4952](https://github.com/apache/pulsar/pull/4952)
- [Client] Add transaction coordinator client [#4953](https://github.com/apache/pulsar/pull/4953)
- [Broker] Ownership change listeners [#5457](https://github.com/apache/pulsar/pull/5457)
- [Coordinator] Bootstrap pulsar system namespace and create TC assign topic [#5515](https://github.com/apache/pulsar/pull/5515)
- [Coordinator] Add transaction metadata store service [#5504](https://github.com/apache/pulsar/pull/5504) 

For a complete list of issues fixed, see

https://github.com/apache/pulsar/milestone/22?closed=1

https://github.com/apache/pulsar/releases/tag/v2.5.0 

### 2.4.2 &mdash; 2019-12-04 <a id="2.4.2"></a>

#### Fixes

* Fixed don't set interrupt flag again after catching interrupt exception in Pulsar Client [#5643](https://github.com/apache/pulsar/pull/5643)
* Fixed data is not deleted after expiration due to connected readers [#5621](https://github.com/apache/pulsar/pull/5621)
* Fixed the go client docs missing in website [#5595](https://github.com/apache/pulsar/pull/5595)
* Fixed schema def build error with protobuf schema [#5569](https://github.com/apache/pulsar/pull/5569)
* Fixed docs about reset cursor [#5551](https://github.com/apache/pulsar/pull/5551)
* Fixed repeated initialization of connectorsManager [#5545](https://github.com/apache/pulsar/pull/5545)
* Fixed Functions unnecessarily restart during FunctionRuntimeManager init phase [#5527](https://github.com/apache/pulsar/pull/5527)
* Fixed list non-persistent topics shows the persistent topics [#5502](https://github.com/apache/pulsar/pull/5502)
* Return after triggering callback with empty result [#5500](https://github.com/apache/pulsar/pull/5500)
* Fixed dispatcher skipping delivery of a batch during concurrent replays [#5499](https://github.com/apache/pulsar/pull/5499)
* Fixed reader_listener option for Python API [#5487](https://github.com/apache/pulsar/pull/5487)
* Fixed wrongly report "3600 messages have timed-out" [#5477](https://github.com/apache/pulsar/pull/5477)
* Fixed broken custom auth-provider that uses authenticationData [#5462](https://github.com/apache/pulsar/pull/5462)
* Fixed negative ack tracker constructor sequence [#5453](https://github.com/apache/pulsar/pull/5453)
* Fixed StringSchema static initialization  [#5445](https://github.com/apache/pulsar/pull/5445)
* Fixed message corruption on OOM for batch messages [#5443](https://github.com/apache/pulsar/pull/5443)
* Fixed couple functions related integration tests [#5434](https://github.com/apache/pulsar/pull/5434)
* Fixed bug that namespace policies does not take effect due to NPE [#5408](https://github.com/apache/pulsar/pull/5408)
* Fixed race condition : Failed to read-more entries on dispatcher [#5391](https://github.com/apache/pulsar/pull/5391)
* Fixed potential deadlock that can occur in addConsumer [#5371](https://github.com/apache/pulsar/pull/5371)
* Fixed proxy to be able to re-send request body [#5361](https://github.com/apache/pulsar/pull/5361)
* Fixed pulsar can't load the customized SerDe [#5357](https://github.com/apache/pulsar/pull/5357)
* Fixed instability in Pulsar Function window integration test [#5337](https://github.com/apache/pulsar/pull/5337)
* Fixed bk write failure, use signals to resume writing [#5322](https://github.com/apache/pulsar/pull/5322)
* Fixed bad_weak_ptr error when closing producer [#5315](https://github.com/apache/pulsar/pull/5315)
* Fixed typo(massage->message) causing prometheus metrics display error [#5299](https://github.com/apache/pulsar/pull/5299)
* Fixed invalidate cache on zk-cache timeout [#5298](https://github.com/apache/pulsar/pull/5298)
* Fixed memory leak caused by not being executed ClientConnection destructor [#5286](https://github.com/apache/pulsar/pull/5286)
* Fixed producer blocked after send an over size message while batch enabled [#5282](https://github.com/apache/pulsar/pull/5282)
* Fixed race condition while triggering message redelivery after an ack-timeout event [#5276](https://github.com/apache/pulsar/pull/5276)
* Fixed behavior when getting a key from functions state that doesn't exist [#5272](https://github.com/apache/pulsar/pull/5272)
* Fixed Cmake to build _pulsar for osx [#5263](https://github.com/apache/pulsar/pull/5263)
* Fixed client backoff setting does not take effect [#5261](https://github.com/apache/pulsar/pull/5261)
* Fixed memory leak caused by deadline_timer holding object reference [#5246](https://github.com/apache/pulsar/pull/5246)
* Fixed in Message Deduplication that may cause incorrect client/broker interaction [#5243](https://github.com/apache/pulsar/pull/5243)
* Fixed bug that fails to search namespace bundle due to NPE [#5191](https://github.com/apache/pulsar/pull/5191)
* Fixed bug that message delivery stops after resetting cursor for failover subscription [#5185](https://github.com/apache/pulsar/pull/5185)
* Fixed exception type check order bug [#5174](https://github.com/apache/pulsar/pull/5174)
* Fixed spark receiver to account for all the consumer config options [#5152](https://github.com/apache/pulsar/pull/5152)
* Fixed broker fails to start with function worker enabled and broker client using TLS [#5151](https://github.com/apache/pulsar/pull/5151)
* Fixed deadlock when resetting cursor [#5136](https://github.com/apache/pulsar/pull/5136)
* Fixed windowed functions were broken when we changed java function instance to use classloaders [#5117](https://github.com/apache/pulsar/pull/5117)
* Fixed storage size always 0 without subscription [#5108](https://github.com/apache/pulsar/pull/5108)

#### Enhancements

* Add subscribe position param for consumer of sink [#5532](https://github.com/apache/pulsar/pull/5532)
* Efficiency improvements for delay delivery tracker [#5498](https://github.com/apache/pulsar/pull/5498)
* Add is_read_compacted to create_reader() in python API [#5483](https://github.com/apache/pulsar/pull/5483)
* Make some common use method of ManagedLedger public [#5472](https://github.com/apache/pulsar/pull/5472)
* Avoid leak on publish failure on batch message [#5442](https://github.com/apache/pulsar/pull/5442)
* Record message failure and avoid exiting from process on publish failure [#5441](https://github.com/apache/pulsar/pull/5441)
* Add support for partitioned topic consumer seek by time [#5435](https://github.com/apache/pulsar/pull/5435)
* Add default loader for latest pyyaml [#5432](https://github.com/apache/pulsar/pull/5432)
* Trim messages which less than mark delete position for message redelivery [#5378](https://github.com/apache/pulsar/pull/5378)
* Make skip all messages async [#5375](https://github.com/apache/pulsar/pull/5375)
* Set default ensemble size to 2 in service conf, to match broker.conf [#5339](https://github.com/apache/pulsar/pull/5339)
* Only seek when reading unexpected entry [#5356](https://github.com/apache/pulsar/pull/5356)
* Don't require both region and endpoint to be specified [#5355](https://github.com/apache/pulsar/pull/5355)
* If cursor is not durable, close dispatcher when all consumers are removed from subscription [#5340](https://github.com/apache/pulsar/pull/5340)
* Disable stickyRead by default [#5321](https://github.com/apache/pulsar/pull/5321)
* Allow to specify delivery delay in C++ client [#5317](https://github.com/apache/pulsar/pull/5317)
* Add debug log + fix thread-factory name  [#5302](https://github.com/apache/pulsar/pull/5302)
* Don't attempt to append on read-only cursor ledger [#5297](https://github.com/apache/pulsar/pull/5297)
* Close previous dispatcher when subscription type changes [#5288](https://github.com/apache/pulsar/pull/5288)
* Improve error handling logic for effectively once [#5271](https://github.com/apache/pulsar/pull/5271)
* Upgrade dependencies for security fixes [#5232](https://github.com/apache/pulsar/pull/5232)
* Ensure consumer background tasks are cancelled after subscribe failures [#5204](https://github.com/apache/pulsar/pull/5204)
* Added missing enum value KeyShared on the python wrapper [#5196](https://github.com/apache/pulsar/pull/5196)
* Make some member variables of Dispatcher volatile [#5193](https://github.com/apache/pulsar/pull/5193)
* Ensure getting list of topics for namespace is handled asynchronously [#5188](https://github.com/apache/pulsar/pull/5188)
* Close RateLimiter instance [#5155](https://github.com/apache/pulsar/pull/5155)
* Throw an error if the key was not specified for querying state [#5145](https://github.com/apache/pulsar/pull/5145)
* Allow configuring region aware placement related settings [#5100](https://github.com/apache/pulsar/pull/5100)
* DeleteBookieRack should remove the rack info from zookeeper [#5084](https://github.com/apache/pulsar/pull/5084)
* Use "info" as the default root logger level [#5079](https://github.com/apache/pulsar/pull/5079)
* Modify BatcherBuilder interface and it's subs to implement java.io.Serializable, otherwise java.io.NotSerializableException occurs when we use plusar-flink-connector [#5068](https://github.com/apache/pulsar/pull/5068)
* Don't return DEFAULT_RACK if ZkBookieRackAffinityMapping can't resolve network location [#5067](https://github.com/apache/pulsar/pull/5067)
* Reload zk cache asynchronously [#5049](https://github.com/apache/pulsar/pull/5049)
* Add different cache flags to ConcurrentOpenLongPairRangeSet for size() and toString() [#5040](https://github.com/apache/pulsar/pull/5040)
* Introduce number of threads in perf producer program [#5036](https://github.com/apache/pulsar/pull/5036)
* Completing connector configuration [#4999](https://github.com/apache/pulsar/pull/4999)
* Add checkstyle validation and fix style violations in the common module [#4989](https://github.com/apache/pulsar/pull/4989)
* Trim deleted entries after recover cursor [#4987](https://github.com/apache/pulsar/pull/4987)
* Expose getLastMessageId method in ConsumerImpl [#4911](https://github.com/apache/pulsar/pull/4911)
* Add a documentation page for metrics reference [#4910](https://github.com/apache/pulsar/pull/4910)
* Provide a convenient method for C++ client producer batch container [#4885](https://github.com/apache/pulsar/pull/4885)
* Add schema admin api get schema info with schema version [#4877](https://github.com/apache/pulsar/pull/4877)
* Return Message ID for send for cpp and cgo client [#4811](https://github.com/apache/pulsar/pull/4811)
* Add the schema admin api [#4800](https://github.com/apache/pulsar/pull/4800)
* Clarify how retention interacts with readers [#4780](https://github.com/apache/pulsar/pull/4780)
* Get schema info with topic partition [#4751](https://github.com/apache/pulsar/pull/4751)
* Remove failed stale producer from the connection [#4741](https://github.com/apache/pulsar/pull/4741)
* Update logic for picking active consumer for failover subscription on non-partitioned topic [#4604](https://github.com/apache/pulsar/pull/4604)

### 2.4.1 &mdash; 2019-08-30 <a id="2.4.1"></a>

#### Fixes

* Fixed wrong serialize of batchBuilder in ProducerConfigurationData [#4620](https://github.com/apache/pulsar/pull/4620)
* Fixed SchemaInfo properties losing when registering schema using admin api [#4617](https://github.com/apache/pulsar/pull/4617)
* Fixed wrong topic domain returned by get partitioned topic [#4613](https://github.com/apache/pulsar/pull/4613)
* Fixed pulsar-dashboard wrong count number of bundles [#4637](https://github.com/apache/pulsar/pull/4637)
* Fixed partitionIndex error in consumer for a single partitioned topic [#4591](https://github.com/apache/pulsar/pull/4591)
* Fixed deadlock on get-status rest-api call in broker [#4616](https://github.com/apache/pulsar/pull/4616)
* Fixed deadlock in subscribeAsync demo [#4649](https://github.com/apache/pulsar/pull/4649)
* Fixed C++ client lookup error over HTTP in standalone [#4625](https://github.com/apache/pulsar/pull/4625)
* Fixed NPE at managed-ledger when fetch reader internal-stats [#4615](https://github.com/apache/pulsar/pull/4615)
* Fixed C++ client producer sendAsync() hang when no enough batched message [#4657](https://github.com/apache/pulsar/pull/4657)
* Fixed issue when submitting NAR via file url [#4577](https://github.com/apache/pulsar/pull/4577)
* Renamed C++ logger enum names to avoid conflicts with compiler macros [#4664](https://github.com/apache/pulsar/pull/4664)
* Fixed leaking of pulsar-io-influxdb in distribution [#4678](https://github.com/apache/pulsar/pull/4678)
* Fixed the default port for https and http in admin client [#4623](https://github.com/apache/pulsar/pull/4623)
* Fixed wrong schema delete when checking compatibility [#4669](https://github.com/apache/pulsar/pull/4669)
* Fixed `docker/publish.sh` doesn't work for pulsar-all and pulsar-standalone images [#4705](https://github.com/apache/pulsar/pull/4705)
* Fixed integration-test failure when execute pip3 install pulsar_client [#4754](https://github.com/apache/pulsar/pull/4754)
* Added log folder in `pulsa-function-go` back [#4736](https://github.com/apache/pulsar/pull/4736)
* Fixed Pulsar SQL NPE when predicate pushdown for publish_time [#4744](https://github.com/apache/pulsar/pull/4744)
* Fixed redelivered message logic of partition topic [#4653](https://github.com/apache/pulsar/pull/4653)
* Fixed C++ log level names in Log4cxxLogger [#4735](https://github.com/apache/pulsar/pull/4735)
* Fixed go function package not executable [#4743](https://github.com/apache/pulsar/pull/4743)
* Added anonymous role to proxy configuration [#4733](https://github.com/apache/pulsar/pull/4733)
* Added kubernetes namespace to function instance url [#4701](https://github.com/apache/pulsar/pull/4701)
* Fixed go function not parse conf content first [#4746](https://github.com/apache/pulsar/pull/4746)
* Made PulsarKafkaProducer thread safe [#4745](https://github.com/apache/pulsar/pull/4745)
* Fixed messages not get acked if there is not sink topic [#4815](https://github.com/apache/pulsar/pull/4815)
* Fixed bug when function package jar/py/go and runtime is not set [#4814](https://github.com/apache/pulsar/pull/4814)
* Fixed topic loading in rest-api not time out in zooKeeperOperationTimeoutSeconds [#4805](https://github.com/apache/pulsar/pull/4805)
* Fixed default retention policy miss upload to zk [#4810](https://github.com/apache/pulsar/pull/4810)
* Added checking function implements correct interface [#4844](https://github.com/apache/pulsar/pull/4844)
* Fixed retention size policy bug [#4825](https://github.com/apache/pulsar/pull/4825)
* Catch throwable in interceptors of consumer and producer [#4860](https://github.com/apache/pulsar/pull/4860)
* Fixed first position in managedLedger is ahead of the last [#4853](https://github.com/apache/pulsar/pull/4853)
* Fixed concurrent access of `uninitializedCursors` in `ManagedLedgerImpl.asyncOpenCursor` [#4837](https://github.com/apache/pulsar/pull/4837)
* Fixed schema not found handling in pulsar-sql [#4890](https://github.com/apache/pulsar/pull/4890)
* Fixed requests not respect no_proxy env variable in dashboard [#4867](https://github.com/apache/pulsar/pull/4867)
* Fixed broken replication msg to specific cluster [#4930](https://github.com/apache/pulsar/pull/4930)
* Fixed dashboard peek parse message error [#4918](https://github.com/apache/pulsar/pull/4918)
* Fixed StructSchema reader cache loading logic [#4962](https://github.com/apache/pulsar/pull/4962)
* Fixed fd leakage in FunctionActioner.downloadFile [#4970](https://github.com/apache/pulsar/pull/4970)
* Fixed python function str bytes convert in example custom_object_function.py [#4946](https://github.com/apache/pulsar/pull/4946)
* Fixed NPE while cleaning up namespace node [#4965](https://github.com/apache/pulsar/pull/4965)
* Fixed the getSchema logic in pulsar proxy [#4975](https://github.com/apache/pulsar/pull/4975)
* Fixed warning by add default loader for latest pyyaml [#4974](https://github.com/apache/pulsar/pull/4974)
* Fixed snappy compressor compile error in pulsar-client-cpp [#4972](https://github.com/apache/pulsar/pull/4972)
* Reinitialize certain components for externally managed runtimes when moving functions [#5007](https://github.com/apache/pulsar/pull/5007)
* Upgraded jackson-databind [#5011](https://github.com/apache/pulsar/pull/5011)
* Fixed the problem of missing dependencies less in docker [#5034](https://github.com/apache/pulsar/pull/5034)
* Fixed duplicated Prometheus TYPE in broker metrics [#4183](https://github.com/apache/pulsar/pull/4183)
* Fixed pulsar sink and source state by init state earlier [#5046](https://github.com/apache/pulsar/pull/5046)

#### Enhancements

* Re-factored Component implementation [#4541](https://github.com/apache/pulsar/pull/4541)
* Provided a clock for generating publish timestamp for producers [#4562](https://github.com/apache/pulsar/pull/4562)
* Reduced unnecessary track message calls [#4595](https://github.com/apache/pulsar/pull/4595)
* Supported dynamic configure with escape char [#4611](https://github.com/apache/pulsar/pull/4611)
* Added null check for function/source/sink configs [#4627](https://github.com/apache/pulsar/pull/4627)
* Added delete dynamic config api [#4614](https://github.com/apache/pulsar/pull/4614)
* Made broker replication mtls configuration dynamic [#4609](https://github.com/apache/pulsar/pull/4609)
* Added authorization to function worker REST endpoints [#4628](https://github.com/apache/pulsar/pull/4628)
* Improved and add authorization to function download and upload [#4644](https://github.com/apache/pulsar/pull/4644)
* Allowed consumer retrieve the sequence id that the producer set [#4645](https://github.com/apache/pulsar/pull/4645)
* Added perPartition parameter to partitioned-stats API [#4639](https://github.com/apache/pulsar/pull/4639)
* Supported Pulsar schema for pulsar kafka client wrapper [#4534](https://github.com/apache/pulsar/pull/4534)
* Supported delete and update event for JDBC Sink [#4358](https://github.com/apache/pulsar/pull/4358)
* Cleaned up tests in the presto module [#4683](https://github.com/apache/pulsar/pull/4683)
* Added allowAutoTopicCreation to broker.conf and related configuration [#4694](https://github.com/apache/pulsar/pull/4694)
* Changed to use classloaders to load Java functions [#4685](https://github.com/apache/pulsar/pull/4685)
* Removed fixed server type check in kerberos [#4758](https://github.com/apache/pulsar/pull/4758)
* Changed type of publish_time to timestamp [#4757](https://github.com/apache/pulsar/pull/4757)
* Added read-timeout to admin-request [#4762](https://github.com/apache/pulsar/pull/4762)
* Added checking of deleted schema when adding schema [#4731](https://github.com/apache/pulsar/pull/4731)
* Added strand to C++ client for exclusive control [#4750](https://github.com/apache/pulsar/pull/4750)
* Added support to create partitioned topic with 1 partition [#4764](https://github.com/apache/pulsar/pull/4764)
* Added getters and setters to PulsarService & BrokerService [#4709](https://github.com/apache/pulsar/pull/4709)
* Added configure ack-timeout tick time [#4760](https://github.com/apache/pulsar/pull/4760)
* Added options to rewrite namespace delimiter for pulsar sql [#4749](https://github.com/apache/pulsar/pull/4749)
* Made Pulsar SQL supports pulsar's primitive schema [#4728](https://github.com/apache/pulsar/pull/4728)
* Added basic authentication capabilities to Pulsar SQL [#4779](https://github.com/apache/pulsar/pull/4779)
* Improved SchemaInfoProvider to fetch schema info asynchronously [#4836](https://github.com/apache/pulsar/pull/4836)
* Supported KeyValue schema use AUTO_CONSUME as key/value schema [#4839](https://github.com/apache/pulsar/pull/4839)
* Provided action type for insert in pulsar-io-jdbc [#4862](https://github.com/apache/pulsar/pull/4862)
* Made partition as internal column in pulsar-sql [#4888](https://github.com/apache/pulsar/pull/4888)
* Added option to disable authentication for proxy /metrics [#4921](https://github.com/apache/pulsar/pull/4921)



### 2.4.0 &mdash; 2019-06-30 <a id="2.4.0"></a>

#### PIPs

* PIP-26: [Added support for delayed message delivery](https://github.com/apache/pulsar/wiki/PIP-26%3A-Delayed-Message-Delivery)
[#4062](https://github.com/apache/pulsar/pull/4062), note that messages are only delayed on shared subscriptions.
* PIP-28: [Pulsar Proxy Gateway Improvement](https://github.com/apache/pulsar/wiki/PIP-28%3A-Pulsar-Proxy-Gateway-Improvement) [#3915](https://github.com/apache/pulsar/pull/3915)
* PIP-29: [One package for both pulsar client and pulsar admin](https://github.com/apache/pulsar/wiki/PIP-29%3A-One-package-for-both-pulsar-client-and-pulsar-admin) [#3662](<https://github.com/apache/pulsar/pull/3662>)
* PIP-30: [Change authentication provider API to support mutual authentication for Kerberos Authentication](https://github.com/apache/pulsar/wiki/PIP-30%3A-change-authentication-provider-API-to-support-mutual-authentication) [#3677](https://github.com/apache/pulsar/pull/3677)
[#3821](https://github.com/apache/pulsar/pull/3821) [#3997](https://github.com/apache/pulsar/pull/3997)
* PIP-32: [Go Function API, Instance and LocalRun](https://github.com/apache/pulsar/wiki/PIP-32%3A-Go-Function-API%2C-Instance-and-LocalRun) [#3854](https://github.com/apache/pulsar/pull/3854) [#4008](https://github.com/apache/pulsar/pull/4008) [#4174](https://github.com/apache/pulsar/pull/4174)
* PIP-33: [Replicated subscriptions](https://github.com/apache/pulsar/wiki/PIP-33%3A-Replicated-subscriptions) to keep subscription state in-sync across multiple geographical regions [#4299](https://github.com/apache/pulsar/pull/4299) [#4340](https://github.com/apache/pulsar/pull/4340) [#4354](https://github.com/apache/pulsar/pull/4354) [#4396](https://github.com/apache/pulsar/pull/4396)
* PIP-34: [Key_Shared](https://github.com/apache/pulsar/wiki/PIP-34%3A-Add-new-subscribe-type-Key_Failover) subscription, [Architecture for Key_Shared](http://pulsar.apache.org/docs/en/concepts-messaging/#key_shared)  [#4079](https://github.com/apache/pulsar/pull/4079) [#4120](https://github.com/apache/pulsar/pull/4120) [#4335](https://github.com/apache/pulsar/pull/4335) [#4372](https://github.com/apache/pulsar/pull/4372) [#4407](https://github.com/apache/pulsar/pull/4407) [#4406](https://github.com/apache/pulsar/pull/4406) [4462](https://github.com/apache/pulsar/pull/4462)
* PIP-36: [Support set message size in broker.conf](https://github.com/apache/pulsar/wiki/PIP-36%3A-Max-Message-Size) [#4247](https://github.com/apache/pulsar/pull/4247)

#### Fixes

* Fix possible message loss using peer-cluster feature [#3426](https://github.com/apache/pulsar/pull/3426)
* Fix pulsar standalone does not read zk port from conf/standalone.conf [#3790](https://github.com/apache/pulsar/pull/3790)
* Fix some issues of ZkIsolatedBookieEnsemblePlacementPolicy [#3917](https://github.com/apache/pulsar/pull/3917) [#3918](https://github.com/apache/pulsar/pull/3918)
* Fix NPE when unload non-existent topic [#3946](https://github.com/apache/pulsar/pull/3946)
* Fix race condition while deleting global topic [#4173](https://github.com/apache/pulsar/pull/4173)
* Fix deadlock on skip messages [#4411](https://github.com/apache/pulsar/pull/4411)
* Fix NPE when closing batch during a reconnection [#4427](https://github.com/apache/pulsar/pull/4427)
* Fix race condition of read-timeout task in managed ledger [#4437](https://github.com/apache/pulsar/pull/4437)
* Disable sticky read by default [#4526](https://github.com/apache/pulsar/pull/4526)
* Fix race condition between timeout-task and add-call complete [#4455](https://github.com/apache/pulsar/pull/4455)

#### Enhancements

* Optimize message replay for large backlog consumer [#3732](https://github.com/apache/pulsar/pull/3732)
* Added support for websocket produce/consume command [#3835](https://github.com/apache/pulsar/pull/3835)
* Added support for TTL config in broker.conf [#3898](https://github.com/apache/pulsar/pull/3898)
* Reduce memory used in ClientCnx for pending lookups [#4104](https://github.com/apache/pulsar/pull/4104)
* Reduce number of hashmap sections for ledger handles cache [#4102](https://github.com/apache/pulsar/pull/4102)
* Added backlog and offloaded size in Prometheus stats [#4150](https://github.com/apache/pulsar/pull/4150)
* Added support for configure the managed ledger cache eviction frequency [#4066](https://github.com/apache/pulsar/pull/4066)
* Added support to avoid payload copy when inserting into managed ledger cache [#4197](https://github.com/apache/pulsar/pull/4197)
* Added support to cache unack-messageId into OpenRangeSet [#3819](https://github.com/apache/pulsar/pull/3819)
* Added support configure static PulsarByteBufAllocator to handle OOM errors [#4196](https://github.com/apache/pulsar/pull/4196)
* Auto refresh new tls certs for jetty webserver [#3645](<https://github.com/apache/pulsar/pull/3645>)
* Create non-persistent topic by pulsar-admin/rest api [#3625](<https://github.com/apache/pulsar/pull/3625>)
* Consumer priority-level in Failover subscription [#2954](https://github.com/apache/pulsar/pull/2954)

#### Security

* Added support for other algorithms in token auth [#4528](https://github.com/apache/pulsar/pull/4528)

#### Namespace Policies

* Added support for tenant based bookie isolation [#3933](https://github.com/apache/pulsar/pull/3933)
* Added support for secondary bookie isolation group at namespace [#4458](https://github.com/apache/pulsar/pull/4458)
* Added support for secondary bookie-isolation-group  [#4261](https://github.com/apache/pulsar/pull/4261)
* Added support for replicator rate limit between clusters [#4273](https://github.com/apache/pulsar/pull/4273)
* Disable backlog quota check by default [#4320](https://github.com/apache/pulsar/pull/4320)

#### Tiered Storage

* Added support for Pulsar SQL to read data from tiered storage [#4045](https://github.com/apache/pulsar/pull/4045)

#### Pulsar Schema

* Added schema versioning to support multi version messages produce and consume [#3876](https://github.com/apache/pulsar/pull/3876) [#3670](https://github.com/apache/pulsar/pull/3670) [#4211](https://github.com/apache/pulsar/pull/4211) [#4325](https://github.com/apache/pulsar/pull/4325) [#4548](https://github.com/apache/pulsar/pull/4548)
* Added `TRANSITIVE` schema check strategies to support compatibility check over all existing schemas  [#4214](https://github.com/apache/pulsar/pull/4214)
* Added schema data validator [#4360](https://github.com/apache/pulsar/pull/4360)
* Added support for delete schema when deleting a topic [#3941](https://github.com/apache/pulsar/pull/3941)
* Added generic record builder [#3690](https://github.com/apache/pulsar/pull/3690)

#### Pulsar IO

* Added IO connector for flume source and sink [#3597](<https://github.com/apache/pulsar/pull/3597>)
* Added IO connector for redis sink [#3700](https://github.com/apache/pulsar/pull/3700)
* Added IO connector for solr sink [#3885](https://github.com/apache/pulsar/pull/3885)
* Hide kafka-connecter details for easy use debezium connector [#3825](https://github.com/apache/pulsar/pull/3825)
* Added  IO connector for debezium PostgreSQL source [#3924](https://github.com/apache/pulsar/pull/3924)
* Enhancements for RabbitMQ source configuration [#3937](https://github.com/apache/pulsar/pull/3937)
* Added IO connector for RabbitMQ sink [#3967](https://github.com/apache/pulsar/pull/3967)
* Added IO connector for InfluxDB sink [#4017](https://github.com/apache/pulsar/pull/4017)

#### Pulsar functions

* Added support for authentication [#3735](https://github.com/apache/pulsar/pull/3735) [#3874](https://github.com/apache/pulsar/pull/3874) [#4198](https://github.com/apache/pulsar/pull/4198)
* Fix NPE when stats manager not initialized [#3891](https://github.com/apache/pulsar/pull/3891)
* Added async state manipulation methods [#3798](https://github.com/apache/pulsar/pull/3978)
* Fix fail to update functions in effectively-once mode [#3993](https://github.com/apache/pulsar/issues/3993)
* Added labels to function statefulsets and services [#4038](https://github.com/apache/pulsar/pull/4038)
* Added support for set key for message when using function publish [#4005](https://github.com/apache/pulsar/pull/4005)
* Use negative acknowledge to instead ackTimeout [#4103](https://github.com/apache/pulsar/pull/4103)
* Fix backward compatibility with 2.2 auth not working [#4241](https://github.com/apache/pulsar/pull/4241)

#### Java client

* Added negative acks [#3703](https://github.com/apache/pulsar/pull/3703)
* Added support for backoff strategy configuration [#3848](https://github.com/apache/pulsar/pull/3848)
* Added support for configure TypedMessageBuilder through a Map conf object [#4015](https://github.com/apache/pulsar/pull/4015)
* Added interceptor for negative ack send [#3962](https://github.com/apache/pulsar/pull/3962)
* Added support for seek operate on reader [#4031](https://github.com/apache/pulsar/pull/4031)
* Store key part of KeyValue schema into pulsar message keys [#4117](https://github.com/apache/pulsar/pull/4117)
* Added interceptor for ack timeout  [#4300](https://github.com/apache/pulsar/pull/4300)
* Added support for snappy compression [#4259](https://github.com/apache/pulsar/pull/4259)
* Added support for key based batcher [#4435](https://github.com/apache/pulsar/pull/4435)

#### Python client

* Added negative acks [#3816](https://github.com/apache/pulsar/pull/3816) 
* Added support for snappy compression [#4319](https://github.com/apache/pulsar/pull/4319)

#### Go client

* Added negative acks [#3817](https://github.com/apache/pulsar/pull/3817)
* Added support for go schema [#3904](https://github.com/apache/pulsar/pull/3904)
* Added support for snappy compression [#4319](https://github.com/apache/pulsar/pull/4319)
* Added support for Key_Shared subscription [#4465](https://github.com/apache/pulsar/pull/4465)

#### C++ client

* Added negative acks [#3750](https://github.com/apache/pulsar/pull/3750)
* Fix ack timeout when subscribing to regex topic [#3897](https://github.com/apache/pulsar/pull/3879)
* Added support for Key_Shared subscription [#4366](https://github.com/apache/pulsar/pull/4366)

#### Adaptors

* In Kafka client wrapper, added some configurations [#3753](https://github.com/apache/pulsar/pull/3753) [#3797](https://github.com/apache/pulsar/pull/3797) [#3843](https://github.com/apache/pulsar/pull/3843) [#3887](https://github.com/apache/pulsar/pull/3887) [#3991](https://github.com/apache/pulsar/pull/3911)
* In Apache Flink connector, allow to specify a custom Pulsar producer [#3894](https://github.com/apache/pulsar/pull/3894) and client authentication [#3949](https://github.com/apache/pulsar/pull/3949)
* In Apache Flink connector, added support for accept ClientConfigurationData, ProducerConfigurationData, ConsumerConfigurationData [#4232](https://github.com/apache/pulsar/pull/4232)
* In Apache Storm connector, fix NPE while emitting next tuple [#3991](https://github.com/apache/pulsar/pull/3991) and some add some enhancements [#4280](https://github.com/apache/pulsar/pull/4280) [#4239](https://github.com/apache/pulsar/pull/4239)   [#4238](https://github.com/apache/pulsar/pull/4238) [#4236](https://github.com/apache/pulsar/pull/4236) [#4495](https://github.com/apache/pulsar/pull/4495) [#4494](https://github.com/apache/pulsar/pull/4494)

For a complete list of issues fixed, see

https://github.com/apache/pulsar/milestone/20?closed=1

https://github.com/apache/pulsar/releases/tag/v2.4.0

### 2.3.2 &mdash; 2019-05-30 <a id="2.3.2"></a>

#### Fixes

* Validate admin operation on topic with authoritative parameter [#4270](https://github.com/apache/pulsar/pull/4270)
* fix bug with source local run [#4278](https://github.com/apache/pulsar/pull/4278)
* fix cannot use size (type _Ctype_int) as type _Ctype_ulong [#4212](https://github.com/apache/pulsar/pull/4212)
* Fix segfault in c++ producer [#4219](https://github.com/apache/pulsar/pull/4219)
* AlwaysCompatible doesn't use AlwaysSchemaValidator in 2.3.1 component/schemaregistry [#4181](https://github.com/apache/pulsar/pull/4181)
* Avoid potentially blocking method during topic ownership check [#4190](https://github.com/apache/pulsar/pull/4190)
* [pulsar-broker]Fix: client-producer can't reconnect due to failed producer-future on cnx cache [#4138](https://github.com/apache/pulsar/pull/4138)
* Removing # TYPE comment from topic metrics in Prometheus [#4136](https://github.com/apache/pulsar/pull/4136)
* For functions metrics in prometheus also remove TYPE [#4081](https://github.com/apache/pulsar/pull/4091)
* Fix: set receive queue size for sinks [#4091](https://github.com/apache/pulsar/pull/4091)
* Fix: Exception when switch cluster from auth enabled to auth disabled [#4069](https://github.com/apache/pulsar/pull/4069)
* Fix update cli source sink [#4061](https://github.com/apache/pulsar/pull/4061)
* Fix connectors nested configs [#4067](https://github.com/apache/pulsar/pull/4067)
* For functions metrics, avoid having HELP [#4029](https://github.com/apache/pulsar/pull/4029)
* Fix Python functions state which is completely broken [#4027](https://github.com/apache/pulsar/pull/4027)
* [issue #3975] Bugfix NPE on non durable consumer [#3988](https://github.com/apache/pulsar/pull/3988)
* Fix: Function auth should ignore exception because it might be anonymous user [#4185](https://github.com/apache/pulsar/pull/4185)
* [pulsar-function] fix worker-stats broken admin-api [#4068](https://github.com/apache/pulsar/pull/4068)
* fix errors in sql doc [#4030](https://github.com/apache/pulsar/pull/4030)
* Fix the swagger files generated by removing troublesome class [#4024](https://github.com/apache/pulsar/pull/4024)
* [pulsar-function] fix broken backward compatibility with v1-namespace while registering function [#4224](https://github.com/apache/pulsar/pull/4224)
* Revert dup consumer and related code [#4142](https://github.com/apache/pulsar/pull/4142)
* [issue 4274][pulsar-io]Add double quotation marks for metrics with remote_cluster [#4295](https://github.com/apache/pulsar/pull/4295)

#### Enhancements

* By default, auto configure the size of Bookie read/write cache [#4297](https://github.com/apache/pulsar/pull/4297)
* Upgrade to BookKeeper 4.9.2 [#4288](https://github.com/apache/pulsar/pull/4288)
* [pulsar-function] support bookie authentication from function-worker [#4088](https://github.com/apache/pulsar/pull/4088)
* Optimizing performance for Pulsar function archive download [#4082](https://github.com/apache/pulsar/pull/4082)
* allow users to update output topics for functions and sources [#4092](https://github.com/apache/pulsar/pull/4092)
* improve data-generator source performance [#4058](https://github.com/apache/pulsar/pull/4058)
* [client] Set actual topic name to partitioned consumer [#4064](https://github.com/apache/pulsar/pull/4064)
* ack records in datagenerator print sink [#4052](https://github.com/apache/pulsar/pull/4052)
* [security] Upgrade athenz libraries [#4056](https://github.com/apache/pulsar/pull/4056)
* [python client] Handle subrecords in JsonSchema encoding [#4023](https://github.com/apache/pulsar/pull/4023)
* [Issue 3987][pulsar-broker]Handle config is null when create tenant [#4019](https://github.com/apache/pulsar/pull/4019)
* Add bookkeeper client version constraint [#4013](https://github.com/apache/pulsar/pull/4013)
* Improve error handling for triggering function when there is a schema mismatch [#3995](https://github.com/apache/pulsar/pull/3995)
* [pulsar-broker] add producer/consumer id in error-logging [#3961](https://github.com/apache/pulsar/pull/3961)


### 2.3.1 &mdash; 2019-04-12 <a id="2.3.1"></a>

#### Fixes

 * Fixed C++ batch acks tracker to evict message from sendList array. This was causing a slowdown in
   C++ consumers [#3618](https://github.com/apache/pulsar/pull/3618)
 * Allow publishing messages >5MB with batching (when they compress to <5MB) [#3673](https://github.com/apache/pulsar/pull/3673) and [#3718](https://github.com/apache/pulsar/pull/3718)
 * Use at least 8 threads in Jetty thread pool. This fixed deadlocks in Jetty requests handling [#3776](https://github.com/apache/pulsar/pull/3776)
 * Fixed Reader.HasNext() in Go client [#3764](https://github.com/apache/pulsar/pull/3764)
 * Fixed increasing consumer permits after ack dedup operation. [#3787](https://github.com/apache/pulsar/pull/3787)
 * Set the dedup cursor as "inactive" after recovery [#3612](https://github.com/apache/pulsar/pull/3612)
 * Fix read batching message by pulsar reader [#3830](https://github.com/apache/pulsar/pull/3830)
 * Fix submit function with code specified via URL [#3934](https://github.com/apache/pulsar/pull/3934)
 * Fixed reader reading from a partition [#3960](https://github.com/apache/pulsar/pull/3960)
 * Fixed issue with Authorization header missing after client gets redirected [#3869](https://github.com/apache/pulsar/pull/3869)

#### Enhancements
 * Added `producer.flush()` on Python [#3685](https://github.com/apache/pulsar/pull/3685)
 * Introduced schema builder to define schema [#3682](https://github.com/apache/pulsar/pull/3682)
 * Support passing schema definition for JSON and AVRO schemas [#3766](https://github.com/apache/pulsar/pull/3766)
 * Exposing InitialPosition setting in Python consumer [#3714](https://github.com/apache/pulsar/pull/3714)


 For a complete list of issues fixed, see

 https://github.com/apache/pulsar/milestone/21?closed=1

 https://github.com/apache/pulsar/releases/tag/v2.3.1


### 2.3.0 &mdash; 2019-02-20 <a id="2.3.0"></a>

#### General

 * Support for schema definitions in the Pulsar [Python client library](https://pulsar.apache.org/docs/client-libraries-python/#schema)
 * PIP-25: [Token based authentication](https://pulsar.apache.org/docs/security-token-client/) [#2888](https://github.com/apache/pulsar/pull/2888),
   [#3067](https://github.com/apache/pulsar/pull/3067) and [#3089](https://github.com/apache/pulsar/pull/3089)
 * Updated to [Apache BookKeeper 4.9.0](https://bookkeeper.apache.org/docs/4.9.0/overview/releaseNotes/)
 * ZStandard compression codec [#3159](https://github.com/apache/pulsar/pull/3159). Note that
   when a producer choose Zstd compression, a consumer will need to be at least at version 2.3.0
   to be able to correctly receive the messages.
 * Support for Java 11 [#3006](https://github.com/apache/pulsar/pull/3006)
 * Added `Schema.AUTO_PRODUCE` type to allow to publish serialized data and validate it against the
   topic schema [#2685](https://github.com/apache/pulsar/pull/2685)
 * Added `Schema.KeyValue` to allow for schema to be validated on message keys as well as payloads. [#2885](https://github.com/apache/pulsar/pull/2885)
 * Support TLS authentication and authorization in standalone mode [#3360](https://github.com/apache/pulsar/pull/3360)
 * When creating namespace, use local cluster by default [#3571](https://github.com/apache/pulsar/pull/3571)
 * Tag BookKeeper ledgers created by Pulsar with topic/subscription names for info/debug purposes
   [#3525](https://github.com/apache/pulsar/pull/3525)
 * Enabled sticky reads in BooKeeper reads to increase IO efficiency with read-ahead [#3569](https://github.com/apache/pulsar/pull/3569)
 * Several optimization in Pulsar SQL Presto connector ([#3128](https://github.com/apache/pulsar/pull/3128),
    [#3135](https://github.com/apache/pulsar/pull/3135), [#3139](https://github.com/apache/pulsar/pull/3139),
    [#3144](https://github.com/apache/pulsar/pull/3144), [#3143](https://github.com/apache/pulsar/pull/3143))
 * Configure Pulsar broker data cache automatically from JVM settings [#3573](https://github.com/apache/pulsar/pull/3573)
 * Reuse the SSL context objects [#3550](https://github.com/apache/pulsar/pull/3550)
 * Automatic schema update can be disabled through admin interface [#2691](https://github.com/apache/pulsar/pull/2691)
 * Support Dead-Letter-Queue from WebSocket proxy [#2968](https://github.com/apache/pulsar/pull/2968)
 * Pull-mode for WebSocket proxy [#3058](https://github.com/apache/pulsar/pull/3058)
 * Export Jetty stats to Prometheus [#2804](https://github.com/apache/pulsar/pull/2804)
 * Added stats for Pulsar proxy [#2740](https://github.com/apache/pulsar/pull/2740)
 * Allow subscribers to access subscription admin-api [#2981](https://github.com/apache/pulsar/pull/2981)
 * Make brokers read on closest Bookie in a multi-region deployment [#3171](https://github.com/apache/pulsar/pull/3171)

#### Fixes
 * Fixed deadlock in reusing ZooKeeper event thread [#3591](https://github.com/apache/pulsar/pull/3591)
 * In functions log topic appender, don't set producer name [#3544](https://github.com/apache/pulsar/pull/3544)
 * When cursor recovery encounters empty cursor ledger, fallback to latest snapshot [#3487](https://github.com/apache/pulsar/pull/3487)
 * Fixed C++ regex-consumer when using HTTP service URL [#3407](https://github.com/apache/pulsar/pull/3407)
 * Fix race condition: broker not scheduling read for active consumer [#3411](https://github.com/apache/pulsar/pull/3411)

#### Pulsar IO
 * Added Debezium connector for Change-Data-Capture into Pulsar [#2791](https://github.com/apache/pulsar/pull/2791)
 * Added MongoDB connector [#3561](https://github.com/apache/pulsar/pull/3561)
 * Added Elastic Search connector [#2546](https://github.com/apache/pulsar/pull/2546)
 * Added HBase sink [#3368](https://github.com/apache/pulsar/pull/3368)
 * Added Local files connector [#2869](https://github.com/apache/pulsar/pull/2869)
 * Report source/sink stats in Prometheus [#3261](https://github.com/apache/pulsar/pull/3261)
 * Allow filtering in Twitter Firehose connector [#3298](https://github.com/apache/pulsar/pull/3298)
 * Sources/Sinks can be launched using fat jars as well [#3166](https://github.com/apache/pulsar/pull/3166)

#### Pulsar Functions
 * Added Kubernetes runtime [#1950](https://github.com/apache/pulsar/pull/1950)
 * Secrets interface [#2826](https://github.com/apache/pulsar/pull/2826)
 * Cleanup subscriptions when deleting functions [#3299](https://github.com/apache/pulsar/pull/3299)
 * Add Windowfunction interface to functions api [#3324](https://github.com/apache/pulsar/pull/3324)
 * Support for accessing state in Python [#2714](https://github.com/apache/pulsar/pull/2714)
 * Support submitting Python functions as wheel file
 * Support submitting Python functions as Zip file with dependencies included [#3321](https://github.com/apache/pulsar/pull/3321)
 * Add minimum amount of resources to run setting for functions [#3536](https://github.com/apache/pulsar/pull/3536)
 * Fixed the behavior of Function start/stop [#3477](https://github.com/apache/pulsar/pull/3477)

#### Java client
 * Moved Pulsar v1 client API into separate artifact [#3228](https://github.com/apache/pulsar/pull/3228).<br />
   Applications that are using the Pulsar v1 API, deprecated since 2.0 release, need to update the
   Maven dependency to use the `pulsar-client-1x` artifact instead of `pulsar-client`. Eg.
   ```xml
   <dependency>
       <groupId>org.apache.pulsar</groupId>
       <artifactId>pulsar-client-1x</artifactId>
       <version>2.3.0</version>
   </dependency>
   ```
 * Fixed shading issues with Javadoc bundled in client jars by separating the API in a different
   Maven module [#3309](https://github.com/apache/pulsar/pull/3309)
 * Improve Javadocs [#3592](https://github.com/apache/pulsar/pull/3592)
 * Support specifying multiple hosts in pulsar service url and web url [#3249](https://github.com/apache/pulsar/pull/3249)  
 * Automatically discover when partitions on a topic are increased [#3513](https://github.com/apache/pulsar/pull/3513)
 * Added `Client.getPartitionsForTopic()` [#2972](https://github.com/apache/pulsar/pull/2972)
   ([Javadoc](https://pulsar.apache.org/api/client/org/apache/pulsar/client/api/PulsarClient.html#getPartitionsForTopic-java.lang.String-))
 * Added `Consumer.pauseMessageListener()` and `Consumer.resumeMessageListener()` [#2961](https://github.com/apache/pulsar/pull/2961)
 * Removed shading relocations for Circe-checksum and lz4 libraries, to ensure native libraries
   are correctly loaded when using shaded client lib. [#2191](https://github.com/apache/pulsar/pull/2191)

#### Python client
 * Fixed `Message.properties()` [#3595](https://github.com/apache/pulsar/pull/3595)

#### Go client
 * Added `Producer.flush()` to flush all outstanding messages [#3469](https://github.com/apache/pulsar/pull/3469)
 * Support `Consumer.Seek()` [#3478](https://github.com/apache/pulsar/pull/3478)
 * Added `Message.Topic()` [#3346](https://github.com/apache/pulsar/pull/3346)
 * Allow to specify `SubscriptionInitPos` option in `ConsumerOptions` [#3588](https://github.com/apache/pulsar/pull/3588)
 * Added TLS hostname verification [#3580](https://github.com/apache/pulsar/pull/3580)
 * Allow to link statically against `libpulsar.a`[#3488](https://github.com/apache/pulsar/pull/3488)
 * Expose `Producer.LastSequenceID()` and `Message.SequenceID()` [#3416](https://github.com/apache/pulsar/pull/3416)

#### C++ client
 * Enable batching by default when using `sendAsync()` [#2949](https://github.com/apache/pulsar/pull/2949)
 * Allow to specify schema info in Avro format [#3354](https://github.com/apache/pulsar/pull/3354)
 * Added `Producer.flush()` to flush all outstanding messages [#3020](https://github.com/apache/pulsar/pull/3020)
 * Added TLS hostname verification [#2475](https://github.com/apache/pulsar/pull/2475)
 * Allow to specify `SubscriptionInitialPosition` [#3567](https://github.com/apache/pulsar/pull/3567)
 * Added `Message.getTopicName()` [#3326](https://github.com/apache/pulsar/pull/3326)
 * Added `Cosnsumer.receiveAsync()` [#3389](https://github.com/apache/pulsar/pull/3389)
 * Build `libpulsar.a` with all required dependencies [#3488](https://github.com/apache/pulsar/pull/3488)
 * Removed Boost from Pulsar API headers [#3374](https://github.com/apache/pulsar/pull/3374)

#### Adaptors

 * Kafka client wrapper, added support for explicit partitioning and custom partitioner [#3462](https://github.com/apache/pulsar/pull/3462)
 * Support config `auto.offset.reset` to Pulsar KafkaConsumer [#3273](https://github.com/apache/pulsar/pull/3273)
 * In Apache Flink connector, added support for Batch Sink API ([2979#](https://github.com/apache/pulsar/pull/2979),
   [#3039](https://github.com/apache/pulsar/pull/3039) and [#3046](https://github.com/apache/pulsar/pull/3046))
 * Added [Java batch examples](https://github.com/apache/pulsar/tree/master/examples/flink/src/main/java/org/apache/flink/batch/connectors/pulsar/example) for Flink adaptor
 * Added [Java streaming examples](https://github.com/apache/pulsar/tree/master/examples/flink/src/main/java/org/apache/flink/streaming/connectors/pulsar/example) for Flink adaptor
 * Added [Scala examples](https://github.com/apache/pulsar/tree/master/examples/flink/src/main/scala/org/apache/flink/batch/connectors/pulsar/example) for Flink adaptor

For a complete list of issues fixed, see

https://github.com/apache/pulsar/milestone/18?closed=1

https://github.com/apache/pulsar/releases/tag/v2.3.0



### 2.2.1 &mdash; 2018-12-31 <a id="2.2.1"></a>

This release includes fixes for 2.2.0 release. In particular:

* Fixed issue when proxy HTTP admin API requests Pulsar proxy [#3022](https://github.com/apache/pulsar/pull/3022)

* Fixed `Consumer.unsubscribe()` in Python client library [#3093](https://github.com/apache/pulsar/pull/3093)

* Fixed ZLib decompression in C++ client [#2854](https://github.com/apache/pulsar/pull/2854)

* Fixed Pulsar functions context publish in Python [#2844](https://github.com/apache/pulsar/pull/2844)

For a complete list of issues fixed, see

https://github.com/apache/pulsar/milestone/19?closed=1

https://github.com/apache/incubator-pulsar/releases/tag/v2.2.1


### 2.2.0 &mdash; 2018-10-24 <a id="2.2.0"></a>

This is the first release of Pulsar as an Apache Top Level Project

This is a feature release, including several new features, improvements and fixes for  issues reported for 2.1.1-incubating.
 
* [Pulsar Java Client Interceptors](https://github.com/apache/pulsar/pull/2471)

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
