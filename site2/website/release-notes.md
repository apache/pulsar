
## Apache Pulsar Release Notes

### 2.8.1 &mdash; 2021-09-10 <a id=“2.8.1”></a>

### Broker
- Fix the issue of all web threads get stuck when deleting a namespace [#11596](https://github.com/apache/pulsar/pull/11596)
- Improve error logs in BacklogQuotaManager [#11469](https://github.com/apache/pulsar/pull/11469)
- Solve the issue of precise rate limiting does not take effect [#11446](https://github.com/apache/pulsar/pull/11446)
- Fix replicated subscriptions direct memory leak [#11396](https://github.com/apache/pulsar/pull/11396)
- Change ContextClassLoader to NarClassLoader in ProtocolHandler [#11276](https://github.com/apache/pulsar/pull/11276)
- Fix the issue of ledger rollover scheduled tasks were executed before reaching the ledger maximum rollover time [#11116](https://github.com/apache/pulsar/pull/11116)
- Fix publish_time not set error when broker entry metadata enable without AppendBrokerTimestampMetadataInterceptor [#11014](https://github.com/apache/pulsar/pull/11014)
- Fix parseMessageMetadata error cause by not skip broker entry metadata [#10968](https://github.com/apache/pulsar/pull/10968)
- Fix issue where Key_Shared consumers could get stuck [#10920](https://github.com/apache/pulsar/pull/10920)
- Fix throwable exception not thrown [#10863](https://github.com/apache/pulsar/pull/10863)
- Handle multiple topic creation for the same topic-name in broker [#10847](https://github.com/apache/pulsar/pull/10847)
- Add REST API to enable or disable replicated subscriptions [#10790](https://github.com/apache/pulsar/pull/10790)
- Fix issue that message ordering could be broken when redelivering messages on Key_Shared subscription [#10762](https://github.com/apache/pulsar/pull/10762)
- Fix set-publish-rate when using preciseTopicPublishRateLimiterEnable=true [#10384](https://github.com/apache/pulsar/pull/10384)
- Upgrade BookKeeper version to resolve the BouncyCastle issue [#11759](https://github.com/apache/pulsar/pull/11759)
- Fix getPreviousPosition NPE [#11621](https://github.com/apache/pulsar/pull/11621)
- Remove duplicated configuration [#11283](https://github.com/apache/pulsar/pull/11283)
- Source tarball: apply executable file permissions to shell scripts [#11858](https://github.com/apache/pulsar/pull/11858)
- Fix java_test_functions build failed [#11829](https://github.com/apache/pulsar/pull/11829)
- Fix generate javadoc for kafka-connect-adaptor failed [#11807](https://github.com/apache/pulsar/pull/11807)
- Fix unnecessary user interactions when building pulsar-standalone image [#11623](https://github.com/apache/pulsar/pull/11623)
- Do not expose meaningless stats for publisher [#11454](https://github.com/apache/pulsar/pull/11454)
- Add metrics storageLogicalSize for the TopicStats and NamespaceStats [#11430](https://github.com/apache/pulsar/pull/11430)
- Compress managed ledger info [#11490](https://github.com/apache/pulsar/pull/11490)
- Print message metadata when getting message by id [#11092](https://github.com/apache/pulsar/pull/11092)
- Query parameter "negativeAckRedeliveryDelay" should be effective even if DLQ is disabled [#11495](https://github.com/apache/pulsar/pull/11495)
- Fix websocket TLS bug [#11243](https://github.com/apache/pulsar/pull/11243)
- Fix the Pulsar Proxy flaky test (Collector already registered that provides name: jvm_memory_direct_bytes_used) [#11900](https://github.com/apache/pulsar/pull/11900)
- Fix flaky test testReacquireLocksAfterSessionLost [#11815](https://github.com/apache/pulsar/pull/11815)
- Fix flaky test testUpdateDynamicLocalConfiguration [#11115](https://github.com/apache/pulsar/pull/11115)
- Fix flaky test testBrokerRanking [#11114](https://github.com/apache/pulsar/pull/11114)
- Fix flaky test in AdminApiOffloadTest [#11028](https://github.com/apache/pulsar/pull/11028)
- Fix the flaky test in the ManagedLedgerTest [#11016](https://github.com/apache/pulsar/pull/11016)
- Make Metadata ZKSessionTest less Flaky [#10955](https://github.com/apache/pulsar/pull/10955)
- Make MetadataCacheTest reliable. [#10877](https://github.com/apache/pulsar/pull/10877)
- Fix pulsar_standalone docker image build failed [#11862](https://github.com/apache/pulsar/pull/11862)
- Producer getting producer busy is removing existing producer from list [#11804](https://github.com/apache/pulsar/pull/11804)
- Revert PR 11594 to avoid copy data to direct buffer [#11792](https://github.com/apache/pulsar/pull/11792)
- Upgrade aircompressor to 0.20 [#11790](https://github.com/apache/pulsar/pull/11790)
- Fix wrappedBuffer always using the same block of memory [#11782](https://github.com/apache/pulsar/pull/11782)
- Fix com.squareup.okhttp-okhttp-2.7.4.jar unaccounted for in LICENSE bug [#11769](https://github.com/apache/pulsar/pull/11769)
- Handle NPE and memory leak when full key range isn't covered with active consumers [#11749](https://github.com/apache/pulsar/pull/11749)
- Call .release() when discarding entry to prevent direct memory leak [#11748](https://github.com/apache/pulsar/pull/11748)
- Avoid duplicated disconnecting producers after fail to add entry[#11741](https://github.com/apache/pulsar/pull/11741)
- Expose compaction metrics to Prometheus [#11739](https://github.com/apache/pulsar/pull/11739)
- Fix the topic in a fenced state and can not recover[#11737](https://github.com/apache/pulsar/pull/11737)
- Remove subscription when closing Reader on non-persistent topics [#11731](https://github.com/apache/pulsar/pull/11731)
- Fix branch-2.8 cherry-pick issue. [#11694](https://github.com/apache/pulsar/pull/11694)
- KeyShared dispatcher on non-persistent topics was not respecting consumer flow-control [#11692](https://github.com/apache/pulsar/pull/11692)
- Fix the bug, can not update topic when the update topicName is contained by an existed topic as a part [#11686](https://github.com/apache/pulsar/pull/11686)
- If a topic has compaction policies configured, we must ensure the subscription is always pre-created [#11672](https://github.com/apache/pulsar/pull/11672)
- Fix testSetReplicatedSubscriptionStatus run failed [#11661](https://github.com/apache/pulsar/pull/11661)
- Fix Pulsar didn't respond error messages when throw InterceptException [#11650](https://github.com/apache/pulsar/pull/11650)
- Fix license mismatch [#11645](https://github.com/apache/pulsar/pull/11645)
- Remove unnecessary jar name in LICENSE files [#11644](https://github.com/apache/pulsar/pull/11644)
- Fix java.lang.NoSuchMethodError: java.nio.ByteBuffer.position(I)Ljava/nio/ByteBuffer when enabling topic metadata compression [#11594](https://github.com/apache/pulsar/pull/11594)
- Fix decode compression managedLedgerInfo data [#11569](https://github.com/apache/pulsar/pull/11569)
- Fix data lost when using earliest position to subscribe to a topic [#11547](https://github.com/apache/pulsar/pull/11547)
- Add test for auto-created partitioned system topic [#11545](https://github.com/apache/pulsar/pull/11545)
- Replace orElse with orElseGet to avoid calling too many times. [#11542](https://github.com/apache/pulsar/pull/11542)
- Fix the schema deletion when delete topic with delete schema [#11501](https://github.com/apache/pulsar/pull/11501)
- Add metrics for writing or reading size of cursor [#11500](https://github.com/apache/pulsar/pull/11500)
- Do not create system topic for heartbeat namespace [#11499](https://github.com/apache/pulsar/pull/11499)
- Add additional servlet support to broker [#11498](https://github.com/apache/pulsar/pull/11498)
- Add metrics [AddEntryWithReplicasBytesRate] for namespace [#11472](https://github.com/apache/pulsar/pull/11472)
- Deep copy the tenants to avoid concurrent sort exception [#11463](https://github.com/apache/pulsar/pull/11463)
- Reduce the probability of cache inconsistencies [#11423](https://github.com/apache/pulsar/pull/11423)
- Reduce integration test memory usage in CI [#11414](https://github.com/apache/pulsar/pull/11414)
- Swap getTopicReference(topic) with serviceUnit.includes to reduce calling getTopicReference [#11405](https://github.com/apache/pulsar/pull/11405)
- Invalidate the read handle after all cursors are consumed[#11389](https://github.com/apache/pulsar/pull/11389)
- Parallel Precise Publish Rate Limiting Fix [#11372](https://github.com/apache/pulsar/pull/11372)
- Fix concurrency issues in NarUnpacker [#11343](https://github.com/apache/pulsar/pull/11343)
- Close the replicator and replication client when delete cluster[#11342](https://github.com/apache/pulsar/pull/11342)
- Add multi roles support for authorization [#11341](https://github.com/apache/pulsar/pull/11341)
- Fix NPE when unloading persistent partitioned topic [#11310](https://github.com/apache/pulsar/pull/11310)
- Fixed retention of keys in compaction [#11287](https://github.com/apache/pulsar/pull/11287)
- Fix missing replicator metrics [#11264](https://github.com/apache/pulsar/pull/11264)
- Simplify managedLedger retention trim logic [#11255](https://github.com/apache/pulsar/pull/11255)
- Fix retention size policy delete too much ledgers [#11242](https://github.com/apache/pulsar/pull/11242)
- Fix init WebSocketService with ClusterData [#11234](https://github.com/apache/pulsar/pull/11234)
- Make the compaction phase one loop timeout configurable [#11206](https://github.com/apache/pulsar/pull/11206)
- Fixed using CommandSubscribe.getConsumerName() without checking [#11199](https://github.com/apache/pulsar/pull/11199)
- Fix some typos of the PersistentTopics [#11187](https://github.com/apache/pulsar/pull/11187)
- Fix failing auth test. [#11186](https://github.com/apache/pulsar/pull/11186)
- Fix the deadlock when using hasMessageAvailableAsync and readNextAsync [#11183](https://github.com/apache/pulsar/pull/11183)
- Fix compaction entry read exception [#11175](https://github.com/apache/pulsar/pull/11175)
- Set -Dio.netty.tryReflectionSetAccessible=true for pulsar processes [#11138](https://github.com/apache/pulsar/pull/11138)
- Fix broker dispatch byte rate limiter. [#11135](https://github.com/apache/pulsar/pull/11135)
- Change test group to broker for ReplicatorTest and fix the test [#11134](https://github.com/apache/pulsar/pull/11134)
- Fix subscription permission not working in reset cursor [#11132](https://github.com/apache/pulsar/pull/11132)
- Fix Flaky-test: [TopicFromMessageTest].[testMultiTopicConsumerBatchShortName [#11125](https://github.com/apache/pulsar/pull/11125)
- Fix the timestamp description for resetCursor [#11121](https://github.com/apache/pulsar/pull/11121)
- Fix MsgDropRate missing from NonPersistentTopics stats output. [#11119](https://github.com/apache/pulsar/pull/11119)
- Fix GetListInBundle return all Topics in bundle [#11110](https://github.com/apache/pulsar/pull/11110)
- Added missing configuration entries [#11095](https://github.com/apache/pulsar/pull/11095)
- Fix inputs to return a list of topic [#11094](https://github.com/apache/pulsar/pull/11094)
- Add authoritative flag for topic policy to avoid redirect loop [#11051](https://github.com/apache/pulsar/pull/11051)
- Made the PulsarClusterMetadataTeardown deletes idempotent [#11042](https://github.com/apache/pulsar/pull/11042)
- Fix flaky test testEnableAndDisableTopicDelayedDelivery [#11009](https://github.com/apache/pulsar/pull/11009)
- Do not expose meaningless stats for consumers[#11005](https://github.com/apache/pulsar/pull/11005)
- Fix NoClassDefFoundError - io.airlift.compress.lz4.UnsafeUtil [#10983](https://github.com/apache/pulsar/pull/10983)
- Fix direct memory leak in getLastMessageId [#10977](https://github.com/apache/pulsar/pull/10977)
- Fix the backlog issue with --precise-backlog=true [#10966](https://github.com/apache/pulsar/pull/10966)
- Fix create partitioned topic in replicated namespace [#10963](https://github.com/apache/pulsar/pull/10963)
- Fix incorrect port of advertisedListener [#10961](https://github.com/apache/pulsar/pull/10961)
- Fix NonRecoverableLedgerException when get last message ID by Reader [#10957](https://github.com/apache/pulsar/pull/10957)
- Fix compaction not working for system topic [#10941](https://github.com/apache/pulsar/pull/10941)
- Fix peek message failure when broker entry metadata is enabled [#10924](https://github.com/apache/pulsar/pull/10924)
- Fix the unit tests for the websocket and run tests under websocket group [#10921](https://github.com/apache/pulsar/pull/10921)
- When the Replicator is enabled, no managedLedger is created when updating the number of partitions [#10910](https://github.com/apache/pulsar/pull/10910)
- Correct code example in transaction doc [#10901](https://github.com/apache/pulsar/pull/10901)
- When topic does not exist, optimize the prompt message [#10845](https://github.com/apache/pulsar/pull/10845)

### Topic Policy
- Refine topic level backlog quota policies warning log [#11863](https://github.com/apache/pulsar/pull/11863)
- Avoid redundant calls for getting the offload policies from the offloader [#11629](https://github.com/apache/pulsar/pull/11629)
- Fix some topic policy operation without backoff [#11560](https://github.com/apache/pulsar/pull/11560)
- Add backoff for setting for getting topic policies[#11487](https://github.com/apache/pulsar/pull/11487)
- Disable replicating system topic across clusters[#11376](https://github.com/apache/pulsar/pull/11376)
- When deleting a topic, delete the topic policy together[#11316](https://github.com/apache/pulsar/pull/11316)
- Fix using partitioned topic name to get Policy [#11294](https://github.com/apache/pulsar/pull/11294)
- Fix replay topic policy message not work [#11136](https://github.com/apache/pulsar/pull/11136)
- Fix race condition of the SystemTopicBasedTopicPoliciesService [#11097](https://github.com/apache/pulsar/pull/11097)
- Fix retention policy in topic policy not work [#11021](https://github.com/apache/pulsar/pull/11021)
- Fix potential data lost on the system topic when topic compaction have not triggered yet [#11003](https://github.com/apache/pulsar/pull/11003)
- Make getTopicPoliciesAsyncWithRetry as a default method [#11518](https://github.com/apache/pulsar/pull/11518)

### Proxy
- Fixed Proxy leaking outbound connections [#11848](https://github.com/apache/pulsar/pull/11848)

### Functions
- Support protobuf schema for Pulsar function [#11709](https://github.com/apache/pulsar/pull/11709)
- Fix cast exception occurs if function/source/sink type is ByteBuffer [#11611](https://github.com/apache/pulsar/pull/11611)
- Fix source stats exposing empty exceptions list [#11478](https://github.com/apache/pulsar/pull/11478)
- Set exposePulsarAdmin to true if enabled [#11417](https://github.com/apache/pulsar/pull/11417)
- Add instanceId and fqn into log message properties [#11399](https://github.com/apache/pulsar/pull/11399)
- Fix tls_validate_hostname is not supported in python functions runtime [#11087](https://github.com/apache/pulsar/pull/11087)
- Use the subscription name defined in function details [#11076](https://github.com/apache/pulsar/pull/11076)
- Fix build failure because of spotbugs [#10792](https://github.com/apache/pulsar/pull/10792)
- Use keyword argument to create pulsar_client [#11080](https://github.com/apache/pulsar/pull/11080)

### Java Client
- Fixed accessing MessageImpl after it was enqueued on user queue [#11824](https://github.com/apache/pulsar/pull/11824)
- Remove consumer reference from PulsarClient on subscription failure [#11758](https://github.com/apache/pulsar/pull/11758)
- Clean up MultiTopicsConsumerImpl reference on consumer creation failure [#11754](https://github.com/apache/pulsar/pull/11754)
- Fix null MessageId may be passed to its compareTo() method [#11607](https://github.com/apache/pulsar/pull/11607)
- Fix Consumer listener does not respect receiver queue size [#11455](https://github.com/apache/pulsar/pull/11455)
- Avoid infinite waiting for consumer close [#11347](https://github.com/apache/pulsar/pull/11347)
- Fix non-persistent topic get partitioned metadata error on discovery [#10806](https://github.com/apache/pulsar/pull/10806)
- Add AvroSchema UUID support fix [#10428](https://github.com/apache/pulsar/pull/10428)
- Handle receiveAsync() failures in MultiTopicsConsumer [#11843](https://github.com/apache/pulsar/pull/11843)
- Fix bin/pulsar-client produce not supporting v2 topic name through websocket [#11069](https://github.com/apache/pulsar/pull/11069)
- Fixed race condition on multi-topic consumer [#11764](https://github.com/apache/pulsar/pull/11764)
- Forget to update memory usage on message send timeout [#11761](https://github.com/apache/pulsar/pull/11761)
- Fixed block forever bug in Consumer.batchReceive [#11691](https://github.com/apache/pulsar/pull/11691)
- Fix add listenerName for geo-replicator [#10779](https://github.com/apache/pulsar/pull/10779)

### C++ Client
- Use same regex code at ZTSClient [#11323](https://github.com/apache/pulsar/pull/11323)
- Use sendRequestWithId to add timeout to hasMessageAvailable [#11600](https://github.com/apache/pulsar/pull/11600)
- Fix bugs that were not exposed by broken C++ CI before [#11557](https://github.com/apache/pulsar/pull/11557)
- Simple implementation of getting number of references from C++ client [#11535](https://github.com/apache/pulsar/pull/11535)
- Fix brew error in site docs to compile C++ client. [#11512](https://github.com/apache/pulsar/pull/11512)
- Support Windows Debug mode build [#11302](https://github.com/apache/pulsar/pull/11302)
- Fix missed header for some compilers [#11152](https://github.com/apache/pulsar/pull/11152)
- Fix boost download link in Dockerfile [#11129](https://github.com/apache/pulsar/pull/11129)
- Fix Setting KeySharedMeta in SubscribeCommand [#11088](https://github.com/apache/pulsar/pull/11088)
- Fix Windows 32 bits compile and runtime failures [#11082](https://github.com/apache/pulsar/pull/11082)
- Add connection timeout configuration [#11029](https://github.com/apache/pulsar/pull/11029)
- Fix Windows build issues about static library [#10956](https://github.com/apache/pulsar/pull/10956)
- Fix incorrect connect timeout implementation [#11889](https://github.com/apache/pulsar/pull/11889)
- Fix CPP build not failing when tests fail [#11575](https://github.com/apache/pulsar/pull/11575)
- Avoid throwing exceptions when setting socket option [#11329](https://github.com/apache/pulsar/pull/11329)

### Python Client
- Fix redefined Record or Enum in Python schema [#11595](https://github.com/apache/pulsar/pull/11595)
- Fix Python doc generate [#11585](https://github.com/apache/pulsar/pull/11585)
- Fix Python schema array map with record [#11530](https://github.com/apache/pulsar/pull/11530)
- Fixed import when AvroSchema is not being used [#11034](https://github.com/apache/pulsar/pull/11034)
- Fix deadlock caused by ExecutorService::close [#11882](https://github.com/apache/pulsar/pull/11882)
- Fixed crash when using Python logger [#10981](https://github.com/apache/pulsar/pull/10981)
- Ensure producer is keeping the client object alive [#11887](https://github.com/apache/pulsar/pull/11887)
- Fix fields that are ignoring the required key argument [#11508](https://github.com/apache/pulsar/pull/11508)
- Fix handle complex schema [#11400](https://github.com/apache/pulsar/pull/11400)
- Check if the record is not None [#11559](https://github.com/apache/pulsar/pull/11559)

### Security
- Upgrade commons-compress to 1.21 [#11345](https://github.com/apache/pulsar/pull/11345)
- Fix GetTopicsOfNamespace with binary lookup service not check auth [#11172](https://github.com/apache/pulsar/pull/11172)
- Use ubuntu:20.04 base image for Pulsar docker images [#11026](https://github.com/apache/pulsar/pull/11026)
- Upgrade vertx to 3.9.8 to address CVE-2019-17640 [#10889](https://github.com/apache/pulsar/pull/10889)
- Exclude and remove freebuilder dependency [#10869](https://github.com/apache/pulsar/pull/10869)
- Upgrade bouncycastle version to 1.69 [#10867](https://github.com/apache/pulsar/pull/10867)
- Upgrade K8s client-java to 12.0.1 [#10866](https://github.com/apache/pulsar/pull/10866)
- Upgrade caffeine to 2.9.1 [#10865](https://github.com/apache/pulsar/pull/10865)
- Upgrade commons-codec to 1.15 [#10864](https://github.com/apache/pulsar/pull/10864)
- Load credentials from secrets for Kinesis connectors [#10822](https://github.com/apache/pulsar/pull/10822)
- Forbid to read other topic's data in managedLedger layer [#11912](https://github.com/apache/pulsar/pull/11912)
- Bump Netty version to 4.1.66.Final [#11344](https://github.com/apache/pulsar/pull/11344)

### Transaction
- Pending ack set managed ledger config true [#11494](https://github.com/apache/pulsar/pull/11494)
- Add getTxnID method in Transaction.java [#11438](https://github.com/apache/pulsar/pull/11438)
- Fix direct memory leak related to commit and abort markers [#11407](https://github.com/apache/pulsar/pull/11407)
- Fix transaction buffer client handle endTxn op when topic or sub have been deleted[#11304](https://github.com/apache/pulsar/pull/11304)
- Fix the transaction markers that are not deleted as expected[#11126](https://github.com/apache/pulsar/pull/11126)
- Fix delete sub then delete pending ack[#11023](https://github.com/apache/pulsar/pull/11023)
- Prevent NPE in case of closeAsync() without a successful execution of startAsync() [#10948](https://github.com/apache/pulsar/pull/10948)
- Fixed possible deadlock in the initialization of MLTransactionLog [#11194](https://github.com/apache/pulsar/pull/11194)
- Fix broker init transaction related topic. [#11022](https://github.com/apache/pulsar/pull/11022)

### Pulsar Admin
- Fix pulsar admin method:getMessageById[#11852](https://github.com/apache/pulsar/pull/11852)
- Allow create functions with package URL [#11666](https://github.com/apache/pulsar/pull/11666)
- Add compacted topic metrics for TopicStats in CLI [#11564](https://github.com/apache/pulsar/pull/11564)
- Fix time based backlog quota. [#11509](https://github.com/apache/pulsar/pull/11509)
- Add offload ledger info for admin topics stats [#11465](https://github.com/apache/pulsar/pull/11465)
- Add complete metadata for admin.topics().examineMessages [#11443](https://github.com/apache/pulsar/pull/11443)
- Remove duplicate check for replicationClusterSet [#11429](https://github.com/apache/pulsar/pull/11429)
- Pulsar Admin List Subscription lists only subscriptions created for Partition-0 when partition specific subscriptions are created [#11355](https://github.com/apache/pulsar/pull/11355)
- Expose broker entry metadata and deliverAtTime to peekMessages/getMessages [#11279](https://github.com/apache/pulsar/pull/11279)
- Allow null to be set as namespace level subscription TTL [#11253](https://github.com/apache/pulsar/pull/11253)
- Enable peeking encrypted batch messages [#11244](https://github.com/apache/pulsar/pull/11244)
- Fix async response filter [#11052](https://github.com/apache/pulsar/pull/11052)
- Add error log for schema admin operation [#11427](https://github.com/apache/pulsar/pull/11427)

### Tiered Storage
- OffloadPoliciesImplBuilder missing method and not implements OffloadPolicies.Builder [#11453](https://github.com/apache/pulsar/pull/11453)
- Remove unused listener to reduce creating executor pool [#11215](https://github.com/apache/pulsar/pull/11215)

### Pulsar IO
- Make KafkaSourceRecord ack() non-blocking to avoid deadlock [#11435](https://github.com/apache/pulsar/pull/11435)
- Allow Sinks to use native AVRO and JSON [#11322](https://github.com/apache/pulsar/pull/11322)
- Refine the key in redis sink when key is null [#11192](https://github.com/apache/pulsar/pull/11192)
- Change the nar package name for pulsar-io-kafka-connect-adaptor [#10976](https://github.com/apache/pulsar/pull/10976)

### 2.8.0 &mdash; 2021-06-12 <a id=“2.8.0”></a>

### Update notice

Due to a [breaking change in the Schema API](https://github.com/apache/pulsar/pull/10878), it may happen that some Pulsar Functions or Pulsar IO connectors fail to work, 
throwing an `IncompatibleClassChangeError`.

In this case, you need to rebuild your Function using Apache Pulsar 2.8.0 as a dependency and redeploy it.

If you are running on Kubernetes, you can temporarily let the Functions Worker pod run with a previous version of Pulsar
in order to not cause downtime.

For more context about this issue, see [[Pulsar Functions and IO] Cannot upgrade Function built for Pulsar 2.7 to Pulsar 2.8](https://github.com/apache/pulsar/issues/11338).

#### PIPs
- [PIP 45] Pluggable metadata interface [#9148](https://github.com/apache/pulsar/pull/9148) [#9221](https://github.com/apache/pulsar/pull/9221) [#9240](https://github.com/apache/pulsar/pull/9240) [#9273](https://github.com/apache/pulsar/pull/9273) [#9274](https://github.com/apache/pulsar/pull/9274) [#9338](https://github.com/apache/pulsar/pull/9338) [#9346](https://github.com/apache/pulsar/pull/9346) [#9351](https://github.com/apache/pulsar/pull/9351) [#9412](https://github.com/apache/pulsar/pull/9412) [#9485](https://github.com/apache/pulsar/pull/9485) [#9586](https://github.com/apache/pulsar/pull/9586) [#9967](https://github.com/apache/pulsar/pull/9967) [#9973](https://github.com/apache/pulsar/pull/9973) [#10391](https://github.com/apache/pulsar/pull/10391) [#10457](https://github.com/apache/pulsar/pull/10457) [#10532](https://github.com/apache/pulsar/pull/10532) [#10545](https://github.com/apache/pulsar/pull/10545) [#10647](https://github.com/apache/pulsar/pull/10647)
- [PIP 50] Package management service [#8637](https://github.com/apache/pulsar/pull/8637) [#8680](https://github.com/apache/pulsar/pull/8680) [#8744](https://github.com/apache/pulsar/pull/8744) [#8764](https://github.com/apache/pulsar/pull/8764) [#8816](https://github.com/apache/pulsar/pull/8816) [#8817](https://github.com/apache/pulsar/pull/8817) [#8858](https://github.com/apache/pulsar/pull/8858) [#8861](https://github.com/apache/pulsar/pull/8861) [#8893](https://github.com/apache/pulsar/pull/8893) [#8907](https://github.com/apache/pulsar/pull/8907)
- [PIP 68] Exclusive producer [#8685](https://github.com/apache/pulsar/pull/8685) [#8992](https://github.com/apache/pulsar/pull/8992) [#9554](https://github.com/apache/pulsar/pull/9554) [#9600](https://github.com/apache/pulsar/pull/9600)
- [PIP 70] Lightweight broker entry metadata [#8618](https://github.com/apache/pulsar/pull/8618) [#9067](https://github.com/apache/pulsar/pull/9067) [#9088](https://github.com/apache/pulsar/pull/9088) [#9091](https://github.com/apache/pulsar/pull/9091)
- [PIP 71] Pulsar SQL migrate SchemaHandle to Presto decoder [#8422](https://github.com/apache/pulsar/pull/8422)
- [PIP 74] Client memory limits [#8965](https://github.com/apache/pulsar/pull/8965) [#9676](https://github.com/apache/pulsar/pull/9676)
- [PIP 75] Perform serialization/deserialization with LightProto [#9046](https://github.com/apache/pulsar/pull/9046)
- [PIP 76] Streaming offloader [#9096](https://github.com/apache/pulsar/pull/9096)
- [PIP 82] Tenant and namespace level rate limiting [#10008](https://github.com/apache/pulsar/pull/10008) [#10201](https://github.com/apache/pulsar/pull/10201) [#10204](https://github.com/apache/pulsar/pull/10204) [#10218](https://github.com/apache/pulsar/pull/10218)
- [PIP 83] Message consumption with pooled buffer [#10184](https://github.com/apache/pulsar/pull/10184)
- [PIP 85] Support get reader schema for a message in Java Client [#10476](https://github.com/apache/pulsar/pull/10476)

#### Transactions
- Support pending ack state persistence [#8881](https://github.com/apache/pulsar/pull/8881)
- Stable position and low watermark for the transaction buffer [#9195](https://github.com/apache/pulsar/pull/9195)
- Transaction timeout support [#9229](https://github.com/apache/pulsar/pull/9229)
- Transaction coordinator retry to complete the transaction [#9236](https://github.com/apache/pulsar/pull/9236)
- Fix race condition when appending transaction log [#9238](https://github.com/apache/pulsar/pull/9238)
- Transaction buffer snapshot [#9490](https://github.com/apache/pulsar/pull/9490)
- Add metrics for transaction coordinator [#9706](https://github.com/apache/pulsar/pull/9706)
- Clean the useless transaction individual acknowledgements based on low watermark [#9722](https://github.com/apache/pulsar/pull/9722)
- Fix memory leak when deleting transaction marker [#9751](https://github.com/apache/pulsar/pull/9751)
- Check the transaction state at the client side [#9776](https://github.com/apache/pulsar/pull/9776)
- Clean aborted transactions for the transaction buffer [#9974](https://github.com/apache/pulsar/pull/9974)
- Fix transaction coordinator retry to end transaction [#10131](https://github.com/apache/pulsar/pull/10131)
- Fix NPE when opening a new transaction [#10139](https://github.com/apache/pulsar/pull/10139)
- Fix transaction log failed to recover [#10146](https://github.com/apache/pulsar/pull/10146)
- Fix transaction coordinator recover timeout [#10162](https://github.com/apache/pulsar/pull/10162)
- Handling committing and aborting state when recovering transaction coordinator [#10179](https://github.com/apache/pulsar/pull/10179)
- Fix NPE in case of enableTransactionCoordinator=false [#10182](https://github.com/apache/pulsar/pull/10182)
- Fix transaction buffer client timeout [#10206](https://github.com/apache/pulsar/pull/10206)
- Fix recover max local id issue for the transaction coordinator [#10224](https://github.com/apache/pulsar/pull/10224)
- Support using transactions on standalone [#10238](https://github.com/apache/pulsar/pull/10238)
- Fix transaction buffer lookup issue [#10257](https://github.com/apache/pulsar/pull/10257)
- Fix transaction timeout issue at the client side [#10323](https://github.com/apache/pulsar/pull/10323)
- Fix transaction client reconnect issue after transaction coordinator unloaded [#10327](https://github.com/apache/pulsar/pull/10327)
- Fix transaction timeout not canceled after expired [#10366](https://github.com/apache/pulsar/pull/10366)
- Transaction log low watermark optimization [#10422](https://github.com/apache/pulsar/pull/10422)
- Fix the issue of transaction buffer client channel is inactive [#10407](https://github.com/apache/pulsar/pull/10407)
- Add Admin API for getting transaction coordinator stats [#10639](https://github.com/apache/pulsar/pull/10639)
- Add Admin API for getting transaction in transaction buffer stats [#10642](https://github.com/apache/pulsar/pull/10642)
- Add Admin API for getting transaction in pending ack stats [#10648](https://github.com/apache/pulsar/pull/10648)
- Add Admin API for getting transaction buffer stats and pending ack stats [#10650](https://github.com/apache/pulsar/pull/10650)
- Add Admin API for getting transaction coordinator internal stats [#10653](https://github.com/apache/pulsar/pull/10653)
- Setup transaction metadata with metadata store [#10677](https://github.com/apache/pulsar/pull/10677)
- Fix issue with acknowledge messages from multiple subscriptions of a topic [#10689](https://github.com/apache/pulsar/pull/10689)
- Admin API for getting transaction metadata [#10690](https://github.com/apache/pulsar/pull/10690)
- Admin API for getting slow transactions [#10701](https://github.com/apache/pulsar/pull/10701)
- Fix transaction log handle managed ledger WriteFail state [#10711](https://github.com/apache/pulsar/pull/10711)
- Admin API for getting pending ack internal stats [#10725](https://github.com/apache/pulsar/pull/10725)
- Fix transaction ack delete marker position when don't have transaction ack [#10741](https://github.com/apache/pulsar/pull/10741)
- Fix transaction pending ack generate managedLedgerStats fail exception [#10797](https://github.com/apache/pulsar/pull/10797)
- Use Zookeeper Prometheus metric provider to export Zookeeper metrics [#10803](https://github.com/apache/pulsar/pull/10803)
- Always allow system topic for transaction buffer snapshot auto create [#10876](https://github.com/apache/pulsar/pull/10876)

#### Security
- Optional auth method name header in HTTP authentication [#6799](https://github.com/apache/pulsar/pull/6799)
- Topics level authorization support [#7523](https://github.com/apache/pulsar/pull/7523)
- Authorization for setting topic/subscription auto-creation and subscription expire time on namespace [#7900](https://github.com/apache/pulsar/pull/7900)
- Allow serializable stream-provider field into AuthenticationTls [#10020](https://github.com/apache/pulsar/pull/10020)
- Add configuration for running OWASP Dependency Check for all modules [#10288](https://github.com/apache/pulsar/pull/10288)
- Update default TLS protocols to TLSv1.3 and TLSv1.2 for the broker and proxy [#10598](https://github.com/apache/pulsar/pull/10598)

#### Broker
- Share EventLoopGroup between broker and BookKeeper client [#2603](https://github.com/apache/pulsar/pull/2603)
- Dispatch batch messages according to consumer permits [7266](https://github.com/apache/pulsar/pull/7266)
- Improve the max pending publish buffer mechanism [7406](https://github.com/apache/pulsar/pull/7406)
- Allow disabling HTTP TRACE/TRACK verbs [#7907](https://github.com/apache/pulsar/pull/7907)
- Fix DispatchRateLimiter does not take effect [#8611](https://github.com/apache/pulsar/pull/8611)
- Validate namespace isolation policy regex before updating [#8804](https://github.com/apache/pulsar/pull/8804)
- Perform automatically cert refresh for pulsar-admin [#8831](https://github.com/apache/pulsar/pull/8831)
- Fix updating rack info dynamically [#8844](https://github.com/apache/pulsar/pull/8844)
- Fix deadlock when checking message expiration [#8877](https://github.com/apache/pulsar/pull/8877)
- Support limit max topics per namespace [#8942](https://github.com/apache/pulsar/pull/8942)
- Make ledger rollover check task internally [#8946](https://github.com/apache/pulsar/pull/8946)
- Clean up topic that failed to unload from the cache [#8968](https://github.com/apache/pulsar/pull/8968)
- Support get broker response for the message acknowledgement [#8996](https://github.com/apache/pulsar/pull/8996)
- Support message index for a topic [#9039](https://github.com/apache/pulsar/pull/9039)
- Supporting limit max topics per namespace by namespace policy [#9042](https://github.com/apache/pulsar/pull/9042)
- Streaming dipsatcher support [#9056](https://github.com/apache/pulsar/pull/9056)
- Make Netty acceptor threadPool size configurable [#9061](https://github.com/apache/pulsar/pull/9061)
- Fix deadlock when unloading namespace bundles [#9116](https://github.com/apache/pulsar/pull/9116)
- Fixed checking for maxTopicsPerNamespace [#9121](https://github.com/apache/pulsar/pull/9121)
- Change the service URL to a not required param when creating a cluster [#9127](https://github.com/apache/pulsar/pull/9127)
- Support setting replicator dispatch rate policy at the topic level [#9175](https://github.com/apache/pulsar/pull/9175)
- Fix max topic for namespace does not work [#9193](https://github.com/apache/pulsar/pull/9193)
- Fix memory leak of the managed ledger interceptor [#9194](https://github.com/apache/pulsar/pull/9194)
- Fix max consumers per topic cannot be disabled at the namespace level [#9214](https://github.com/apache/pulsar/pull/9214)
- Support schema REST API for V1 topics [#9218](https://github.com/apache/pulsar/pull/9218)
- Fix peek message metadata while enabling broker entry metadata [#9255](https://github.com/apache/pulsar/pull/9255)
- Support fetching metadata from entry data in publish callback [#9257](https://github.com/apache/pulsar/pull/9257)
- Wait for the async broker port listener close operations to complete at shutdown [#9308](https://github.com/apache/pulsar/pull/9308)
- Support script based mapping network topology [#9363](https://github.com/apache/pulsar/pull/9363)
- Make managed ledger storage configurable [#9397](https://github.com/apache/pulsar/pull/9397)
- Support setting enabled subscription types [#9401](https://github.com/apache/pulsar/pull/9401)
- Fixed NPE and cache invalidation in leader election [#9460](https://github.com/apache/pulsar/pull/9460)
- Fix exception when get an optional field for Protobuf message [#9468](https://github.com/apache/pulsar/pull/9468)
- Ignore replicated subscription configurations from the client when disabled by broker [#9523](https://github.com/apache/pulsar/pull/9523)
- Support expose producer metrics through Prometheus endpoint [#9541](https://github.com/apache/pulsar/pull/9541)
- Fix NPE that occurs in PersistentStickyKeyDispatcherMultipleConsumers when debug log enabled [#9587](https://github.com/apache/pulsar/pull/9587)
- Make LocalPolicies immutable to avoid concurrent modify inconsistent [#9598](https://github.com/apache/pulsar/pull/9598)
- Fix writing/encoding issue of GenericJsonRecord [#9608](https://github.com/apache/pulsar/pull/9608)
- Expose the native record for struct schema [#9614](https://github.com/apache/pulsar/pull/9614)
- Add metrics for producer throttling [#9649](https://github.com/apache/pulsar/pull/9649)
- Fix MaxUnackedMessagesPerConsumer cannot be changed dynamically and cannot be disabled [#9657](https://github.com/apache/pulsar/pull/9657)
- Added support for force deleting tenant [#9677](https://github.com/apache/pulsar/pull/9677)
- Fix managed ledger not found exception when force delete namespace [#9691](https://github.com/apache/pulsar/pull/9691)
- Reduce CPU consumption of metrics creation [#9735](https://github.com/apache/pulsar/pull/9735)
- Ensure read-lock is not continuously held on a section while iterating over concurrent maps [#9787](https://github.com/apache/pulsar/pull/9787)
- Add pending read subscription metrics to stats-internal [#9788](https://github.com/apache/pulsar/pull/9788)
- Allow broker to discover and unblock stuck subscription [#9789](https://github.com/apache/pulsar/pull/9789)
- Disabled the tenants/namespaces force deletion by default [#9819](https://github.com/apache/pulsar/pull/9819)
- Add metrics for the connections of the broker [#9876](https://github.com/apache/pulsar/pull/9876)
- Make readModifyUpdate in MetadataCacheImpl thread-safe [#9900](https://github.com/apache/pulsar/pull/9900)
- Optimize NamespaceBundle.toString() which is on the hot path [#9976](https://github.com/apache/pulsar/pull/9976)
- Support set compaction threshold in broker.conf [#9989](https://github.com/apache/pulsar/pull/9989)
- Support set properties for a namespace [#10015](https://github.com/apache/pulsar/pull/10015)
- Fix cannot cleanup expired data after managed-ledger restart [#10087](https://github.com/apache/pulsar/pull/10087)
- [Default configuration] Enable sticky read by default [#10090](https://github.com/apache/pulsar/pull/10090)
- Add time based backlog quota [#10093](https://github.com/apache/pulsar/pull/10093)
- Fix IllegalStateException in PersistentReplicator [#10098](https://github.com/apache/pulsar/pull/10098)
- Support set max consumers per subscription for non-persistent dispatcher [#10121](https://github.com/apache/pulsar/pull/10121)
- Limit the number of producers/consumers that can connect per topic for each IP address [#10188](https://github.com/apache/pulsar/pull/10188)
- Close namespace clients when PulsarService is closed [#10196](https://github.com/apache/pulsar/pull/10196)
- Shutdown Broker gracefully, but forcefully after brokerShutdownTimeoutMs [#10199](https://github.com/apache/pulsar/pull/10199)
- Update the authentication data when an authentication refresh happens [#10203](https://github.com/apache/pulsar/pull/10203)
- Add preciseTopicPublishRateLimiterEnable to broker.conf [#10216](https://github.com/apache/pulsar/pull/10216)
- Fix the typo in the chunkedMessageRate stats and metrics [#10223](https://github.com/apache/pulsar/pull/10223)
- Fix entry cache size to be a negative value [#10225](https://github.com/apache/pulsar/pull/10225)
- Fix replicated subscriptions related LightProto issues [#10247](https://github.com/apache/pulsar/pull/10247)
- Pause the replicated subscriptions snapshots when there is no real traffic [#10292](https://github.com/apache/pulsar/pull/10292)
- Fix the inconsistency of advertised address [#10312](https://github.com/apache/pulsar/pull/10312)
- Support listenerName for HttpLookupService [#10319](https://github.com/apache/pulsar/pull/10319)
- Support truncate topic [#10326](https://github.com/apache/pulsar/pull/10326)
- Fix authorization error if partition number of partitioned topic is updated [10333](https://github.com/apache/pulsar/pull/10333)
- Fix managed ledger name that transaction log used [#10334](https://github.com/apache/pulsar/pull/10334)
- Catch topic policy not hit exception in handleSubscribe [#10341](https://github.com/apache/pulsar/pull/10341)
- Fix ConcurrentModificationException when attempting to update local broker data [#10347](https://github.com/apache/pulsar/pull/10347)
- Support seek to separate messageId/time for multiple topic reader [#10348](https://github.com/apache/pulsar/pull/10348)
- Resource locks should automatically revalidate after a metadata session is re-established [#10351](https://github.com/apache/pulsar/pull/10351)
- Add authentication data for the remote cluster [#10357](https://github.com/apache/pulsar/pull/10357)
- Support array type claims in JWT [#10375](https://github.com/apache/pulsar/pull/10375)
- Optimize locks in AuthenticationAthenz [#10381](https://github.com/apache/pulsar/pull/10381)
- Prevent carrying state of PositionImplRecyclable when recycled [#10404](https://github.com/apache/pulsar/pull/10404)
- Dispatch messages to consumer with permits [#10417](https://github.com/apache/pulsar/pull/10417)
- Fix NPE in unblock stuck subscription task when dispatcher has not created [#10430](https://github.com/apache/pulsar/pull/10430)
- Fix topic loading fails without any error when replicator init fails [#10432](https://github.com/apache/pulsar/pull/10432)
- Set timeout to unblock web-threads on update partition API [#10447](https://github.com/apache/pulsar/pull/10447)
- Fix CPU 100% when deleting namespace [#10454](https://github.com/apache/pulsar/pull/10454)
- Remove residual info after forcibly deleting the namespace [#10465](https://github.com/apache/pulsar/pull/10465)
- Fix potential memory leak of TopicPolicies [#10466](https://github.com/apache/pulsar/pull/10466)
- Fix publish callback's entry data is null during ledger rollover [#10467](https://github.com/apache/pulsar/pull/10467)
- Fix readModifyUpdate should return the new value in the future for metadata cache [#10474](https://github.com/apache/pulsar/pull/10474)
- Fix partitioned system topic check issue [#10529](https://github.com/apache/pulsar/pull/10529)
- Removed AspectJ based metrics for ZooKeeper [#10533](https://github.com/apache/pulsar/pull/10533)
- Allow creating MetadataCache with custom serde [#10543](https://github.com/apache/pulsar/pull/10543)
- Fix ack receipt version check issue [#10551](https://github.com/apache/pulsar/pull/10551)
- Expose average message size metrics for a topic [#10553](https://github.com/apache/pulsar/pull/10553)
- Fixed missed ZK caching when fetching list of namespaces for a tenant [#10594](https://github.com/apache/pulsar/pull/10594)
- Setup pulsar cluster with MetadataStore [#10600](https://github.com/apache/pulsar/pull/10600)
- Setup initial namespaces with MetadataStore [#10612](https://github.com/apache/pulsar/pull/10612)
- Convert bundle split into an HTTP async operation [#10619](https://github.com/apache/pulsar/pull/10619)
- Add metrics for non-contiguous deleted messages range [#10638](https://github.com/apache/pulsar/pull/10638)
- Fix consumer related topic stats only available while consumer or reader are connected [#10644](https://github.com/apache/pulsar/pull/10644)
- Allow configuring the number of BK client worker threads [#10649](https://github.com/apache/pulsar/pull/10649)
- Fix ConcurrentOpenLongPairRangeSet remove all ranges [#10656](https://github.com/apache/pulsar/pull/10656)
- Ensure all the ReadHandle gets properly closed on cache invalidation [#10659](https://github.com/apache/pulsar/pull/10659)
- Avoid the context switch when managedLedgerNewEntriesCheckDelayInMillis=0 [#10660](https://github.com/apache/pulsar/pull/10660)
- Allow configuring busy-wait in broker and client [#10661](https://github.com/apache/pulsar/pull/10661)
- Use a single JWT parser instance [#10664](https://github.com/apache/pulsar/pull/10664)
- Fix issues in advanceNonDurableCursors [#10667](https://github.com/apache/pulsar/pull/10667)
- LockManager use a concurrent hash map to handle locks notifications [#10680](https://github.com/apache/pulsar/pull/10680)
- Avoid making copies of internal maps when iterating [#10691](https://github.com/apache/pulsar/pull/10691)
- Fix issue where StackOverflowError occurs when trying to redeliver a large number of already acked messages [#10696](https://github.com/apache/pulsar/pull/10696)
- Fix NPE when filtering read entries [#10704](https://github.com/apache/pulsar/pull/10704)
- Fix deadlock when enabling managed ledger interceptor [#10706](https://github.com/apache/pulsar/pull/10706)
- Fix inconsistent behavior in LongPairRangeSet [#10713](https://github.com/apache/pulsar/pull/10713)
- Fix repeated iterator generation of LongPairRangeSet [#10722](https://github.com/apache/pulsar/pull/10722)
- Cancel scheduled tasks as the first step in closing for the managed ledger [#10739](https://github.com/apache/pulsar/pull/10739)
- Prevent race conditions between timeout and completion of managed ledger [#10740](https://github.com/apache/pulsar/pull/10740)
- Add advertised listener name for geo-replicator [#10751](https://github.com/apache/pulsar/pull/10751)
- Add a read lock when traversing batchDeletedIndexes [#10763](https://github.com/apache/pulsar/pull/10763)
- Replace use of Prometheus client CollectorRegistry.getSampleValue [#10764](https://github.com/apache/pulsar/pull/10764)
- Release OpAddEntry.data when entry is copied and discarded for the managed ledger [#10773](https://github.com/apache/pulsar/pull/10773)
- Avoid warning logs on topic policies not present [#10785](https://github.com/apache/pulsar/pull/10785)
- Remove pulsar-zookeeper module and call zookeeper quorumPeerMain to start zookeeper [#10823](https://github.com/apache/pulsar/pull/10823)
- Fix consumer stuck issue due to reuse entry wrapper [#10824](https://github.com/apache/pulsar/pull/10824)
- Fix possible data race in getFirstAvailableConsumerPermits [#10831](https://github.com/apache/pulsar/pull/10831)

#### Functions
- Update default function log location in function_worker.yaml [#8470](https://github.com/apache/pulsar/pull/8470)
- Optimize batch source discovery and task ack [#8498](https://github.com/apache/pulsar/pull/8498)
- Provide an interface for functions worker service [#8560](https://github.com/apache/pulsar/pull/8560)
- Improve naming of threads used in batch source [#8608](https://github.com/apache/pulsar/pull/8608)
- Fix the reader used to read the metadata topic during worker service initialization not been closed [#8637](https://github.com/apache/pulsar/pull/8637)
- Add timeout to hasMessageAvailable to leader election process [#8687](https://github.com/apache/pulsar/pull/8687)
- Support key based batch builder for Go Functions [#8761](https://github.com/apache/pulsar/pull/8761)
- Fix panic when discarding message for Go Functions [#8776](https://github.com/apache/pulsar/pull/8776)
- Move initialize dlog namespace metadata to bin/pulsar [#8781](https://github.com/apache/pulsar/pull/8781)
- Support URL fetching for Go/Python Functions [#8808](https://github.com/apache/pulsar/pull/8808)
- Add cmd flag for retaining key ordering [#8886](https://github.com/apache/pulsar/pull/8886)
- Supports to use package command [#8973](https://github.com/apache/pulsar/pull/8973)
- Support create and update sink with package name [#8987](https://github.com/apache/pulsar/pull/8987)
- Support create and update source with package name [#8988](https://github.com/apache/pulsar/pull/8988)
- Allow stats operations not to be blocked in functions [#9005](https://github.com/apache/pulsar/pull/9005)
- Adding timeout to open table call for function state [#9006](https://github.com/apache/pulsar/pull/9006)
- Fix get function info error of REST API [#9115](https://github.com/apache/pulsar/pull/9115)
- Improve the error message when creating sinks [#9131](https://github.com/apache/pulsar/pull/9131)
- Expose Pulsar Admin through Function context [#9246](https://github.com/apache/pulsar/pull/9246)
- Enable Function Workers to use exclusive producer to write to internal topics [#9275](https://github.com/apache/pulsar/pull/9275)
- Add additional logging when setting up state table for function instance [#9304](https://github.com/apache/pulsar/pull/9304)
- Allow memory limit to be set for the pulsar client used in the ThreadRuntime in Pulsar Function [#9320](https://github.com/apache/pulsar/pull/9320)
- Make admin operations on Statestore non blocking [#9348](https://github.com/apache/pulsar/pull/9348)
- Fix maxPendingAsyncRequests not affect Kubernetes Runtime [#9349](https://github.com/apache/pulsar/pull/9349)
- Add download directory support to function Kubernetes Runtime [#9377](https://github.com/apache/pulsar/pull/9377)
- Optimize built-in source/sink startup by eliminating redundant NAR unpacking and checksum calculation [#9413](https://github.com/apache/pulsar/pull/9413) [#9500](https://github.com/apache/pulsar/pull/9500)
- Enhance Kubernetes manifest customizer with default options [#9445](https://github.com/apache/pulsar/pull/9445)
- Fix possible deadlock on broker-function service startup [#9499](https://github.com/apache/pulsar/pull/9499)
- Close InputStreams properly [#9568](https://github.com/apache/pulsar/pull/9568)
- Add maximum allowed amount of resources setting for functions [#9584](https://github.com/apache/pulsar/pull/9584)
- Support writing general records to Pulsar sink [#9590](https://github.com/apache/pulsar/pull/9590)
- Move metrics port configuration to InstanceConfig [#9610](https://github.com/apache/pulsar/pull/9610)
- Add resource granularity settings for functions [#9736](https://github.com/apache/pulsar/pull/9736)
- Prevent invalid broker or proxy configuration for authorization [#9746](https://github.com/apache/pulsar/pull/9746)
- Log stacktraces of threads that failed to terminate on shutdown within timeout in ExecutorProvider [#9840](https://github.com/apache/pulsar/pull/9840)
- Support get partition index for a Record [#9947](https://github.com/apache/pulsar/pull/9947)
- Deleting a Pulsar Function with a name that includes a colon character crashes the pulsar broker [#9946](https://github.com/apache/pulsar/issues/9946)
- Exposing Prometheus metrics for Pulsar function local run mode [#10156](https://github.com/apache/pulsar/pull/10156)
- Expose local run config metrics-port-start to CLI [#10185](https://github.com/apache/pulsar/pull/10185)
- Fix Pulsar Function localrun with multiple instances and metrics server is enabled [#10208](https://github.com/apache/pulsar/pull/10208)
- Improve localrun performance by using JVM options [#10273](https://github.com/apache/pulsar/pull/10273)
- Fix function API can not use authdata to check superuser [#10364](https://github.com/apache/pulsar/pull/10364)
- Fix potential bug getting stats and remove unnecessary error log [#10500](https://github.com/apache/pulsar/pull/10500)
- Fix deadlock on Monitoring thread blocked by LeaderService.isLeader() [#10502](https://github.com/apache/pulsar/pull/10502)
- Allow user to set custom configs to plugin worker service [#10504](https://github.com/apache/pulsar/pull/10504)
- K8s Function Name Length Check Allows Invalid StatefulSet [#10531](https://github.com/apache/pulsar/pull/10531)
- Get function cluster from broker config when start function worker with broker [#10552](https://github.com/apache/pulsar/pull/10552)
- Process async results in the same Java runnable thread [#10618](https://github.com/apache/pulsar/pull/10618)
- Support using AutoProduceBytesSchema as the function output schema [#10716](https://github.com/apache/pulsar/pull/10716)
- ReflectionUtils use Class.forName in order to properly discover classes in Functions Runtime while using DefaultImplementation [#10827](https://github.com/apache/pulsar/pull/10827)
- Fix the out of index issue when dispatch messages based on the avgBatchSizePerMsg [#10828](https://github.com/apache/pulsar/pull/10828)

#### IO Connectors
- [Kafka Source Connector] Fix invalid topic name generation [#9035](https://github.com/apache/pulsar/pull/9035)
- [Kafka Source Connector] Allow managing Avro encoded messages [#9448](https://github.com/apache/pulsar/pull/9448)
- Configure Pulsar IO connector YAML file generator for all connectors [#9629](https://github.com/apache/pulsar/pull/9629)
- [Kafka Source Connector] KeyValue schema support for KafkaBytesSource [#10002](https://github.com/apache/pulsar/pull/10002)
- Allow using GenericObject for developing a Sink connector [#10034](https://github.com/apache/pulsar/pull/10034)
- Sink<GenericObject> unwrap internal AutoConsumeSchema and allow to handle topics with KeyValue schema [#10211](https://github.com/apache/pulsar/pull/10211)
- Using ObjectMapper instead of Gson to parse Source/SInk configuration [#10441](https://github.com/apache/pulsar/pull/10441)
- Expose subscription type in the SinkContext [#10446](https://github.com/apache/pulsar/pull/10446)
- Add the ability to seek/pause/resume for a topic to the SinkContext [#10498](https://github.com/apache/pulsar/pull/10498)
- Use Message.getReaderSchema() in Pulsar IO Sinks when possible [#10557](https://github.com/apache/pulsar/pull/10557)
- [Kinesis Sink Connector] Fix backoff class not found [#10744](https://github.com/apache/pulsar/pull/10744)
- [Kinesis Sink Connector] Fix does not acknowledge messages [#10769](https://github.com/apache/pulsar/pull/10769)
- [Kafka Sink Adaptor] Support non-primitive schemas [#10410](https://github.com/apache/pulsar/pull/10410)

#### Pulsar SQL
- Fix the misleading setting in presto configuration [#8549](https://github.com/apache/pulsar/pull/8549)
- Fix injection factory cast error [#9472](https://github.com/apache/pulsar/pull/9472)
- Add max split entry queue size bytes limitation [#9628](https://github.com/apache/pulsar/pull/9628)
- Fix Pulsar SQL query bytes schema data error [#9631](https://github.com/apache/pulsar/pull/9631)
- Fix Pulsar SQL query compression data [#9663](https://github.com/apache/pulsar/pull/9663)
- Support native Protobuf decoder [#9841](https://github.com/apache/pulsar/pull/9841)
- Support query uppercase topic name [#9980](https://github.com/apache/pulsar/pull/9980)
- Only return persistent topic when list tables [#10368](https://github.com/apache/pulsar/pull/10368)
- Fix Presto startup on JDK11 [#10559](https://github.com/apache/pulsar/pull/10559)
- Fix pulsar sql issues when run select count(*) for the table with primary schema [#10840](https://github.com/apache/pulsar/pull/10840)

#### Tiered Storage
- Fix AWS credentials usages [#8950](https://github.com/apache/pulsar/pull/8950)
- Support ALI OSS tiered storage [#8985](https://github.com/apache/pulsar/pull/8985)

#### Proxy
- Fix the proxy does not support partitioned topic auto-creation type [#7903](https://github.com/apache/pulsar/issues/7903)
- Enable Conscrypt for Jetty in the Broker and in the Proxy [#10541](https://github.com/apache/pulsar/pull/10541)

#### Java Client
- Fix warn log on the producer side when duplicated messages have been dropped [#8729](https://github.com/apache/pulsar/pull/8729)
- Allow setting subscription name for Reader [#8801](https://github.com/apache/pulsar/pull/8801)
- Fix authParams showing in log with secret string(*****) [#8910](https://github.com/apache/pulsar/pull/8910)
- Avoid enabling DLQ on Key_Shared subscription [#9163](https://github.com/apache/pulsar/pull/9163)
- Add support for the JSON format token [#9313](https://github.com/apache/pulsar/pull/9313)
- Expose reached end of topic API for Reader/Consumer [#9381](https://github.com/apache/pulsar/pull/9381)
- Fix returned a completed future when acknowledging a batch message before complete the individual acknowledgments [#9383](https://github.com/apache/pulsar/pull/9383)
- Fix NPE when strip checksum for the producer [#9400](https://github.com/apache/pulsar/pull/9400)
- Fix inconsistent equals and hashCode for MessageIds [#9440](https://github.com/apache/pulsar/pull/9440)
- Allow disabling producer max queue size [#9650](https://github.com/apache/pulsar/pull/9650)
- Include pulsar-client-admin-api in the shaded version of pulsar-client-admin [#9689](https://github.com/apache/pulsar/pull/9689)
- Fix NPE in the ClientCnx [#9761](https://github.com/apache/pulsar/pull/9761)
- Fix DLQ can't work with AUTO_CONSUME schema [#9935](https://github.com/apache/pulsar/issues/9935)
- Fix NPE caused by null value of SchemaInfo's properties [#9985](https://github.com/apache/pulsar/pull/9985)
- Support multi-topic reader [#9995](https://github.com/apache/pulsar/pull/9995)
- Process messages from different partitions on different listener threads [#10017](https://github.com/apache/pulsar/pull/10017)
- Ensure close resource to avoid memory leak [#10028](https://github.com/apache/pulsar/pull/10028)
- Support set start message ID for each topic/partition on Reader [#10033](https://github.com/apache/pulsar/pull/10033)
- Add a JSON RecordBuilder to the GenericJsonSchema [#10052](https://github.com/apache/pulsar/pull/10052)
- Allow GenericRecord to wrap any Java Object [#10057](https://github.com/apache/pulsar/pull/10057)
- Fix NPE while configuring consumer builder [#10063](https://github.com/apache/pulsar/pull/10063)
- Support get native schema [#10076](https://github.com/apache/pulsar/pull/10076)
- Support KeyValue on Message.getValue() when using AutoConsumeSchema [#10107](https://github.com/apache/pulsar/pull/10107)
- Change the default retry topic name and dead letter topic name [#10129](https://github.com/apache/pulsar/pull/10129)
- Fix KeyValue with SEPARATED encoding for the GenericObject [#10186](https://github.com/apache/pulsar/pull/10186)
- Ensure download schema before decoding the payload for the AUTO_CONSUME schema [#10248](https://github.com/apache/pulsar/pull/10248)
- Fix typo of the maxPendingChunkedMessage method [#10223](https://github.com/apache/pulsar/pull/10223)
- Trait NONE schema as BYTE schema for AUTO_CONSUME schema [#10277](https://github.com/apache/pulsar/pull/10277)
- Fix pause consume issue with MultiTopicsConsumerImpl [#10305](https://github.com/apache/pulsar/pull/10305)
- Make message consumption thread safe and lock-free [#10352](https://github.com/apache/pulsar/pull/10352)
- Reset state before recycling OpSendMsg instance [#10405](https://github.com/apache/pulsar/pull/10405)
- Fix hasMessageAvailable return true but can't read message [#10414](https://github.com/apache/pulsar/pull/10414)
- Fix NPE in GenericJsonRecord [#10482](https://github.com/apache/pulsar/pull/10482)
- Fix behaviour of Schema.AUTO_CONSUME() with KeyValueSchema and multi versions [#10492](https://github.com/apache/pulsar/pull/10492)
- Avoid sending flow requests with zero permits [#10507](https://github.com/apache/pulsar/pull/10507)
- Make failPendingMessages called from within the ProducerImpl object mutex [#10528](https://github.com/apache/pulsar/pull/10528)
- Add schemaType field in SchemaHash [#10573](https://github.com/apache/pulsar/pull/10573)
- Fix NPE when ACK grouping tracker checks duplicated message id [#10586](https://github.com/apache/pulsar/pull/10586)
- Support consume multiple schema types messages by AutoConsumeSchema [#10604](https://github.com/apache/pulsar/pull/10604)
- Fixed issues in pulsar-client shading configuration [#10614](https://github.com/apache/pulsar/pull/10614)
- MessageCrypto interface should not expose Netty ByteBuf class in the API [#10616](https://github.com/apache/pulsar/pull/10616)
- Added org.apache.bookkeeper:cpu-affinity to shaded profile [#10681](https://github.com/apache/pulsar/pull/10681)
- Skip the periodic re-check of the partitions count on non-partitioned topic [#10708](https://github.com/apache/pulsar/pull/10708)
- Unlock the write lock of the UnAckedMessageTracker before call redeliverUnacknowledgedMessages [#10768](https://github.com/apache/pulsar/pull/10768)
- Fix AutoConsumeSchema decode data without schema version [#10811](https://github.com/apache/pulsar/pull/10811)

#### C++ Client
- Fix dangling reference bug in getRandomName [#8596](https://github.com/apache/pulsar/pull/8596)
- Optimize batch message buffer allocation [#8749](https://github.com/apache/pulsar/pull/8749)
- Make pool connections configurable in perf tools [#8913](https://github.com/apache/pulsar/pull/8913)
- Support setting listener name [#9119](https://github.com/apache/pulsar/pull/9119)
- Fix batch message handling of the UnAckedMessageTracker [#9170](https://github.com/apache/pulsar/pull/9170)
- Fix ServerError is not converted to string in log [#9277](https://github.com/apache/pulsar/pull/9277)
- Remove Boost::System runtime dependency [#9498](https://github.com/apache/pulsar/pull/9498)
- Removed usages of boost::regex [#9533](https://github.com/apache/pulsar/pull/9533)
- Account for different variables names on different CMake versions [#9559](https://github.com/apache/pulsar/pull/9559)
- Allow to disable static or dynamic lib at build time [#9570](https://github.com/apache/pulsar/pull/9570)
- Avoid multiple compilations of same source files [#9675](https://github.com/apache/pulsar/pull/9675)
- Support configure debug level logs simply [#10031](https://github.com/apache/pulsar/pull/10031)
- Add /opt/homebrew/ as a possible path for OpenSSL on Mac [#10141](https://github.com/apache/pulsar/pull/10141)
- Fix race condition in MemoryLimitController [#10142](https://github.com/apache/pulsar/pull/10142)
- Fix releasing semaphore and memory quota after send timeout [#10144](https://github.com/apache/pulsar/pull/10144)
- Allow configuring memory limit from C API [#10145](https://github.com/apache/pulsar/pull/10145)
- Fix use-after-free undefined behavior due to object lifetime problem [#10220](https://github.com/apache/pulsar/pull/10220)
- Support enable replicate subscription [#10243](https://github.com/apache/pulsar/pull/10243)
- Fix C++ client cannot be built with Boost <=1.53 [#10307](https://github.com/apache/pulsar/pull/10307)
- Support check connect state [#10349](https://github.com/apache/pulsar/pull/10349)
- Avoid sending flow requests with zero permits [#10506](https://github.com/apache/pulsar/pull/10506)
- Add single file logger factory [#10712](https://github.com/apache/pulsar/pull/10712)
- Reduce redeliverMessages when message listener is enabled [#10726](https://github.com/apache/pulsar/pull/10726)

#### Python Client
- Replace Exceptions with PulsarExceptions [#7600](https://github.com/apache/pulsar/pull/7600)
- Added logger wrapper support to make logging configurable [#7713](https://github.com/apache/pulsar/pull/7713)
- Initial python 3.9 client wheel build support [#9389](https://github.com/apache/pulsar/pull/9389)
- Support enable replicate subscription [#10243](https://github.com/apache/pulsar/pull/10243)

#### WebSocket
- Negative acknowledge support [#8249](https://github.com/apache/pulsar/pull/8249)
- Support deliverAt and deliverAfter attribute [#8945](https://github.com/apache/pulsar/pull/8945)
- Fix returned status code does not depend on the exception type [#9031](https://github.com/apache/pulsar/pull/9031)
- Allow to consume and pass message to client without decryption [#10026](https://github.com/apache/pulsar/pull/10026)
- Support pong command [#10035](https://github.com/apache/pulsar/pull/10035)

#### Pulsar Admin
- Support get applied message TTL policy for a topic [#9225](https://github.com/apache/pulsar/pull/9225)
- Support get applied inactive topic policy for a topic [#9230](https://github.com/apache/pulsar/pull/9230)
- Support get applied delayed delivery policy for a topic [#9245](https://github.com/apache/pulsar/pull/9245)
- Support get applied max unacked message per subscription policy for a topic [#9290](https://github.com/apache/pulsar/pull/9290)
- Support get applied max producer for a topic [#9293](https://github.com/apache/pulsar/pull/9293)
- Support get applied max consumer for a topic [#9296](https://github.com/apache/pulsar/pull/9296)
- Support get applied deduplication status policy for a topic [#9339](https://github.com/apache/pulsar/pull/9339)
- Support get applied retention policy for a topic [#9362](https://github.com/apache/pulsar/pull/9362)
- Support get applied offloader policy for a topic [#9505](https://github.com/apache/pulsar/pull/9505)
- Support get applied max unacked messages per consumer for a topic [#9694](https://github.com/apache/pulsar/pull/9694)
- Support get applied dispatch rate policy for a topic [#9824](https://github.com/apache/pulsar/pull/9824)
- Support get applied subscription dispatch rate policy for a topic [#9827](https://github.com/apache/pulsar/pull/9827)
- Support get applied backlog quota policy for a topic [#9828](https://github.com/apache/pulsar/pull/9828)
- Support get applied persistence policy for a topic [#9831](https://github.com/apache/pulsar/pull/9831)
- Support get applied cluster subscribe rate for a topic [#9832](https://github.com/apache/pulsar/pull/9832)
- Support get applied replicator dispatch rate for a topic [#9833](https://github.com/apache/pulsar/pull/9833)
- Support get applied compaction threshold [#10038](https://github.com/apache/pulsar/pull/10038)
- Lazily init PulsarAdmin in PulsarAdminTool [#9312](https://github.com/apache/pulsar/pull/9312)
- Fix create partition of existing topic does not throw RestException [#9342](https://github.com/apache/pulsar/pull/9342)
- Support get service URL of the leader broker [#9799](https://github.com/apache/pulsar/pull/9799)
- Support get persistent topics or non-persistent topics for pulsar admin client [#9877](https://github.com/apache/pulsar/pull/9877)
- Fix can not disable and remove max consumer per subscription [#10070](https://github.com/apache/pulsar/pull/10070)
- Add support for setting time based limit on backlog quota [#10401](https://github.com/apache/pulsar/pull/10401)
- Make client-admin-api to use interfaces with builders instead of POJOs [#10818](https://github.com/apache/pulsar/pull/10818)

#### Tools
- [Pulsar Perf] Support load WebSocket service URL from config file [#9000)](https://github.com/apache/pulsar/pull/9000)
- [BookKeeper Shell] Make bookkeeper shell more user friendly [#9281](https://github.com/apache/pulsar/pull/9281)
- [Client Tools] Supports end-to-end encryption [#9615](https://github.com/apache/pulsar/pull/9615)
- [Pulsar Perf] Support specify topics and subscriptions [#9716](https://github.com/apache/pulsar/pull/9716)
- [Client Tools] Allow printing GenericRecord contents [#9785](https://github.com/apache/pulsar/pull/9785)
- [Pulsar Perf] Fix compatibility issues with previous version [#9838](https://github.com/apache/pulsar/pull/9838)
- [Pulsar Perf] Add ability to create partitioned topics [#9859](https://github.com/apache/pulsar/pull/9859)
- [Client Tools] handle data with different schemas when consuming messages [#10301](https://github.com/apache/pulsar/pull/10301)
- [Client Tools] Fix NoClassDefFoundError when running pulsar cli tools in dev directory [#10807](https://github.com/apache/pulsar/pull/10807)

#### Dependencies
- Upgrade Kubernetes client and remove sundr-codegen [#8576](https://github.com/apache/pulsar/pull/8576)
- Upgrade ZooKeeper to 3.6.2 and Curator to 5.1.0 [#8549](https://github.com/apache/pulsar/pull/8549)
- [CVE-2020-26238] Upgrade cron-utils to 9.1.3 and sprint-context to 5.3.1 [#8822](https://github.com/apache/pulsar/pull/8822)
- Upgrade Swagger to 1.6.2 and Maven Swagger Plugin to 3.1.7 [#8845](https://github.com/apache/pulsar/pull/8845)
- Upgrade to Apache Avro 1.10.2 [#9898](https://github.com/apache/pulsar/pull/9898)
- Add jersey-client as dependency of pulsar-client-auth-sasl [#10055](https://github.com/apache/pulsar/pull/10055)
- Upgrade Athenz version to 1.10.9 and remove yahoo.bintray.com repository [#10079](https://github.com/apache/pulsar/pull/10079)
- [CVE-2020-15250] Upgrade junit version to 4.13.1 [#10147](https://github.com/apache/pulsar/pull/10147)
- [CVE-2020-8908,CVE-2018-10237] Upgrade jclouds to 2.3.0 [#10149](https://github.com/apache/pulsar/pull/10149)
- Remove jackson-mapper-asl dependency to resolve multiple CVEs [#10262](https://github.com/apache/pulsar/pull/10262)
- [CVE-2021-21409] Upgrade Netty to 4.1.63.Final [#10266](https://github.com/apache/pulsar/pull/10266)
- [CVE-2018-12541] Upgrade vertx to 3.9.7 [#10261](https://github.com/apache/pulsar/pull/10261)
- Upgrade BookKeeper to 4.14.1 [#10686](https://github.com/apache/pulsar/pull/10686)
- [CVE-2021-28169] Upgrade Jetty to 9.4.42.v20210604 [#10907](https://github.com/apache/pulsar/pull/10907)

### 2.6.4 &mdash; 2021-06-02 <a id=“2.6.4”></a>

#### Broker
- Disallow parsing of token with none signature in authenticateToken [#9172](https://github.com/apache/pulsar/pull/9172)
- Fix marking individual deletes as dirty [#9732](https://github.com/apache/pulsar/pull/9732)
- Issue 9082: Broker expires messages one at a time after topic unload [#9083](https://github.com/apache/pulsar/pull/9083)
- [logging] Upgrade Log4j2 version to 2.14.0, replace legacy log4j dependency with log4j-1.2-api [#8880](https://github.com/apache/pulsar/pull/8880)
- Upgrade Bouncy castle to newest version [#8047](https://github.com/apache/pulsar/pull/8047)
- Fixed logic for forceful topic deletion [#7356](https://github.com/apache/pulsar/pull/7356)
- Perform periodic flush of ManagedCursor mark-delete posistions [#8634](https://github.com/apache/pulsar/pull/8634)
- Fix the batch index ack persistent issue. [#9504](https://github.com/apache/pulsar/pull/9504)
- Fix the partition number not equals expected error [#9446](https://github.com/apache/pulsar/pull/9446)
- fix the closed ledger did not delete after expired [#9136](https://github.com/apache/pulsar/pull/9136)
- Fix testBrokerSelectionForAntiAffinityGroup by increasing OverloadedThreshold [#9393](https://github.com/apache/pulsar/pull/9393)

### Tiered storage
- [tiered-storage] Allow AWS credentials to be refreshed [#9387](https://github.com/apache/pulsar/pull/9387)

### Java client
- Compression must be applied during deferred schema preparation and enableBatching is enabled [#9396](https://github.com/apache/pulsar/pull/9396)

### C++ client
- [C++] Remove namespace check for MultiTopicsConsumerImpl [#9520](https://github.com/apache/pulsar/pull/9520)


### 2.7.3 &mdash; 2021-07-27 <a id=“2.7.3”></a>

#### Broker
- Fix Delayed Messages. [#11374](https://github.com/apache/pulsar/pull/11374)
- Fix missing replicator metrics. [#11264](https://github.com/apache/pulsar/pull/11264)
- Fix branch-2.7 test. [#11254](https://github.com/apache/pulsar/pull/11254)
- Fix broker dispatch byte rate limiter [#11249](https://github.com/apache/pulsar/pull/11249)
- Fix websocket TLS bug [#11243](https://github.com/apache/pulsar/pull/11243)
- Fix init WebSocketService with ClusterData [#11234](https://github.com/apache/pulsar/pull/11234)
- Fix ledger roll over scheduler task [#11226](https://github.com/apache/pulsar/pull/11226)
- Remove unused listener to reduce creating executor pool [#11215](https://github.com/apache/pulsar/pull/11215)
- Make the compaction phase one loop timeout configurable [#11206](https://github.com/apache/pulsar/pull/11206)
- Fix failing auth test. [#11186](https://github.com/apache/pulsar/pull/11186)
- Fix the dead lock when using hasMessageAvailableAsync and readNextAsync [#11183](https://github.com/apache/pulsar/pull/11183)
- Fix compaction entry read exception [#11175](https://github.com/apache/pulsar/pull/11175)
- On multi-topic consumer, we shouldn't keep checking the partitioned metadata [#11168](https://github.com/apache/pulsar/pull/11168)
- Fix replay topic policy message not work [#11136](https://github.com/apache/pulsar/pull/11136)
- Fix broker dispatch byte rate limiter. [#11135](https://github.com/apache/pulsar/pull/11135)
- Change test group to broker for ReplicatorTest and fix the test [#11134](https://github.com/apache/pulsar/pull/11134)
- Fix ledger rollover scheduled task [#11116](https://github.com/apache/pulsar/pull/11116)
- Fix race condition of the SystemTopicBasedTopicPoliciesService [#11097](https://github.com/apache/pulsar/pull/11097)
- Print message metadata when getting message by id [#11092](https://github.com/apache/pulsar/pull/11092)
- Fix flaky test testEnableAndDisableTopicDelayedDelivery [#11009](https://github.com/apache/pulsar/pull/11009)
- Fix potential data lost on the system topic when topic compaction has not triggered yet [#11003](https://github.com/apache/pulsar/pull/11003)
- Fix direct memory leak in getLastMessageId [#10977](https://github.com/apache/pulsar/pull/10977)
- Fix the backlog issue with --precise-backlog=true [#10966](https://github.com/apache/pulsar/pull/10966)
- Fix NonRecoverableLedgerException when get last message ID by Reader [#10957](https://github.com/apache/pulsar/pull/10957)
- Fix compaction not working for system topic [#10941](https://github.com/apache/pulsar/pull/10941)
- Fix issue where Key_Shared consumers could get stuck [#10920](https://github.com/apache/pulsar/pull/10920)
- When the Replicator is enabled, no managedLedger is created when updating the number of partitions [#10910](https://github.com/apache/pulsar/pull/10910)
- Handle multiple topic creation for the same topic-name in broker [#10847](https://github.com/apache/pulsar/pull/10847)
- Release OpAddEntry.data when entry is copied and discarded [#10773](https://github.com/apache/pulsar/pull/10773)
- Fix issue that message ordering could be broken when redelivering messages on Key_Shared subscription [#10762](https://github.com/apache/pulsar/pull/10762)
- Fix solution for preventing race conditions between timeout and completion [#10740](https://github.com/apache/pulsar/pull/10740)
- Cancel scheduled tasks as the first step in closing [#10739](https://github.com/apache/pulsar/pull/10739)
- MINOR: Add error message to setMaxPendingMessagesAcrossPartitions [#10709](https://github.com/apache/pulsar/pull/10709)
- Make PrometheusMetricsTest. testAuthMetrics pass on CI [#10699](https://github.com/apache/pulsar/pull/10699)
- Fix issue where StackOverflowError occurs when trying to redeliver a large number of already acked messages [#10696](https://github.com/apache/pulsar/pull/10696)
- Revert "Creating a topic does not wait for creating cursor of replicators" [#10674](https://github.com/apache/pulsar/pull/10674)
- Use single instance of parser [#10664](https://github.com/apache/pulsar/pull/10664)
- Ensure all the ReadHandle gets properly closed on cache invalidation [#10659](https://github.com/apache/pulsar/pull/10659)
- Fix ConcurrentOpenLongPairRangeSet remove all ranges [#10656](https://github.com/apache/pulsar/pull/10656)
- TopicPoliciesTest.testMaxSubscriptionsFailFast fails [#10640](https://github.com/apache/pulsar/pull/10640)
- Add metrics for non-contiguous deleted messages range [#10638](https://github.com/apache/pulsar/pull/10638)
- Fixed missed ZK caching when fetching a list of namespaces for a tenant [#10594](https://github.com/apache/pulsar/pull/10594)
- Made OpAddEntry.toString() more robust to nulls to prevent NPEs [#10548](https://github.com/apache/pulsar/pull/10548)
- Fix partitioned system topic check bug [#10529](https://github.com/apache/pulsar/pull/10529)
- Make failPendingMessages called from within the ProducerImpl object mutex [#10528](https://github.com/apache/pulsar/pull/10528)
- Fix deadlock on Monitoring thread blocked by LeaderService.isLeader() [#10512](https://github.com/apache/pulsar/pull/10512)
- Fix: Topic loading fails without any error when replicator init fails [#10432](https://github.com/apache/pulsar/pull/10432)
- Fix hasMessageAvailable return true but can't read message [#10414](https://github.com/apache/pulsar/pull/10414)
- Added more unit tests to the JavaInstanceTest class [#10369](https://github.com/apache/pulsar/pull/10369)
- Fix authorization error if partition number of partitioned topic is updated. [#10333](https://github.com/apache/pulsar/pull/10333)
- Fix the inconsistency of AdvertisedAddress [#10312](https://github.com/apache/pulsar/pull/10312)
- Fix missing LoggerFactoryPtr type. [#10164](https://github.com/apache/pulsar/pull/10164)
- Ensure read-lock is not continuously held on a section while iterating over concurrent maps [#9787](https://github.com/apache/pulsar/pull/9787)
- Zookeeper connections monitor data [#9778](https://github.com/apache/pulsar/pull/9778)
- Change getWorkerService method to throw UnsupportedOperationException [#9738](https://github.com/apache/pulsar/pull/9738)
- Fix flaky unit test [#9262](https://github.com/apache/pulsar/pull/9262)
- Supply debug log for OpAddEntry [#9239](https://github.com/apache/pulsar/pull/9239)

#### Dependency upgrade
- Upgrade Jetty to 9.4.42.v20210604 [#10907](https://github.com/apache/pulsar/pull/10907)

#### Proxy
- Enable AutoTopicCreationType partitioned through proxy [#8048](https://github.com/apache/pulsar/pull/8048)

#### Pulsar Admin
- Fix create partitioned topic in replicated namespace [#11140](https://github.com/apache/pulsar/pull/11140)
- Add authoritative flag for topic policy to avoid redirect loop [#11131](https://github.com/apache/pulsar/pull/11131)
- Fix non-persistent topic get partitioned metadata error on discovery [#10806](https://github.com/apache/pulsar/pull/10806)
- Fix kinesis sink backoff class not found [#10744](https://github.com/apache/pulsar/pull/10744)

#### Docker
- K8s Function Name Length Check Allows Invalid StatefulSet  [#10531](https://github.com/apache/pulsar/pull/10531)

#### Client
- [Java] Cleaned some code in GenericJsonRecord [#10527](https://github.com/apache/pulsar/pull/10527)
- [C++] Avoid sending flow requests with zero permits [#10506](https://github.com/apache/pulsar/pull/10506)

#### Functions and Pulsar IO
- Fix kinesis sink connector does not ack messages [#10769](https://github.com/apache/pulsar/pull/10769)
- Remove reference to ProducerSpec from Pulsar Functions GO [#10635](https://github.com/apache/pulsar/pull/10635)
- Process async results in the same Java runnable thread [#10618](https://github.com/apache/pulsar/pull/10618)


### 2.7.2 &mdash; 2021-05-11 <a id=“2.7.2”></a>

#### Broker
- Fix the useless retry when the maximum number of subscriptions is reached [#9991](https://github.com/apache/pulsar/pull/9991)
- wrong timeunit in updating lastLedgerCreationInitiationTimestamp [#10049](https://github.com/apache/pulsar/pull/10049)
- Avoid spammy logs in case of BK problems [#10088](https://github.com/apache/pulsar/pull/10088)
- Fix NonDurableCursorImpl initialPosition by startCursorPosition greater than lastConfirmedEntry problem. [#10095](https://github.com/apache/pulsar/pull/10095)
- fix 8115 Some partitions get stuck after adding additional consumers to the KEY_SHARED subscriptions [#10096](https://github.com/apache/pulsar/pull/10096)
- Add underReplicate state in the topic internal stats [#10013](https://github.com/apache/pulsar/pull/10013)
- Continue graceful shutdown even if web service closing fails [#9835](https://github.com/apache/pulsar/pull/9835)
- Issue 9804: Allow to enable or disable the cursor metrics [#9814](https://github.com/apache/pulsar/pull/9814)
- Allow to configure BookKeeper all BK client features using bookkeeper_ prefix [#9232](https://github.com/apache/pulsar/pull/9232)
- Fix NPEs and thread safety issue in PersistentReplicator [#9763](https://github.com/apache/pulsar/pull/9763)
- Non Persistent Topics: Auto-create partitions even when the auto-creation is disabled [#9786](https://github.com/apache/pulsar/pull/9786)
- Issue 9602: Add schema type validation [#9797](https://github.com/apache/pulsar/pull/9797)
- Fix message not dispatch for key_shared sub type in non-persistent subscription [#9826](https://github.com/apache/pulsar/pull/9826)
- zkBookieRackAffinityMapping bug to support for bookkeeper dnsResolver [#9894](https://github.com/apache/pulsar/pull/9894)
- Messaging Fix delay message block [#10078](https://github.com/apache/pulsar/pull/10078)
- Make PersistentDispatcherMultipleConsumers.readMoreEntries synchronized [#10435](https://github.com/apache/pulsar/pull/10435)
- Fix issue in reusing EntryBatchIndexesAcks instances [#10400](https://github.com/apache/pulsar/pull/10400)
- Fix schema not added when subscribing an empty topic without schema [#9853](https://github.com/apache/pulsar/pull/9853)
- Support advertisedListeners for standalone [#10297](https://github.com/apache/pulsar/pull/10297)
- Fix schema ledger deletion when deleting topic with delete schema. [#10383](https://github.com/apache/pulsar/pull/10383)
- Fix primitive schema upload for ALWAYS_COMPATIBLE strategy. [#10386](https://github.com/apache/pulsar/pull/10386)
- Fix schema type check issue when use always compatible strategy [#10367](https://github.com/apache/pulsar/pull/10367)
- Fix CPU 100% when deleting namespace [#10337](https://github.com/apache/pulsar/pull/10337)
- add return statement to exit asyncMarkDelete early on failure [#10272](https://github.com/apache/pulsar/pull/10272)
- Adding more permits debug statements to better diagnose permit issues [#10217](https://github.com/apache/pulsar/pull/10217)

#### Bookie
- Fallback to PULSAR_GC if BOOKIE_GC is not defined [#9621](https://github.com/apache/pulsar/pull/9621)
- Fallback to PULSAR_EXTRA_OPTS if BOOKIE_EXTRA_OPTS isn't defined [#10397](https://github.com/apache/pulsar/pull/10397)

#### Dependency upgrade
- Upgrade Bouncy Castle to 1.68 [#9199](https://github.com/apache/pulsar/pull/9199)
- Upgrade athenz version and remove yahoo.bintray.com repository [#10471](https://github.com/apache/pulsar/pull/10471)
- Upgrade Netty version to 4.1.60.final [#10073](https://github.com/apache/pulsar/pull/10073)
- Upgrade commons-io to address CVE-2021-29425 [#10287](https://github.com/apache/pulsar/pull/10287)
- Upgrade Jetty libraries to 9.4.39.v20210325 [#10177](https://github.com/apache/pulsar/pull/10177)

#### Proxy
- Issue 10221: Fix authorization error while using proxy and `Prefix` subscription authentication mode [#10226](https://github.com/apache/pulsar/pull/10226)

#### Pulsar Admin
- Add get version command for pulsar rest api, pulsar-admin, pulsar-client [#9975](https://github.com/apache/pulsar/pull/9975)

#### Pulsar SQL
- Using pulsar SQL query messages will appear NoSuchLedger… [#9910](https://github.com/apache/pulsar/pull/9910)

#### Docker
- Allow DockerImage to be built from source tarball [#9846](https://github.com/apache/pulsar/pull/9846)
- Fix docker standalone image error [#10359](https://github.com/apache/pulsar/pull/10359)
- Suppress printing of "skip Processing" lines in startup scripts [#10275](https://github.com/apache/pulsar/pull/10275)
- Issue 10058:apply-config-from-env.py to commented default values [#10060](https://github.com/apache/pulsar/pull/10060)

#### Client
- [Java] Fix: seemingly equal ClientConfigurationData's objects end up not being equal [#10091](https://github.com/apache/pulsar/pull/10091)
- [Java] Fix AutoConsumeSchema KeyValue encoding [#10089](https://github.com/apache/pulsar/pull/10089)
- [Java] Fix error OutOfMemoryError while using KeyValue<GenericRecord, GenericRecord> [#9981](https://github.com/apache/pulsar/pull/9981)
- [Java] Fix concurrency issue in incrementing epoch (#10278) [#10436](https://github.com/apache/pulsar/pull/10436)
- [Java] Allow pulsar client receive external timer [#9802](https://github.com/apache/pulsar/pull/9802)
- [Java] Handle NPE while receiving ack for closed producer [#8979](https://github.com/apache/pulsar/pull/8979)
- [Java] Fix batch size not set when deserializing from byte array [#9855](https://github.com/apache/pulsar/pull/9855)
- [Java] Fix ensure single-topic consumer can be closed [#9849](https://github.com/apache/pulsar/pull/9849)
- [Java] Issue 9585: delete disconnected consumers to allow auto-discovery [#9660](https://github.com/apache/pulsar/pull/9660)
- [Python] Support Python Avro schema set default value. [#10265](https://github.com/apache/pulsar/pull/10265)
- [Python] Fix nested Map or Array in schema doesn't work [#9548](https://github.com/apache/pulsar/pull/9548)
- [C++,Python] [PIP-60] Add TLS SNI support for cpp and python clients [#8957](https://github.com/apache/pulsar/pull/8957)
- [C++] Fix C++ client cannot be built on Windows [#10363](https://github.com/apache/pulsar/pull/10363)
- [C++] Fix paused zero queue consumer still pre-fetches messages [#10036](https://github.com/apache/pulsar/pull/10036)
- [C++] Fix segfault when get topic name from received message id [#10006](https://github.com/apache/pulsar/pull/10006)
- [C++] SinglePartition message router is always picking the same partition [#9702](https://github.com/apache/pulsar/pull/9702)
- [C++] Reduce log level for ack-grouping tracker [#10094](https://github.com/apache/pulsar/pull/10094)
- [WebSocket Client] WebSocket url token param value optimization [#10187](https://github.com/apache/pulsar/pull/10187)
- [WebSocket Client] Make the browser client support the token authentication [#9886](https://github.com/apache/pulsar/pull/9886)

#### Functions and Pulsar IO
- Allow customizable function logging [#10389](https://github.com/apache/pulsar/pull/10389)
- Pass through record properties from Pulsar Sources [#9943](https://github.com/apache/pulsar/pull/9943)
- ISSUE 10153: Pulsar Functions Go fix time unit ns -> ms [#10160](https://github.com/apache/pulsar/pull/10160)
- Kinesis Connector: Fix kinesis sink can not retry to send messages [#10420](https://github.com/apache/pulsar/pull/10420)
- Kinesis Connector: Fix null error messages in onFailure exception in KinesisSink. [#10416](https://github.com/apache/pulsar/pull/10416)

#### Tiered Storage
- Prevent Class Loader Leak; Restore Offloader Directory Override [#9878](https://github.com/apache/pulsar/pull/9878)
- Add logs for cleanup offloaded data operation [#9852](https://github.com/apache/pulsar/pull/9852)

### 2.7.1 &mdash; 2021-03-18 <a id=“2.7.1”></a>

#### Broker

- Fix topic ownership is not checked when getting topic policy [#9781](https://github.com/apache/pulsar/pull/9781)
- Fix the issue of consumers cannot be created for older subscriptions if the limit of `maxSubscriptionsPerTopic` is reached [#9758](https://github.com/apache/pulsar/pull/9758)
- Fix marking individual deletes as dirty [#9732](https://github.com/apache/pulsar/pull/9732)
- Fix broker-address header added when response has already been committed [#9744](https://github.com/apache/pulsar/pull/9744)
- Fix ByteBuffer allocate error in the AirliftUtils [#9667](https://github.com/apache/pulsar/pull/9667)
- Use Atomic Field Updater to increment volatile messagesConsumedCounter [#9656](https://github.com/apache/pulsar/pull/9656)
- Schema comparison logic change [#9612](https://github.com/apache/pulsar/pull/9612)
- Add metrics for the cursor ack state [#9618](https://github.com/apache/pulsar/pull/9618)
- Fix race condition in BrokerService topic cache [#9565](https://github.com/apache/pulsar/pull/9565)
- Avoid introducing bookkeeper-common into the pulsar-common [#9551](https://github.com/apache/pulsar/pull/9551)
- Async read entries with max size bytes [#9532](https://github.com/apache/pulsar/pull/9532)
- Fix the metric data of msgDelayed for partitioned topics is not aggregated [#9529](https://github.com/apache/pulsar/pull/9529)
- Fix the batch index ack persistent issue [#9504](https://github.com/apache/pulsar/pull/9504)
- Fix logic in ManagedLedgerWriter when config threadNum >= ledgerNum [#9479](https://github.com/apache/pulsar/pull/9497)
- Do not use a static map of listeners in TopicPoliciesService [#9486](https://github.com/apache/pulsar/pull/94861)
- Makes subscription start from MessageId.latest as default [#9444](https://github.com/apache/pulsar/pull/9444)
- Fix setting backlogQuota will always succeed [#9382](https://github.com/apache/pulsar/pull/9382)
- Skip clear delayed messages while dispatch does not init [#9378](https://github.com/apache/pulsar/pull/9378)
- Expose offloaded storage size to the topic stats [#9335](https://github.com/apache/pulsar/pull/9335)
- Expose more info with unknown exception [#9323](https://github.com/apache/pulsar/pull/9323)
- Add alerts for expired/expiring soon tokens [#9321](https://github.com/apache/pulsar/pull/9321)
- Fix fake complete issue in offloading [#9306](https://github.com/apache/pulsar/pull/9306)
- Fix system topic can not auto created [#9272](https://github.com/apache/pulsar/pull/9272)
- Fix BookkeeperSchemaStorage NPE [#9264](https://github.com/apache/pulsar/pull/9264)
- Fix race condition on producer/consumer maps in ServerCnx [#9256](https://github.com/apache/pulsar/pull/9256)
- Fix interceptor disabled in ResponseHandlerFilter.java [#9252](https://github.com/apache/pulsar/pull/9252)
- Fix the interceptor that not handle boundary for multipart/form-data [#9247](https://github.com/apache/pulsar/pull/9247)
- Add authentication metrics [#9244](https://github.com/apache/pulsar/pull/9244)
- Handle web application exception to redirect request [#9228](https://github.com/apache/pulsar/pull/9228)
- Skip the interceptor for MediaType.MULTIPART_FORM_DATA [#9217](https://github.com/apache/pulsar/pull/9217)
- Keep topic-level policies commands consistent with that for namespace level [#9215](https://github.com/apache/pulsar/pull/9215)
- Fix handle topic loading failure due to broken schema ledger [#9212](https://github.com/apache/pulsar/pull/9212)
- Fix issue with topic compaction when compaction ledger is empty [#9206](https://github.com/apache/pulsar/pull/9206)
- Fix incoming message size issue that introduced in #9113 [#9182](https://github.com/apache/pulsar/pull/9182)
- Disallow parsing of token with none signature in authenticateToken [#9172](https://github.com/apache/pulsar/pull/9172)
- Fix locking for ConsumerImpl when creating deadLetterProducer [#9166](https://github.com/apache/pulsar/pull/9166)
- Fix maxProducersPerTopic cannot be disabled at the namespace level [#9157](https://github.com/apache/pulsar/pull/9157)
- Fix wrong default value [#9149](https://github.com/apache/pulsar/pull/9149)
- Fix the closed ledger did not delete after expired [#9136](https://github.com/apache/pulsar/pull/9136)
- Additional error checks in TwoPhasesCompactor [#9133](https://github.com/apache/pulsar/pull/9133)
- Fix master broker while subscribing to non-persistent partitioned topics without topic auto-creation [#9107](https://github.com/apache/pulsar/pull/9107)
- Support chained authentication with same auth method name [#9094](https://github.com/apache/pulsar/pull/9094)
- Broker expires messages one at a time after topic unload [#9083](https://github.com/apache/pulsar/pull/9083)
- Add refresh authentication command in broker [#9064](https://github.com/apache/pulsar/pull/9064)
- Add updateRates method for kop to collect publish rate [#9094](https://github.com/apache/pulsar/pull/9049)
- Fix DelayedDelivery at the broker level has a default value [#9030](https://github.com/apache/pulsar/pull/9030)
- Getting the stats of a non-persistent topic that has been cleaned causes it to re-appear [#9029](https://github.com/apache/pulsar/pull/9029)
- Add raw Prometheus metrics provider [#9021](https://github.com/apache/pulsar/pull/9021)
- Improve error handling when broker doesn't trust client certificates [#8998](https://github.com/apache/pulsar/pull/8998)
- Remove duplicated broker Prometheus metrics type [8995](https://github.com/apache/pulsar/pull/8995)
- Peeking at compressed messages throws an exception (Readonly buffers not supported by Airlift) [#8990](https://github.com/apache/pulsar/pull/8990)
- Make namespaces isolation policy updates take effect on time [#8976](https://github.com/apache/pulsar/pull/8976)
- Fix NPE in PersistentStickyKeyDispatcherMultipleConsumers [#8969](https://github.com/apache/pulsar/pull/8969)
- Fix the recovery not respect to the isolation group settings [#8961](https://github.com/apache/pulsar/pull/8961)
- Add properties default value for SchemaInfoBuilder [#8952](https://github.com/apache/pulsar/pull/8952)
- Consumer support update stats with specified stats [#8951](https://github.com/apache/pulsar/pull/8951)
- Support configure max subscriptions per topic on the topic level policy [#8948](https://github.com/apache/pulsar/pull/8948)
- Fix subscription dispatch rate does not work after the topic unload without dispatch rate limit [#8947](https://github.com/apache/pulsar/pull/8947)
- Avro custom schema not working in consumer [#8939](https://github.com/apache/pulsar/pull/8939)
- Expose non-contiguous deleted messages ranges stats [#8936](https://github.com/apache/pulsar/pull/8936)
- Intercept beforeSendMessage calls [#8932](https://github.com/apache/pulsar/pull/8932)
- Monitor if a cursor moves its mark-delete position [#8930](https://github.com/apache/pulsar/pull/8930)
- Capture stats with precise backlog [#8928](https://github.com/apache/pulsar/pull/8928)
- Support configure max subscriptions per topic on the namespace level policy [#8924](https://github.com/apache/pulsar/pull/8924)
- Export Prometheus metric for messageTTL [#8871](https://github.com/apache/pulsar/pull/8871)
- Add pulsar-perf new feature: one subscription has more than one consumer [#8837](https://github.com/apache/pulsar/pull/8837)
- Execute removing non-persistent subscription of a topic from a different thread to avoid deadlock when removing inactive subscriptions [#8820](https://github.com/apache/pulsar/pull/8820)
- Fix get partition metadata problem for a non-existed topic [#8818](https://github.com/apache/pulsar/pull/8818)
- Fix the problem that batchMessageId is converted to messageIdImpl [#8779](https://github.com/apache/pulsar/pull/8779)
- Clear delayed messages when clear backlog [#8691](https://github.com/apache/pulsar/pull/8691)
- Fixes first automatic compaction issue [#8209](https://github.com/apache/pulsar/pull/8209)

#### Proxy

- Fix Proxy Config bindAddress does not working for servicePort [#9068](https://github.com/apache/pulsar/pull/9068)
- Return correct authz and auth errors from proxy to client [#9055](https://github.com/apache/pulsar/pull/9055)
- Fix the metadata setup compatibility issue [#8959](https://github.com/apache/pulsar/pull/8959)
- Support HAProxy proxy protocol for broker and proxy [#8686](https://github.com/apache/pulsar/pull/8686)

#### Pulsar Perf

- Dump JVM information [#9769](https://github.com/apache/pulsar/pull/9769)
- pulsar-perf uses DefaultCryptoKeyReader for E2E encryption  [#9668](https://github.com/apache/pulsar/pull/9668)
- Add --batch-index-ack for the pulsar-perf [#9521](https://github.com/apache/pulsar/pull/9521)

#### Transaction

- Fix deleteTransactionMarker memory leak [#9752](https://github.com/apache/pulsar/pull/9752)
- Fix transaction messages order error and deduplication error [#9024](https://github.com/apache/pulsar/pull/9024)
- Fix transaction log replay not handle right [#8723](https://github.com/apache/pulsar/pull/8723)

#### Pulsar Admin

- Validate offload param [#9737](https://github.com/apache/pulsar/pull/9737)
- Inform user when expiring message request is not executed. [#9561](https://github.com/apache/pulsar/pull/9561)
- Fix get-message-by-id throwing NPE when message is null [#9537](https://github.com/apache/pulsar/pull/9537)
- Expire message by position [#9519](https://github.com/apache/pulsar/pull/9519)
- Add subscription backlog size info for topicstats [#9302](https://github.com/apache/pulsar/pull/9302)
- Expose schema ledger in `topic stats-internal` [#9284](https://github.com/apache/pulsar/pull/9284)
- Fix potential HTTP get hangs in the Pulsar Admin [#9203](https://github.com/apache/pulsar/pull/9203)
- Fix admin-api-brokers list failed [#9191](https://github.com/apache/pulsar/pull/9191)
- Fix force delete namespace did not delete all topics of the namespace [#8806](https://github.com/apache/pulsar/pull/8806)
- Change method `getWebServiceUrl` into async [#8746](https://github.com/apache/pulsar/pull/8746)
- Fix cannot get lastMessageId for an empty topic due to message retention [#8725](https://github.com/apache/pulsar/pull/8725)

#### Pulsar SQL

- Duplicate key `__pfn_input_topic__` in presto server [#9686](https://github.com/apache/pulsar/pull/9686)
- Pulsar sql key-value schema separated model support [#9685](https://github.com/apache/pulsar/pull/9685)
- Fix OffloadPolicies json serialization error in Pulsar SQL [#9300](https://github.com/apache/pulsar/pull/9300)

#### Client

- [Java] Add original info when publishing message to dead letter topic [#9655](https://github.com/apache/pulsar/pull/9655)
- [Java] Fix hasMessageAvailable() with empty topic [#9798](https://github.com/apache/pulsar/pull/9798)
- [Java] Add BouncyCastleProvider as security provider to prevent NPE [#9601](https://github.com/apache/pulsar/pull/9601)
- [Java] Async the DLQ process [#9552](https://github.com/apache/pulsar/pull/9552)
- [Java] Fix the partition number not equals expected error [#9446](https://github.com/apache/pulsar/pull/9446)
- [Java] Cleanup consumer on multitopic subscribe failure [#9419](https://github.com/apache/pulsar/pull/9419)
- [Java] Compression must be applied during deferred schema preparation and enableBatching is enabled [#9396](https://github.com/apache/pulsar/pull/9396)
- [Java] Add default implementation of CryptoKeyReader [#9379](https://github.com/apache/pulsar/pull/9379)
- [Java] Able to handling messages with multiple listener threads in order for the Key_Shared subscription [#9329](https://github.com/apache/pulsar/pull/9329)
- [Java] Fix NPE when MultiTopicsConsumerImpl receives null value messages [#9113](https://github.com/apache/pulsar/pull/9113)
- [Java] Fix Unavailable Hash Range Condition [#9041](https://github.com/apache/pulsar/pull/9041)
- [Java] Add more information in send timeout exception [#8931](https://github.com/apache/pulsar/pull/8931)
- [Java] GenericJsonReader converts the null value to string "null" [#8883](https://github.com/apache/pulsar/pull/8883)
- [Java] Always remove message data size [#8566](https://github.com/apache/pulsar/pull/8566)
- [Python] Support python end to end encryption [#9588](https://github.com/apache/pulsar/pull/9588)
- [C++] Add 'encrypted' option in commands.newproducer() [#9542](https://github.com/apache/pulsar/pull/9542)
- [C++] Remove namespace check for MultiTopicsConsumerImpl [#9520](https://github.com/apache/pulsar/pull/9520)
- [C++] Fix broken replication msg to specific cluster [#9372](https://github.com/apache/pulsar/pull/9372)
- [C++] Fix compilation issue caused by non-virtual destructor [#9106](https://github.com/apache/pulsar/pull/9106)
- [C++] Expose cpp end to end encryption interface [#9074](https://github.com/apache/pulsar/pull/9074)
- [C++] Fix Consumer send redeliverMessages repeatedly [#9072](https://github.com/apache/pulsar/pull/9072)
- [C++] Add consumer's configs for reader [#8905](https://github.com/apache/pulsar/pull/8905)
- [C++] Add reader internal subscription name setter [#8823](https://github.com/apache/pulsar/pull/8823)
- [C++] Fix race condition in BlockingQueue [#8765](https://github.com/apache/pulsar/pull/8765)
- [C++] Fix cpp client do AcknowledgeCumulative not clean up previous message [#8606](https://github.com/apache/pulsar/pull/8606)
- [C++] Implement batch aware producer router [#8395](https://github.com/apache/pulsar/pull/8395)
- [Websocket] Fix the initial sequence id error [#8724](https://github.com/apache/pulsar/pull/8724)

#### Function

- Add downloadDirectory support to function k8s runtime [#9619](https://github.com/apache/pulsar/pull/9619)
- Kubernetes runtime functions create rfc1123 compliant labels [#9556](https://github.com/apache/pulsar/pull/9556)
- Fix can't create functions with m-TLS [#9553](https://github.com/apache/pulsar/pull/9553)
- Fix reading metrics will always get stuck in some cases [#9538](https://github.com/apache/pulsar/pull/9538)
- Call the corresponding restart according to the componentype [#9519](https://github.com/apache/pulsar/pull/9519)
- Fix narExtractionDirectory not set [#9319](https://github.com/apache/pulsar/pull/9319)
- Fix java function logging appender not added to java function logger [#9299](https://github.com/apache/pulsar/pull/9299)
- Fix don't attempt to clean up packages when Source/Sink is builtin [#9289](https://github.com/apache/pulsar/pull/9289)
- Fix function worker get superuser role [#9259](https://github.com/apache/pulsar/pull/9259)
- Fix broker and functions-worker authentication compatibility [#9190](https://github.com/apache/pulsar/pull/9190)
- Splitting the authentication logic of function worker and client [#8824](https://github.com/apache/pulsar/pull/8824)
- [Go] Fix metrics server handler error [#9394](https://github.com/apache/pulsar/pull/9394)
- [Go] Add metrics server to go function [#9318](https://github.com/apache/pulsar/pull/9318)
- [Go] Fix publishfunc example is broken [#9124](https://github.com/apache/pulsar/pull/9124)

#### Pulsar IO

- Add option for auto.offset.reset to kafka source [#9482](https://github.com/apache/pulsar/pull/9482)
- Fix debezium-connector error log [#9063](https://github.com/apache/pulsar/pull/9063)
- Fix NSQ source META-INF file name and sourceConfigClass [#8941](https://github.com/apache/pulsar/pull/8941)
- Make Source topic Schema information available to downstream Sinks [#8854](https://github.com/apache/pulsar/pull/8854)

#### Tiered Storage

- Allow AWS credentials to be refreshed [#9387](https://github.com/apache/pulsar/pull/9387)
- Offload manager initialization once [#8739](https://github.com/apache/pulsar/pull/8739)
- Configurable data source for offloaded messages [#8717](https://github.com/apache/pulsar/pull/8717)

### 2.6.3 &mdash; 2021-01-26 <a id=“2.6.3”></a>

#### Broker

- Update the BookKeeper to version 4.11.1 [#8604](https://github.com/apache/pulsar/pull/8604)
- Use the correct configuration for the expiration time of the ZooKeeper cache [#8302](https://github.com/apache/pulsar/pull/8302)
- Refresh ZooKeeper-data cache in background to avoid deadlock and blocking IO on the ZooKeeper thread [#8304](https://github.com/apache/pulsar/pull/8304)
- Add `elapsedMs` in the creation of the ledger log [#8473](https://github.com/apache/pulsar/pull/8473)
- Fix the race condition when calling `acknowledgementWasProcessed()` [#8499](https://github.com/apache/pulsar/pull/8499)
- Fix the way to handle errors for client requests [#8518](https://github.com/apache/pulsar/pull/8518)
- Expose consumer names after the mark delete position for the Key_Shared subscription [#8545](https://github.com/apache/pulsar/pull/8545)
- Close topics that remain fenced forcefully [#8561](https://github.com/apache/pulsar/pull/8561)
- Expose the last disconnected timestamp for producers and consumers [#8605](https://github.com/apache/pulsar/pull/8605)
- Support the HAProxy proxy protocol for Pulsar broker and Pulsar Proxy [#8686](https://github.com/apache/pulsar/pull/8686)
- Clear delayed messages when clearing the backlog [#8691](https://github.com/apache/pulsar/pull/8691)
- Fix the Jclouds Azure credential error [#8693](https://github.com/apache/pulsar/pull/8693)
- Improve environment configiguration handling [#8709](https://github.com/apache/pulsar/pull/8709)
- Fix the issue with failing to get `lastMessageId` for an empty topic due to message retention [#8725](https://github.com/apache/pulsar/pull/8725)
- Ensure that the Offload manager is initialized once [#8739](https://github.com/apache/pulsar/pull/8739)
- Fix the issue with getting partition metadata for a non-existed topic [#8818](https://github.com/apache/pulsar/pull/8818)
- Fix the exception cast error [#8828](https://github.com/apache/pulsar/pull/8828)
- Export Prometheus metric for messageTTL [#8871](https://github.com/apache/pulsar/pull/8871)
- Fix the issue that GenericJsonReader converts the null value to string "null" [#8883](https://github.com/apache/pulsar/pull/8883)
- Capture stats with precise backlog [#8928](https://github.com/apache/pulsar/pull/8928)
- Monitor if a cursor moves its mark-delete position [#8930](https://github.com/apache/pulsar/pull/8930)
- Intercept `beforeSendMessage` calls [#8932](https://github.com/apache/pulsar/pull/8932)
- Expose non-contiguous deleted messages ranges stats [#8936](https://github.com/apache/pulsar/pull/8936)
- Fix NPE in `PersistentStickyKeyDispatcherMultipleConsumers` [#8969](https://github.com/apache/pulsar/pull/8969)
- Fix the issue that an exception is thrown when peeking at compressed messages (Readonly buffers are not supported by Airlift) [#8990](https://github.com/apache/pulsar/pull/8990)
- Remove the duplicated broker Prometheus metrics type [#8995](https://github.com/apache/pulsar/pull/8995)
- Improve the way to handle errors when the broker does not trust client certificates [#8998](https://github.com/apache/pulsar/pull/8998)
- Add the raw Prometheus metrics provider [#9021](https://github.com/apache/pulsar/pull/9021)
- Support chained authentication with same authentication method name [#9094](https://github.com/apache/pulsar/pull/9094)
- Fix regression in apply-config-from-env.py [#9097](https://github.com/apache/pulsar/pull/9097)

#### Proxy

- Fix the `request.getContentLength()` to return 0 if it is less than 0 [#8448](https://github.com/apache/pulsar/pull/8448)
- Add the error log for the Pulsar Proxy starter [#8451](https://github.com/apache/pulsar/pull/8451)
- Support enabling WebSocket on Pulsar Proxy [#8613](https://github.com/apache/pulsar/pull/8613)
- Fix the issue that the Proxy `bindAddress` configuration does not work for the `servicePort` [#9068](https://github.com/apache/pulsar/pull/9068)

#### Java Client

- Fix the connection leak [#6524](https://github.com/apache/pulsar/pull/6524)
- Cancel the sendtimeout task for the producer after creation failure [#8497](https://github.com/apache/pulsar/pull/8497)
- Fix the typo in `pulsar-client-all` module's pom.xml file [#8543](https://github.com/apache/pulsar/pull/8543)
- Add more information in send timeout exception [#8931](https://github.com/apache/pulsar/pull/8931)
- Fix the unavailable hash range condition [#9041](https://github.com/apache/pulsar/pull/9041)
- Fix NPE when `MultiTopicsConsumerImpl` receives null-value messages [#9113](https://github.com/apache/pulsar/pull/9113)
- Fix the issue with the incoming message size that is introduced by issue #9113 [#9182](https://github.com/apache/pulsar/pull/9182)

#### C++ Client

- Catch the exception thrown by the remote_endpoint [#8486](https://github.com/apache/pulsar/pull/8486)
- Fix the potential crash caused by the AckGroupTracker's timer [#8519](https://github.com/apache/pulsar/pull/8519)
- Fix the race condition in `BlockingQueue` [#8765](https://github.com/apache/pulsar/pull/8765)
- Add the reader internal subscription name setter [#8823](https://github.com/apache/pulsar/pull/8823)
- Add consumer's configurations for the reader [#8905](https://github.com/apache/pulsar/pull/8905)

#### Python Client

- Add Oauth2 client wrapper for the python client [#7813](https://github.com/apache/pulsar/pull/7813)

#### Pulsar Perf

- Support WebSocket Producer for V2 topics [#8535](https://github.com/apache/pulsar/pull/8535)

#### Pulsar IO

- Make Schema information of Source topic available to downstream Sinks [#8854](https://github.com/apache/pulsar/pull/8854)
- Fix the error log of the Debezium connector [#9063](https://github.com/apache/pulsar/pull/9063)

#### Functions

- Propagate user-defined parameter into instances of Golang Pulsar Functions [#8132](https://github.com/apache/pulsar/pull/8132)
- Go functions supports Kubernetes runtime [#8352](https://github.com/apache/pulsar/pull/8352)

### 2.7.0 &mdash; 2020-11-25 <a id=“2.7.0”></a>

The following lists fixes and enhancements in the 2.7.0 release.

#### Transactions

- Implement the Transaction Buffer Client [#6544](https://github.com/apache/pulsar/pull/6544)
- Support produce messages with transaction and commit transaction [#7552](https://github.com/apache/pulsar/pull/7552)
- Support consume transaction messages [#7781](https://github.com/apache/pulsar/pull/7781) [#7833](https://github.com/apache/pulsar/pull/7833)
- Message acknowledgment with transaction [#7856](https://github.com/apache/pulsar/pull/7856) [#8007](https://github.com/apache/pulsar/pull/8007)
- Support transaction abort on partition [#7953](https://github.com/apache/pulsar/pull/7953)
- Support transaction abort on subscription [#7979](https://github.com/apache/pulsar/pull/7979)
- Handle pending ack at the client side [#8037](https://github.com/apache/pulsar/pull/8037)
- Pending ack state implementation [#8426](https://github.com/apache/pulsar/pull/8426)
- Support get reponse for message acknowledge [#8161](https://github.com/apache/pulsar/pull/8161)
- Refactor the transaction buffer implementation [#8291](https://github.com/apache/pulsar/pull/8291) [#8347](https://github.com/apache/pulsar/pull/8347)
- Transaction marker deletion [#8318](https://github.com/apache/pulsar/pull/8318)
- Support produce messages with transaction in batch [#8415](https://github.com/apache/pulsar/pull/8415)
- Register transaction metadata before send or ack messages [#8493](https://github.com/apache/pulsar/pull/8493)
- Expose transaction interface [#8505](https://github.com/apache/pulsar/pull/8505)
- Guarantee transaction metadata handlers connected [#8563](https://github.com/apache/pulsar/pull/8563)
- Add the batch size in transaction ack command [#8659](https://github.com/apache/pulsar/pull/8659)
- Implement the Transaction Log [#8658](https://github.com/apache/pulsar/pull/8658)

#### Topic policy

- Support setting message TTL on topic level [#7738](https://github.com/apache/pulsar/pull/7738)
- Support setting retention on topic level [#7747](https://github.com/apache/pulsar/pull/7747)
- Support setting delayed delivery policy on topic level [#7784](https://github.com/apache/pulsar/pull/7784)
- Support setting max unacked message per subscription on topic level [#7802](https://github.com/apache/pulsar/pull/7802)
- Support setting persistence policie on topic level [#7817](https://github.com/apache/pulsar/pull/7817)
- Support setting max unacked messages per consumer on topic level [#7818](https://github.com/apache/pulsar/pull/7818)
- Support setting deduplication policy on topic level [#7821](https://github.com/apache/pulsar/pull/7821)
- Support setting message dispatch rate on topic level [#7863](https://github.com/apache/pulsar/pull/7863))
- Support setting compaction threshold on topic level [#7881](https://github.com/apache/pulsar/pull/7881)
- Support setting offload policy on topic level [#7883](https://github.com/apache/pulsar/pull/7883)
- Support setting max producers for a topic [#7914](https://github.com/apache/pulsar/pull/7914)
- Support setting max consumers for a topic [#7968](https://github.com/apache/pulsar/pull/7968)
- Support setting publish rate limitation for a topic [#7948](https://github.com/apache/pulsar/pull/7948)
- Support setting inactive topic policy on topic level [#7986](https://github.com/apache/pulsar/pull/7986)
- Support setting subscribe rate for a topic [#7991](https://github.com/apache/pulsar/pull/7991)
- Support setting max consumers per subscription on topic level [#8003](https://github.com/apache/pulsar/pull/8003)
- Support setting subscription dispatch rate on topic level [#8087](https://github.com/apache/pulsar/pull/8087)
- Support setting deduplication snapshot interval on topic level [#8552](https://github.com/apache/pulsar/pull/8552)

#### Broker

- Upgrade BookKeeper version to 4.12.0 [#8447](https://github.com/apache/pulsar/pull/8447)
- Capture the add entry latency of managed-ledger [#4419](https://github.com/apache/pulsar/pull/4419)
- Keep max-concurrent http web-request configurable [#7250](https://github.com/apache/pulsar/pull/7250)
- Perform the unload in background after bundle split [#7387](https://github.com/apache/pulsar/pull/7387)
- Cleanup already deleted namespace topics when remove cluster [#7473](https://github.com/apache/pulsar/pull/7473)
- Support partitioned topics in the Reader [#7518](https://github.com/apache/pulsar/pull/7518)
- Support partitioned topic lookup [#7605](https://github.com/apache/pulsar/pull/7605)
- Make OrderedExecutor threads number configurable [#7765](https://github.com/apache/pulsar/pull/7765)
- Add config to lazily recover cursors when recovering a managed ledger [#7858](https://github.com/apache/pulsar/pull/7858)
- Make BookKeeper throttle configurable [#7901](https://github.com/apache/pulsar/pull/7901)
- Report compacted topic ledger info when calling get internal stats [#7988](https://github.com/apache/pulsar/pull/7988)
- Add broker config to enforce producer to publish encrypted message [#8055](https://github.com/apache/pulsar/pull/8055)
- Expose ensemble placement policy in bookkeeper.conf [#8210](https://github.com/apache/pulsar/pull/8210)
- Support limit topic publish rate at the broker level [#8235](https://github.com/apache/pulsar/pull/8235)
- Support limit the max tenants of the Pulsar cluster [#8261](https://github.com/apache/pulsar/pull/8261)
- Support limit the max namespaces per tenant [#8267](https://github.com/apache/pulsar/pull/8267)
- Support limit max subscriptions per topic [#8289](https://github.com/apache/pulsar/pull/8289)
- Added metrics for topic lookups operations [#8272](https://github.com/apache/pulsar/pull/8272)
- Added REST handler for broker ready probe [#8303](https://github.com/apache/pulsar/pull/8303)
- Configure namespace anti-affinity in local policies [#8349](https://github.com/apache/pulsar/pull/8349)
- Handle hash collision in KeyShared subscription mode [#8396](https://github.com/apache/pulsar/pull/8396)
- Configure maxMsgReplDelayInSeconds for each repl-cluster [#8409](https://github.com/apache/pulsar/pull/8409)
- Support taking de-duplication snapshots based on time [#8474](https://github.com/apache/pulsar/pull/8474)
- Support namespace-level duplication snapshot [#8506](https://github.com/apache/pulsar/pull/8506)
- Expose consumer names after the mark delete position for the Key_Shared subscription [#8545](https://github.com/apache/pulsar/pull/8545)
- Close topics that remain fenced forcefully [#8561](https://github.com/apache/pulsar/pull/8561)

#### Functions

- Separate out FunctionMetadata related helper functions [#7146](https://github.com/apache/pulsar/pull/7146)
- Attach names for all producers/readers in worker service [#7165](https://github.com/apache/pulsar/pull/7165)
- Add support to read compacted topic [#7193](https://github.com/apache/pulsar/pull/7193)
- Re-work Function MetaDataManager to make all metadata writes only by the leader [#7255](https://github.com/apache/pulsar/pull/7255)
- Fix leader/scheduler assignment processing lag problem [#7237](https://github.com/apache/pulsar/pull/7237)
- Set source spec's negativeacktimeout as well as timeout [#7337](https://github.com/apache/pulsar/pull/7337)
- Add an endpoint to check whether function worker service is initialized [#7350](https://github.com/apache/pulsar/pull/7350)
- Functions metadata compaction [#7377](https://github.com/apache/pulsar/pull/7377)
- Implement rebalance mechanism [#7388](https://github.com/apache/pulsar/pull/7388)
- Improve security setting [#7424](https://github.com/apache/pulsar/pull/7424)
- Allow function rebalance to be run periodically [#7449](https://github.com/apache/pulsar/pull/7449)
- Log scheduler stats for Pulsar Functions [#7474](https://github.com/apache/pulsar/pull/7474)
- Add BatchPushSource interface [#7493](https://github.com/apache/pulsar/pull/7493)
- Rejigger contract between LeaderService and rest of components [#7520](https://github.com/apache/pulsar/pull/7520)
- Allow null consume in BatchPushSource [#7573](https://github.com/apache/pulsar/pull/7573)
- Add readiness api for the worker leader [#7601](https://github.com/apache/pulsar/pull/7601)
- Reduce in the leader init time in Pulsar Functions [#7611](https://github.com/apache/pulsar/pull/7611)
- Export Function worker internal stats via Prometheus [#7641](https://github.com/apache/pulsar/pull/7641)
- Allow ability to specify retain key ordering in functions [#7647](https://github.com/apache/pulsar/pull/7647)
- Added ability to specify runtime for localrunner [#7681](https://github.com/apache/pulsar/pull/7681)
- Add additional metrics for Pulsar Function Worker [#7685](https://github.com/apache/pulsar/pull/7685)
- Use available cores for io thread processing [#7689](https://github.com/apache/pulsar/pull/7689)
- Added ability to specify producer config for functions and sources [#7721](https://github.com/apache/pulsar/pull/7721)
- Allow the option to make producers thread local [#7764](https://github.com/apache/pulsar/pull/7764)
- Add ability for BatchPushSource to notify errors asynchronously [#7865](https://github.com/apache/pulsar/pull/7865)
- Allow ability to specify sub position in functions [#7891](https://github.com/apache/pulsar/pull/7891)
- Add hostname to consumer/producer properties in Pulsar Functions [#7897](https://github.com/apache/pulsar/pull/7897)
- Allow specifying state storage url for Source/Sink localrun [#7930](https://github.com/apache/pulsar/pull/7930)
- Enable function worker JVM metrics to be reported via Prometheus [#8097](https://github.com/apache/pulsar/pull/8097)
- Add ability to specify EnvironmentBasedSecretsProvider in LocalRunner [#8098](https://github.com/apache/pulsar/pull/8098)
- Added ability to specify secrets class in localrunner builder [#8127](https://github.com/apache/pulsar/pull/8127)
- Add access to the current message from the function context [#8290](https://github.com/apache/pulsar/pull/8290)
- Enable e2e encryption for Pulsar Function [#8432](https://github.com/apache/pulsar/pull/8432)
- Support key_based batch builder for functions and sources [#8523](https://github.com/apache/pulsar/pull/8523)
- Refactor Context and State API to allow plugging different state store implementations [#8537](https://github.com/apache/pulsar/pull/8537)

#### IO connectors

- [HDFS] Add config to create sub directory from current time [#7771](https://github.com/apache/pulsar/pull/7771)
- [NSQ] Add NSQ Source [#8372](https://github.com/apache/pulsar/pull/8372)

#### Schema

- Add java8 date and time type to primitive schemas [#7874](https://github.com/apache/pulsar/pull/7874)
- Native protobuf schema support [#7874](https://github.com/apache/pulsar/pull/7874)
- Refactor multi-version schema reader [#8464](https://github.com/apache/pulsar/pull/8464)

#### Tiered storage 
- Support Azure BlobStore offload [#8436](https://github.com/apache/pulsar/pull/8436)

#### Clients

- [Java] Support acknowledging a list of messages [#7688](https://github.com/apache/pulsar/pull/7688)
- [Java] Remove UUID generation on sending message [#7705](https://github.com/apache/pulsar/pull/7705)
- [Java] Perform producer compression from IO threads [#7733](https://github.com/apache/pulsar/pull/7733)
- [C++] Allow to configure KeyShared with out of order delivery [#7842](https://github.com/apache/pulsar/pull/7842)
- [Java] Await thread pool termination when closing Pulsar client [#7962](https://github.com/apache/pulsar/pull/7962)
- [Java] Support non-durable subscription for pulsar-client cli [#8100](https://github.com/apache/pulsar/pull/8100)
- [Java] Cancel producer sendtimeout task after creation failure [#8497](https://github.com/apache/pulsar/pull/8497)
- [cgo] Remove CGO client from repo [#8514](https://github.com/apache/pulsar/pull/8514)


#### Admin

- [Pulsar Admin] support config request timeout [#7698](https://github.com/apache/pulsar/pull/7698)
- [Pulsar Admin] Ensure deleting a partitioned-topic on a non existing namespace returns 404  [#7777](https://github.com/apache/pulsar/pull/7777)
- [Pulsar Admin] Added support to force deleting namespace [#7993](https://github.com/apache/pulsar/pull/7993)
- [Pulsar Admin] Allow to get ledger metadata along with topic stats-internal [#8180](https://github.com/apache/pulsar/pull/8180)
- [Pulsar Admin] Support remove namespace level offload policy [#8446](https://github.com/apache/pulsar/pull/8446)
- [Pulsar Admin] Suport get list of bundles under a namespace [#8450](https://github.com/apache/pulsar/pull/8450)
- [Pulsar Admin] Add ability to examine specific message by position relative to earliest or latest message [#8494](https://github.com/apache/pulsar/pull/8494)
- [Pulsar Admin] Add key-shared consumer range to internal topic stats [#8567](https://github.com/apache/pulsar/pull/8567)

#### Fixes

- [Java Client] Fix connection leak [#6524](https://github.com/apache/pulsar/pull/6524)
- [Broker] Prevent redirection of lookup requests from looping [#7200](https://github.com/apache/pulsar/pull/7200)
- [Broker] Ensure that admin operations are gated by super user check [#7226](https://github.com/apache/pulsar/pull/7226)
- [Broker] Fix race condition when delete topic forcelly [#7356](https://github.com/apache/pulsar/pull/7356)
- [Tiered Storage] Fix NPE when offload data to GCS [#7400](https://github.com/apache/pulsar/pull/7400)
- [Function]Fix race condition in which exitFuture in FunctionAssignmentTailer never gets completed even though the tailer thread has exited [#7351](https://github.com/apache/pulsar/pull/7351)
- [Function] Various fixes and optimizations for processing assignments in function worker [#7338](https://github.com/apache/pulsar/pull/7338)
- [Function] Fix deadlock between create function and leader initialization [#7508](https://github.com/apache/pulsar/pull/7508)
- [Pulsar Admin] Fix exceptions being ignored in PulsarAdmin [#7510](https://github.com/apache/pulsar/pull/7510)
- [Broker] Fix the nondurable consumer can not specify the initial position [#7702](https://github.com/apache/pulsar/pull/7702)
- [Broker] Fixed race condition on deleting topic with active readers [#7715](https://github.com/apache/pulsar/pull/7715)
- [Broker] Avoid ConcurrentModificationException of LocalBrokerData [#7729](https://github.com/apache/pulsar/pull/7729)
- [C++ Client] Fix race condition caused by consumer seek and close [#7819](https://github.com/apache/pulsar/pull/7819)
- [Pulsar Proxy] Fix memory leak with debug log-level  [#7963](https://github.com/apache/pulsar/pull/7963)
- [Broker] Double check from zookeeper if availableBrokers is empty for discovery service [#7975](https://github.com/apache/pulsar/pull/7975)
- [Broker] Fix broker-ml bucket stats show high metrics rate [#8218](https://github.com/apache/pulsar/pull/8218)
- [Broker] Fix incorrect configuration for zk-cache expire time  [#8302](https://github.com/apache/pulsar/pull/8302)
- [Function] Fix returned status code for get function state when state does not exist [#8437](https://github.com/apache/pulsar/pull/8437)
- [Broker] Fix the residual of inactive partitioned-topic cleaning [#8442](https://github.com/apache/pulsar/pull/8442)
- [Pulsar Proxy] Fix request.getContentLength() to return 0 if it is less than 0 [#8448](https://github.com/apache/pulsar/pull/8448)
- [Broker] Fix race condition when calling acknowledgementWasProcessed() [#8499](https://github.com/apache/pulsar/pull/8499)
- [Java Client] Fix handling errors for client requests [#8518](https://github.com/apache/pulsar/pull/8518)
- [C++ Client] Fix potential crash caused by AckGroupTracker's timer [#8519](https://github.com/apache/pulsar/pull/8519)

### 2.6.2 &mdash; 2020-11-09 <a id=“2.6.2”></a>

The following lists fixes and enhancements in the 2.6.2 release.

#### Broker

- [Broker] Catch throwable when start pulsar [7221](https://github.com/apache/pulsar/pull/7221)
- [Broker] Protobuf-shaded package can not update version [7228](https://github.com/apache/pulsar/pull/7228)
- [Broker] Check for null arguments in Namespaces Rest API [7247](https://github.com/apache/pulsar/pull/7247)
- [Broker] Handle SubscriptionBusyException in resetCursor api [7335](https://github.com/apache/pulsar/pull/7335)
- [Broker] Converted the namespace bundle unload into async operation [7364](https://github.com/apache/pulsar/pull/7364)
- [Broker] Update Jersey to 2.31 [7515](https://github.com/apache/pulsar/pull/7515)
- [Broker] Stop to dispatch when skip message temporally since Key_Shared consumer stuck on delivery [7553](https://github.com/apache/pulsar/pull/7553)
- [Broker] Fix bug where producer for geo-replication is not closed when topic is unloaded [7735](https://github.com/apache/pulsar/pull/7735)
- [Broker] Make resetting cursor in REST API asynchronous [7744](https://github.com/apache/pulsar/pull/7744)
- [Broker] Reestablish namespace bundle ownership from false negative releasing and false positive acquiring [7773](https://github.com/apache/pulsar/pull/7773)
- [Broker] make pulsar executor pool size configurable [7782](https://github.com/apache/pulsar/pull/7782)
- [Broker] Redirect Get message by id request when broker not serve for the topic [7786](https://github.com/apache/pulsar/pull/7786)
- [Broker] Make zk cache executor thread pool size configurable [7794](https://github.com/apache/pulsar/pull/7794)
- [Broker] Implement toString() method for TopicMessageIdImpl class [7807](https://github.com/apache/pulsar/pull/7807)
- [Broker] Fix pending batchIndexAcks bitSet batchSize in PersistentAcknowledgmentsGroupingTracker [7828](https://github.com/apache/pulsar/pull/7828)
- [Broker] Fix deadlock when adding consumer [7841](https://github.com/apache/pulsar/pull/7841)
- [Broker] Split message ranges by ledger ID and store them in individualDeletedMessages [7861](https://github.com/apache/pulsar/pull/7861)
- [Broker]  Fix pulsar metrics providing wrong information [7905](https://github.com/apache/pulsar/pull/7905)
- [Broker] Don't fail the health check request when trying to delete the previous subscription [7906](https://github.com/apache/pulsar/pull/7906)
- [Broker] Add configuration to set number of channels per bookie [7910](https://github.com/apache/pulsar/pull/7910)
- [Broker] Fix publish buffer of one ServerCnx calculated multi-times when check broker's publish buffer [7926](https://github.com/apache/pulsar/pull/7926)
- [Broker] Add some logging to improve Authentication debugging and Fix typos in code "occured" -> "occurred" [7934](https://github.com/apache/pulsar/pull/7934)
- [Broker] Fix NPE when acknowledge messages at the broker side [7937](https://github.com/apache/pulsar/pull/7937)
- [Broker] Fix the wrong issuer url concatenate [7980](https://github.com/apache/pulsar/pull/7980)
- [Broker] Upgrade the snakeyaml version to 1.26 [7994](https://github.com/apache/pulsar/pull/7994)
- [Broker] Exclude vertx from bookkeeper-http package [7997](https://github.com/apache/pulsar/pull/7997)
- [Broker] Check null point before setting auto read [7999](https://github.com/apache/pulsar/pull/7999)
- [Broker] Fix IndexOutOfBoundsException in the KeyShared subscription when dispatching messages to consumers [8024](https://github.com/apache/pulsar/pull/8024)
- [Broker] Upgrade jetty-util version to 9.4.31 [8035](https://github.com/apache/pulsar/pull/8035)
- [Broker] Add replicated check to checkInactiveSubscriptions [8066](https://github.com/apache/pulsar/pull/8066)
- [Broker] Add get-last-message-id admin for v1 api [8081](https://github.com/apache/pulsar/pull/8081)
- [Broker] Fix client lookup hangs when broker restarts [8101](https://github.com/apache/pulsar/pull/8101)
- [Broker] Should not cache the owner that does not belong to current server [8111](https://github.com/apache/pulsar/pull/8111)
- [Broker] Support to specify multi ipv6 hosts in brokerServiceUrl [8120](https://github.com/apache/pulsar/pull/8120)
- [Broker] Intercept messages to consumers and add intercept exception [8129](https://github.com/apache/pulsar/pull/8129)
- [Broker] Add ChannelFutures utility class to pulsar-common [8137](https://github.com/apache/pulsar/pull/8137)
- [Broker] Support Disable Replicated Subscriptions [8144](https://github.com/apache/pulsar/pull/8144)
- [Broker] Fix error code returned to client when service unit is not ready [8147](https://github.com/apache/pulsar/pull/8147)
- [Broker] Skip intercepting multipart requests [8156](https://github.com/apache/pulsar/pull/8156)
- [Broker] Enable intercept filters only when interceptors are configured [8157](https://github.com/apache/pulsar/pull/8157)
- [Broker] Clean inactive non-persistent subscriptions [8166](https://github.com/apache/pulsar/pull/8166)
- [Broker] Add a new state for namespace-level TTL [8178](https://github.com/apache/pulsar/pull/8178)
- [Broker] Fix peek messages failed with subscriptionName not exist [8182](https://github.com/apache/pulsar/pull/8182)
- [Broker] Fix pulsar service close exception [8197](https://github.com/apache/pulsar/pull/8197)
- [Broker] Use ThreadPoolExecutor instead of EventLoop [8208](https://github.com/apache/pulsar/pull/8208)
- [Broker] Close ZK connections at end of metadata setup [8228](https://github.com/apache/pulsar/pull/8228)
- [Broker] Delete associated ledgers before deleting cluster metadata [8244](https://github.com/apache/pulsar/pull/8244)
- [Broker] Fix stuck lookup operations when the broker is starting up [8273](https://github.com/apache/pulsar/pull/8273)
- [Broker] Fix Broker enters an infinite loop in ManagedLedgerImpl.asyncReadEntries [8284](https://github.com/apache/pulsar/pull/8284)
- [Broker] Fix message TTL on Key_Shared subscription and Fix ordering issue when replay messages [8292](https://github.com/apache/pulsar/pull/8292)
- [Broker] Fix race condition in updating readPosition in ManagedCursorImpl [8299](https://github.com/apache/pulsar/pull/8299)
- [Broker] Refresh ZooKeeper-data cache in background to avoid deadlock and blocking IO on ZK thread [8304](https://github.com/apache/pulsar/pull/8304)
- [Broker] Upgrade hdfs2 version to 2.8.5 [8319](https://github.com/apache/pulsar/pull/8319)
- [Broker] Upgrade solr version to 8.6.3 [8328](https://github.com/apache/pulsar/pull/8328)
- [Broker] Fix deadlock that occurred during topic ownership check [8406](https://github.com/apache/pulsar/pull/8406)

#### Proxy

- [Proxy] Add advertisedAddress config field to ProxyConfiguration [7542](https://github.com/apache/pulsar/pull/7542) 
- [Proxy] Fix deadlock in pulsar proxy [7690](https://github.com/apache/pulsar/pull/7690)
- [Proxy] Handle NPE while updating proxy stats [7766](https://github.com/apache/pulsar/pull/7766)
- [Proxy] Fix the null exception when starting the proxy service [8019](https://github.com/apache/pulsar/pull/8019)
- [Proxy] Add proxy plugin interface to support user defined additional servlet [8067](https://github.com/apache/pulsar/pull/8067)

#### Pulsar SQL

- [Pulsar SQL] Upgrade Presto version to 332 [7194](https://github.com/apache/pulsar/pull/7194)
- [Pulsar SQL] Replace com.ning.asynchttpclient with org.asynchttpclient [8099](https://github.com/apache/pulsar/pull/8099)

#### Java Client

- [Java Client] Support input-stream for trustStore cert [7442](https://github.com/apache/pulsar/pull/7442)
- [Java Client] Avoid subscribing the same topic again [7823](https://github.com/apache/pulsar/pull/7823)
- [java Client] Add autoPartitionsUpdateInterval for producer and consumer [7840](https://github.com/apache/pulsar/pull/7840)
- [Java Client] Avoid resolving address for sni-host + thread-safe connection creation [8062](https://github.com/apache/pulsar/pull/8062)
- [Java Client] Websocket interface decode URL encoding [8072](https://github.com/apache/pulsar/pull/8072)
- [Java Client] Always use SNI for TLS enabled Pulsar Java broker client [8117](https://github.com/apache/pulsar/pull/8117)
- [Java Client] Improve timeout handling in ClientCnx to cover all remaining request types (GetLastMessageId, GetTopics, GetSchema, GetOrCreateSchema) [8149](https://github.com/apache/pulsar/pull/8149)
- [Java Client] Fix ConsumerImpl memory leaks [8160](https://github.com/apache/pulsar/pull/8160)
- [Java Client] Fix issue where paused consumer receives new message when reconnecting [8165](https://github.com/apache/pulsar/pull/8165)
- [Java Client] Improve refactored client connection code [8177](https://github.com/apache/pulsar/pull/8177)
- [Java Client] Add log level configuration in pulsar-client [8195](https://github.com/apache/pulsar/pull/8195)
- [Java Client] Remove unnecessary locks [8207](https://github.com/apache/pulsar/pull/8207)
- [Java Client] Fix AutoUpdatePartitionsInterval setting problem [8227](https://github.com/apache/pulsar/pull/8227)
- [Java Client] Add read position when joining in the consumer stats [8274](https://github.com/apache/pulsar/pull/8274)
- [Java Client] Support reset cursor to a batch index of the batching message [8285](https://github.com/apache/pulsar/pull/8285)
- [Java Client] Support exclude the message when reset cursor by message ID [8306](https://github.com/apache/pulsar/pull/8306)
- [Java Client] Increasing timeout for pulsar client io threads to shutdown [8316](https://github.com/apache/pulsar/pull/8316)
- [Java Client] Support cancelling message & batch futures returned from Reader & Consumer [8326](https://github.com/apache/pulsar/pull/8326)
- [Java Client] Disable batch receive timer for Readers [8381](https://github.com/apache/pulsar/pull/8381)
- [Java Client] Fix pause does not work for new created consumer  [8387](https://github.com/apache/pulsar/pull/8387)

#### CPP Client

- [CPP Client] Wait for all seek operations completed [7216](https://github.com/apache/pulsar/pull/7216)
- [CPP Client] Ensure parallel invocations of MultiTopicsConsumerImpl::subscribeAsync with the same topic name do not produce an error. [7691](https://github.com/apache/pulsar/pull/7691)
- [CPP Client] Throw std::exception types [7798](https://github.com/apache/pulsar/pull/7798)
- [CPP Client] Make clear() thread-safe [7862](https://github.com/apache/pulsar/pull/7862)
- [CPP Client] Support key based batching [7996](https://github.com/apache/pulsar/pull/7996)
- [CPP Client] The token endpoint should get from the well-known configuration [8006](https://github.com/apache/pulsar/pull/8006)
- [CPP Client] Add Snappy library to Docker images for building C++ packages [8086](https://github.com/apache/pulsar/pull/8086)
- [CPP Client] Add epoch for C++ client HandleBase to handle create producer timeout [8191](https://github.com/apache/pulsar/pull/8191)
- [CPP Client] Fix some pending requests may never complete when broker's down [8232](https://github.com/apache/pulsar/pull/8232)
- [CPP Client] Client is allocating buffer bigger than needed [8283](https://github.com/apache/pulsar/pull/8283)
- [CPP Client] Client back-pressure is done on batches rather than number of messages [8331](https://github.com/apache/pulsar/pull/8331)
- [CPP Client] Fix message id error when subscribing a single partition [8341](https://github.com/apache/pulsar/pull/8341)

#### Python Client

- [Python Client] Add python schema field default value [8122](https://github.com/apache/pulsar/pull/8122)
- [Python Client] Expose schema version (of writerSchema) in Message [8173](https://github.com/apache/pulsar/pull/8173)

#### Pulsar Functions

- [Pulsar Functions] During Function update, cleanup should only happen for temp files that were generated [7201](https://github.com/apache/pulsar/pull/7201)
- [Pulsar Functions] Have metadata tailer use its own thread for processing [7211](https://github.com/apache/pulsar/pull/7211)
- [Pulsar Functions] Allow kubernetes runtime to customize function instance class path [7844](https://github.com/apache/pulsar/pull/7844)
- [Pulsar Functions] SinkRecord adds an overridden method  [8038](https://github.com/apache/pulsar/pull/8038)
- [Pulsar Functions] Set dryrun of KubernetesRuntime is null [8064](https://github.com/apache/pulsar/pull/8064)
- [Pulsar Functions] Allow disabling forwarding source message properties [8158](https://github.com/apache/pulsar/pull/8158)
- [Pulsar Functions] Missed dryRun on maintenance of secrets [8286](https://github.com/apache/pulsar/pull/8286)

#### Pulsar Perf

- [Pulsar Perf] Support setting message key [7989](https://github.com/apache/pulsar/pull/7989)
- [Pulsar Perf] Make pulsar-perf ioThread number configurable [8090](https://github.com/apache/pulsar/pull/8090)

#### Pulsar Admin

- [Pulsar Admin] Support initial namespace of the cluster without startup the broker [7434](https://github.com/apache/pulsar/pull/7434)
- [Pulsar Admin] Fix some params on consumer broken by #4400 (regex, initialSouscriptionPosition) [7795](https://github.com/apache/pulsar/pull/7795)
- [Pulsar Admin] Return more informative error message when trying to create subscription on non-persistent through Rest API or pulsar-admin CLI [7831](https://github.com/apache/pulsar/pull/7831)
- [Pulsar Admin] Add cli command to get last message Id [8082](https://github.com/apache/pulsar/pull/8082)
- [Pulsar Admin] Support delete all data associated with a cluster [8133](https://github.com/apache/pulsar/pull/8133)
- [Pulsar Admin] Support delete schema ledgers when delete topics [8167](https://github.com/apache/pulsar/pull/8167)
- [Pulsar Admin] Add command to delete a cluster's metadata from ZK [8169](https://github.com/apache/pulsar/pull/8169)
- [Pulsar Admin] Support reset cursor to a batch index for Pulsar Admin [8329](https://github.com/apache/pulsar/pull/8329)

#### Tiered Storage

- [Tiered Storage] Refactored JCloud Tiered Storage [6335](https://github.com/apache/pulsar/pull/6335)
- [Tiered Storage] Remove duplicate updates [8198](https://github.com/apache/pulsar/pull/8198)
- [Tiered Storage] Make the field name in `OffloadPolicies` match with config file [8310](https://github.com/apache/pulsar/pull/8310)

### 2.6.1 &mdash; 2020-08-21 <a id=“2.6.1”></a>

The following lists fixes and enhancements in 2.6.1 release.

#### Broker

- [Broker] Limit batch size to the minimum of the `maxNumberOfMessages` and `maxSizeOfMessages` [#6865](https://github.com/apache/pulsar/pull/6865)
- [Broker] Fix hash range conflict issue in Key_Shared with sticky hash range [#7231](https://github.com/apache/pulsar/pull/7231)
- [Broker] Fix the issue that get lookup permission error [#7234](https://github.com/apache/pulsar/pull/7234)
- [Broker] Update Jetty to version 9.4.29 [#7235](https://github.com/apache/pulsar/pull/7235)
- [Broker] Fix readers backlog stats after data is skipped [#7236](https://github.com/apache/pulsar/pull/7236)
- [Broker] Fix the regression in `isSupperUser` [#7241](https://github.com/apache/pulsar/pull/7241)
- [Broker] Avoid introducing null read position for the managed cursor [#7264](https://github.com/apache/pulsar/pull/7264)
- [Broker] Fix permission operation check on setRetention admin operation [#7281](https://github.com/apache/pulsar/pull/7281)
- [Broker] Fix error in creation of non-durable cursor [#7355](https://github.com/apache/pulsar/pull/7355)
- [Broker] Fix bug related to managedLedger properties [#7357](https://github.com/apache/pulsar/pull/7357)
- [Broker] Add tenant name check in list namespaces function [#7369](https://github.com/apache/pulsar/pull/7369)
- [Broker] Avoid the NPE occurs in method `ManagedLedgerImpl.isOffloadedNeedsDelete` [#7389](https://github.com/apache/pulsar/pull/7389)
- [Broker] Fix producer stuck issue due to NPE thrown when creating a new ledger [#7401](https://github.com/apache/pulsar/pull/7401)
- [Broker] Avoid NPEs at ledger creation when DNS failures happen [#7403](https://github.com/apache/pulsar/pull/7403)
- [Broker] Support decompression payload if needed in KeyShared subscription [#7416](https://github.com/apache/pulsar/pull/7416)
- [Broker] Fix update-cluster cli updates proxy-url [#7422](https://github.com/apache/pulsar/pull/7422)
- [Broker] Handle `NotAllowed Exception` at the client side [#7430](https://github.com/apache/pulsar/pull/7430)
- [Broker] Shade jclouds to avoid Gson conflict [#7435](https://github.com/apache/pulsar/pull/7435)
- [Broker] Consumer is registered on dispatcher even if hash range conflicts on Key_Shared subscription [#7444](https://github.com/apache/pulsar/pull/7444)
- [Broker] Add pulsar-client-messagecrypto-bc into pulsar-client dependency to avoid method not found [#7447](https://github.com/apache/pulsar/pull/7447)
- [Broker] Fix update partitions error for non-persistent topic [#7459](https://github.com/apache/pulsar/pull/7459)
- [Broker] Use CGroup CPU usage when present [#7475](https://github.com/apache/pulsar/pull/7475)
- [Broker] Fix ArrayIndexOutOfBoundsException when dispatch messages to consumer [#7483](https://github.com/apache/pulsar/pull/7483)
- [Broker] Get last entry is trying to read entry -1 [#7495](https://github.com/apache/pulsar/pull/7495)
- [Broker] Fix timeout opening managed ledger operation [#7506](https://github.com/apache/pulsar/pull/7506)
- [Broker] Fixes the exception that occurred when the geo-replication policy is updated [#7514](https://github.com/apache/pulsar/pull/7514)
- [Broker] Update Jackson to version 2.11.1 and ensure all dependencies are pinned [#7519](https://github.com/apache/pulsar/pull/7519)
- [Broker] Fix protobuf generation on handling repeated long number [#7540](https://github.com/apache/pulsar/pull/7540)
- [Broker] Add more logging to the auth operations on failure [#7567](https://github.com/apache/pulsar/pull/7567)
- [Broker] Use Consume/Produce/Lookup interfaces for specific operations in allowTopicOperation [#7587](https://github.com/apache/pulsar/pull/7587)
- [Broker] Support configuring `DeleteInactiveTopic` setting in namespace policy [#7598](https://github.com/apache/pulsar/pull/7598)
- [Broker] Fix NPE when using advertisedListeners [#7620](https://github.com/apache/pulsar/pull/7620)
- [Broker] Fix the issue that deduplication cursor can not be deleted after disabling message deduplication [#7656](https://github.com/apache/pulsar/pull/7656)
- [Broker] Add missing AuthenticationDataSource to canConsumeAsync method call [#7694](https://github.com/apache/pulsar/pull/7694)
- [Broker] Close the previous reader of the health check topic [#7724](https://github.com/apache/pulsar/pull/7724)
- [Broker] Change some WebApplicationException log level to debug [#7725](https://github.com/apache/pulsar/pull/7725)
- [Broker] Replay delayed messages in order [#7731](https://github.com/apache/pulsar/pull/7731)
- [Broker] Fix the wrong returned URL for lookup when specify advertised listener [#7737](https://github.com/apache/pulsar/pull/7737)
- [Broker] Fix topic getting recreated immediately after deletion [#7524](https://github.com/apache/pulsar/pull/7524)
- [Broker] Set default root log level to debug [#7789](https://github.com/apache/pulsar/pull/7789)
- [Broker] Fix producer stucks on creating ledger timeout [#7319](https://github.com/apache/pulsar/pull/7319)
- [Broker] AllowTopicOperationAsync should check the original role is super user [#7788](https://github.com/apache/pulsar/pull/7788)

#### Zookeeper

- [Zookeeper] Use hostname for bookie rackawareness mapping [#7361](https://github.com/apache/pulsar/pull/7361)

#### Pulsar SQL

- [Pulsar SQL] Make Pulsar SQL get correct offload configurations [#7701](https://github.com/apache/pulsar/pull/7701)

#### Pulsar Schema
- [Schema] Fix the error that occurs when getting schemaName by partitioned topic name [#7708](https://github.com/apache/pulsar/pull/7708)

#### Java Client

- [Java Client] Fix the issue that the HTTP header used in Athenz authentication can not be renamed [#7311](https://github.com/apache/pulsar/pull/7311)
- [Java Client] Add more detail information of retry errors [#7341](https://github.com/apache/pulsar/pull/7341)
- [Java Client] Check NPE when a tombstone (null value) is produced. [#7408](https://github.com/apache/pulsar/pull/7408)
- [Java Client] Fix batch ackset recycled multiple times. [#7409](https://github.com/apache/pulsar/pull/7409)
- [Java Client] Support Oauth2 authentication [#7420](https://github.com/apache/pulsar/pull/7420)
- [Java Client] Ensure the create subscription can be completed when the operation timeout happens [#7522](https://github.com/apache/pulsar/pull/7522)
- [Java Client] Fix race condition on the close consumer while reconnecting to the broker. [#7589](https://github.com/apache/pulsar/pull/7589)
- [Java Client] Fix validation never return false [#7593](https://github.com/apache/pulsar/pull/7593)
- [Java Client] Make OAuth2 auth plugin to use AsyncHttpClient [#7615](https://github.com/apache/pulsar/pull/7615)
- [Java Client] Support to set listener name for client CLI [#7621](https://github.com/apache/pulsar/pull/7621)
- [Java Client] Fix batch index filter issue in Consumer [#7654](https://github.com/apache/pulsar/pull/7654)
- [Java Client] Fix the backward compatibility issues with batch index acknowledgment. [#7655](https://github.com/apache/pulsar/pull/7655)
- [Java Client] Fix the issue that batchReceiveAsync is not completed exceptionally when closing consumer [#7661](https://github.com/apache/pulsar/pull/7661)
- [Java Client] Fix producer stats recorder time unit error [#7670](https://github.com/apache/pulsar/pull/7670)
- [Java Client] Fix shutdown AsyncHttpConnector.delayer [#7687](https://github.com/apache/pulsar/pull/7687)

#### CPP Client

- [CPP Client] Fix partition index error in closing callback [#7282](https://github.com/apache/pulsar/pull/7282)
- [CPP Client] Reduce log level for ack-grouping tracker [#7373](https://github.com/apache/pulsar/pull/7373)
- [CPP Client] Support Oauth2 authentication [#7467](https://github.com/apache/pulsar/pull/7467)
- [CPP Client] Fix segment crashes that caused by race condition of timer in cpp client [#7572](https://github.com/apache/pulsar/pull/7572)
- [CPP Client] Fix multitopic consumer segfault on connect error [#7588](https://github.com/apache/pulsar/pull/7588)
- [CPP Client] Add support to read credentials from file [#7606](https://github.com/apache/pulsar/pull/7606)
- [CPP Client] Fix the issuer_url parsing failure in oauth2 [#7791](https://github.com/apache/pulsar/pull/7791)
- [CPP Client] Fix reference leak when reader create [#7793](https://github.com/apache/pulsar/pull/7793)

#### Pulsar Functions

- [Pulsar Function] Use fully qualified hostname as default to advertise worker [#7360](https://github.com/apache/pulsar/pull/7360)
- [Pulsar Function] Fix the function BC issue introduced in release 2.6.0 [#7528](https://github.com/apache/pulsar/pull/7528)
- [Pulsar Function] Improve security setting of Pulsar Functions [#7578](https://github.com/apache/pulsar/pull/7578)
- [Pulsar Function] Differentiate authorization between source/sink/function operations [#7466](https://github.com/apache/pulsar/pull/7466)

#### Go Function

- [Go Function] Fix Go instance config port [#7322](https://github.com/apache/pulsar/pull/7322)
- [Go Function] Remove timestamp from metrics [#7539](https://github.com/apache/pulsar/pull/7539)

#### Pulsar Perf

- [Pulsar Perf] Supports `tlsAllowInsecureConnection` in pulsar-perf produce/consume/read [#7300](https://github.com/apache/pulsar/pull/7300)

### 2.6.0 &mdash; 2020-06-17 <a id=“2.6.0”></a>

#### Features

##### PIPs

* [PIP-37] Large message size support [#4400](https://github.com/apache/pulsar/pull/4400)
* [PIP-39] Namespace change events (System Topic) [#4955](https://github.com/apache/pulsar/pull/4955)
* [PIP-45] Switch ManagedLedger to use MetadataStore interface [#5358](https://github.com/apache/pulsar/pull/5358)
* [PIP 53] Contribute [DotPulsar](https://github.com/apache/pulsar-dotpulsar) to Apache Pulsar
* [PIP-54] Support acknowledgment at batch index level [#6052](https://github.com/apache/pulsar/pull/6052)
* [PIP-58] Support consumers set custom message retry delay [#6449](https://github.com/apache/pulsar/pull/6449)
* [PIP-60] Support SNI routing to support various proxy-server [#6566](https://github.com/apache/pulsar/pull/6566)
* [PIP-61] Advertise multiple addresses [#6903](https://github.com/apache/pulsar/pull/6903)
* [PIP-65] Adapting Pulsar IO Sources to support Batch Sources [#7090](https://github.com/apache/pulsar/pull/7090)

##### Broker 

* [Broker] Add threshold shedder strategy and metrics exporter for loadbalancer [#6772](https://github.com/apache/pulsar/pull/6772)
* [Broker] Add consistent hashing in the Key_Shared distribution [#6791](https://github.com/apache/pulsar/pull/6791)
* [Broker] Fixed ordering issue in KeyShared subscription dispatcher when adding consumer [#7106](https://github.com/apache/pulsar/pull/7106) [#7108](https://github.com/apache/pulsar/pull/7108) [#7188](https://github.com/apache/pulsar/pull/7188)
* [Broker] Add support for key hash range reading in Key_Shared subscription [#5928](https://github.com/apache/pulsar/pull/5928)
* [Broker] Allow for schema reader and writer registration on SchemaDefinition [#6905](https://github.com/apache/pulsar/pull/6905)
* [Broker] Support use null key and null value in KeyValue Schema [#7139](https://github.com/apache/pulsar/pull/7139)
* [Broker] Support multiple pulsar clusters to use the same bk cluster [#5985](https://github.com/apache/pulsar/pull/5985)
* [Broker] Add a flag to skip broker shutdown on transient OOM [#6634](https://github.com/apache/pulsar/pull/6634)
* [Broker] Make zookeeper cache expiry time configurable [#6668](https://github.com/apache/pulsar/pull/6668)
* [Broker] Check replicator periodically to avoid issue due to zookeeper missing watch [#6674](https://github.com/apache/pulsar/pull/6674)
* [Broker] Expose managedLedgerCache, managedLedger, loadBalance metrics to Prometheus [#6705](https://github.com/apache/pulsar/pull/6705)
* [Broker] Optimize consumer fetch messages in case of batch message [#6719](https://github.com/apache/pulsar/pull/6719)
* [Broker] Add configuration to limit max partitions for a partitioned topic [#6794](https://github.com/apache/pulsar/pull/6794)
* [Broker] Change default FlushEntryLogBytes to 256MB to improve bookie io throughput [#6915](https://github.com/apache/pulsar/pull/6915)
* [Broker] Introduce precise topic publish rate limiting [#7078](https://github.com/apache/pulsar/pull/7078)
* [Broker] Expose new entries check delay in the broker.conf [7154](https://github.com/apache/pulsar/pull/7154)
* [Broker] Add broker interceptor for intercepting all Pulsar command and REST API requests [#7143](https://github.com/apache/pulsar/pull/7143)
* [Broker] Only close active consumer for Failover subscription when seek() [#7141](https://github.com/apache/pulsar/pull/7141)
* [Broker] Allow to delete topics that are failing to recover [#7131](https://github.com/apache/pulsar/pull/7131)
* [Broker] Support set netty max frame size in bookkeeper.conf [#7116](https://github.com/apache/pulsar/pull/7116)
* [Broker] Trigger rollover when meeting maxLedgerRolloverTimeMinutes [#7111](https://github.com/apache/pulsar/pull/7111)
* [Broker] Cap the dispatcher batch size in bytes to fixed max [#7097](https://github.com/apache/pulsar/pull/7097)
* [Broker] Support specify managedLedgerMaxSizePerLedgerMbytes in broker.conf [#7085](https://github.com/apache/pulsar/pull/7085)
* [Broker] Allow to grant permissions when the authorization is disabled [#7074](https://github.com/apache/pulsar/pull/7074)
* [Broker] Add messages and bytes counter stats to broker-stats/topics [#7045](https://github.com/apache/pulsar/pull/7045)
* [Broker] Expose new entries check delay in the broker.conf [#7154](https://github.com/apache/pulsar/pull/7154)

##### Function

* [Function] Built-in functions support [#6895](https://github.com/apache/pulsar/pull/6895)
* [Function] Add Go Function heartbeat (and gRPC service) for production usage [#6031](https://github.com/apache/pulsar/pull/6031)
* [Function] Add custom property option to functions [#6348](https://github.com/apache/pulsar/pull/6348)
* [Function] Separate TLS configuration of function worker and broker [#6602](https://github.com/apache/pulsar/pull/6602)
* [Function] Added ability to build consumers in functions and sources [#6954](https://github.com/apache/pulsar/pull/6954)
* [Function] Support DLQ on sources and sinks [#7032](https://github.com/apache/pulsar/pull/7032)

##### Pulsar SQL

* [SQL] KeyValue schema support [#6325](https://github.com/apache/pulsar/pull/6325)
* [SQL] Multiple version schema support [#4847](https://github.com/apache/pulsar/pull/4847)
* [SQL] Fix presto SQL does not start metrics service before queue execute [#7030](https://github.com/apache/pulsar/pull/7030)

##### Pulsar IO

* Added ability for sources to publish messages on their own [#6941](https://github.com/apache/pulsar/pull/6941)
* [RabbitMQ] Allow routing key per message to RabbitMQ sink connector [#5890](https://github.com/apache/pulsar/pull/5890)
* [RabbitMQ] Add passive config options [#6679](https://github.com/apache/pulsar/pull/6679)
* [debezium] Upgrade from v0.10.0-Final to v1.0.0-Final [#5972](https://github.com/apache/pulsar/pull/5972)
* [debezium] Support avro schema for debezium connector [#6034](https://github.com/apache/pulsar/pull/6034)
* [influxdb2]  Add support for influxdb2 in pulsar-influxdb-sink [#6601](https://github.com/apache/pulsar/pull/6601)
* [jdbc] Add jdbc sinks: postgres, mariadb, clickhouse [#6835](https://github.com/apache/pulsar/pull/6835)

##### Pulsar Proxy

* [Proxy] Add REST API to get connection and topic stats [#6473](https://github.com/apache/pulsar/pull/6473)
* [Proxy] Add advertised address option [#6942](https://github.com/apache/pulsar/pull/6942)
* [Proxy] Add proxyLogLevel into config [#6948](https://github.com/apache/pulsar/pull/6948)

##### Clients

* [Java Client] Use pure-java Air-Compressor instead of JNI based libraries [#5390](https://github.com/apache/pulsar/pull/5390)
* [Java Client] Change the time unit of patternAutoDiscoveryPeriod to seconds [#5950](https://github.com/apache/pulsar/pull/5950)
* [Java Client] Support waiting for inflight messages while closing producer [#6648](https://github.com/apache/pulsar/pull/6648)
* [Java Client] Add support to load TLS certs/key dynamically from input stream [#6760](https://github.com/apache/pulsar/pull/6760)
* [Java Client] Support return sequence ID when throwing Exception for async send message [#6825](https://github.com/apache/pulsar/pull/6825)
* [Java Client] Change the default value of maxLookupRedirects of Java client to 20 [#7126](https://github.com/apache/pulsar/pull/7126)
* [Java Client] Limit the number of times lookup requests are redirected [#7096](https://github.com/apache/pulsar/pull/7096)
* [CPP Client] Support seek by time on partitioned topic [#7198](https://github.com/apache/pulsar/pull/7198)
* [CPP Client] Refresh authentication credentials [#7070](https://github.com/apache/pulsar/pull/7070)
* [CPP Client] Fix Result can't be serialized to string inside the library [#7034](https://github.com/apache/pulsar/pull/7034)
* [CPP Client] Support zstd and Snappy compression to C API [#7014](https://github.com/apache/pulsar/pull/7014)
* [Python Client] Add deliver_at and deliver_after for the producer [#6737](https://github.com/apache/pulsar/pull/6737) 

##### Admin

* [Admin] Support delete inactive topic when subscriptions caught up [#6077](https://github.com/apache/pulsar/pull/6077)
* [Admin] Add configuration to disable auto-creation of subscriptions [#6456](https://github.com/apache/pulsar/pull/6456)
* [Admin] Add maxUnackedMessagesPerSubscription and maxUnackedMessagesPerConsumer on namespaces policies [#5936](https://github.com/apache/pulsar/pull/5936)
* [Admin] Support get a message by message ID in pulsar-admin [#6331](https://github.com/apache/pulsar/pull/6331)
* [Admin] Support delete subscription forcefully [#6383](https://github.com/apache/pulsar/pull/6383)
* [Admin] Add subscribe initial position for consumer CLI [#6442](https://github.com/apache/pulsar/pull/6442)
* [Admin] Support to get managed ledger info of a partitioned topic [#6532](https://github.com/apache/pulsar/pull/6532)
* [Admin] Support compact all partitions of a partitioned topic [#6537](https://github.com/apache/pulsar/pull/6537)
* [Admin] Support multi-hosts in PulsarAdmin [#6547](https://github.com/apache/pulsar/pull/6547)
* [Admin] Support to get internal stats for a partitioned topic [#6624](https://github.com/apache/pulsar/pull/6624)
* [Admin] Support enable or disable subscription auto-creation at namespace level [#6637](https://github.com/apache/pulsar/pull/6637)
* [Admin] Enable to set the subscription expiration time for each namespace [#6851](https://github.com/apache/pulsar/pull/6851)

#### Fixes

* [Broker] Fixed increasing number of partitions with attached readers [#7077](https://github.com/apache/pulsar/pull/7077)
* [Broker] Make ZkBookieRackAffinityMapping work as expected [#6917](https://github.com/apache/pulsar/pull/6917)
* [Broker] Fix backlog and backlog size stats keeps growing [#7082](https://github.com/apache/pulsar/pull/7082)
* [Java Client] Fix connection leak [#6524](https://github.com/apache/pulsar/pull/6524)
* [Java Client] Fix message id compare between MessageId and BatchMessageId [#6621](https://github.com/apache/pulsar/pull/6621)
* [Java Client] Fix memory leak when create producer with not exsits topic [#7120](https://github.com/apache/pulsar/pull/7120) [#7124](https://github.com/apache/pulsar/pull/7124)
* [Java Client] Fix duplicated messages sent to dead letter topic [#7021](https://github.com/apache/pulsar/pull/7021)
* [CPP Client] Fix deadlock of consumer for topics auto discovery [#7206](https://github.com/apache/pulsar/pull/7206)
* [Managed Ledger] Fix NPE on opening non-durable cursors on an empty managed ledger [#7133](https://github.com/apache/pulsar/pull/7133)
* [Websocket] Fix incorrect topic URL parse [#6630](https://github.com/apache/pulsar/pull/6630)
* [Pulsar SQL] Fix problem with multiple zookeeper address [#6947](https://github.com/apache/pulsar/pull/6947)
* [Docker] Do not apply env values to pulsar_env.sh and bkenv.sh implicitly [6579](https://github.com/apache/pulsar/pull/6579)

### 2.5.2 &mdash; 2020-05-19 <a id=“2.5.2”></a>

#### Fixes and Enhancements

##### Broker
* [Broker] Implement AutoTopicCreation by namespace level override [#6471](https://github.com/apache/pulsar/pull/6471)
* [Broker] Add custom deletionLag and threshold for offload policies per namespace  [#6422](https://github.com/apache/pulsar/pull/6422)
* [Broker] Invalidate managed ledgers zookeeper cache instead of reloading on watcher triggered [#6659](https://github.com/apache/pulsar/pull/6659) 
* [Broker] Retention policy should be respected when there is no traffic [#6676](https://github.com/apache/pulsar/pull/6676)
* [Broker] Fixed double delete on a namespace [#6713](https://github.com/apache/pulsar/pull/6713)
* [Broker] fix get batch message from http response, only get the first message[#6715](https://github.com/apache/pulsar/pull/6715)
* [Broker] Fix Deadlock by Consumer and Reader[#6728](https://github.com/apache/pulsar/pull/6728)
* [Broker] avoid backpressure by skipping dispatching if consumer channel is not writable [#6740](https://github.com/apache/pulsar/pull/6740)
* [Broker] fix when producing encrypted messages, MessageMetadata objects are not released after they are created. [#6745](https://github.com/apache/pulsar/pull/6745)
* [Broker] Bump netty version to 4.1.48.Final [#6746](https://github.com/apache/pulsar/pull/6746)
* [Broker] Increase timeout for loading topics [#6750](https://github.com/apache/pulsar/pull/6750)
* [Broker] Fix wrong cursor state for cursor without consumer  [#6766](https://github.com/apache/pulsar/pull/6766)
* [Broker] change nondurable cursor to active to improve performance [#6769](https://github.com/apache/pulsar/pull/6769)
* [Broker] register loadbalance znode should attempt to wait until session expired [#6788](https://github.com/apache/pulsar/pull/6788)
* [Broker] Fix some empty message related problems in the compacted topic. [#6795](https://github.com/apache/pulsar/pull/6795)
* [Broker] Avoid creating partitioned topic for partition name [#6846](https://github.com/apache/pulsar/pull/6846)
* [Broker] Add Tls with keystore type config support [#6853](https://github.com/apache/pulsar/pull/6853)
* [Broker] fix consumer stuck when batchReceivePolicy maxNumMessages > maxReceiverQueueSize [#6862](https://github.com/apache/pulsar/pull/6862)
* [Broker] use originalAuthMethod on originalAuthChecker in Proxy Authentication [#6870](https://github.com/apache/pulsar/pull/6870)
* [Broker] Close producer when the topic does not exists. [#6879](https://github.com/apache/pulsar/pull/6879)
* [Broker] Handle all exceptions from `topic.addProducer` [#6881](https://github.com/apache/pulsar/pull/6881)
* [Broker] fix topicPublishRateLimiter not effective after restart broker [#6893](https://github.com/apache/pulsar/pull/6893)
* [Broker] Expose pulsar_out_bytes_total and pulsar_out_messages_total for namespace/subscription/consumer. [#6918](https://github.com/apache/pulsar/pull/6918)
* [Broker] Policy ttlDurationDefaultInSeconds not applying  [#6920](https://github.com/apache/pulsar/pull/6920)
* [Broker] Fix pulsar admin thread number explode bug. [#6940](https://github.com/apache/pulsar/pull/6940)

##### Pulsar Schema

* [Schema] Fix long field parse in GenricJsonRecord [#6622](https://github.com/apache/pulsar/pull/6622) 
* [Schema] Fix the leak of cursor reset if message encode fails in Avro schema. [#6695](https://github.com/apache/pulsar/pull/6695) 
* [Schema] fix Get schema by version can get the deleted schema info #6754 [#6754](https://github.com/apache/pulsar/pull/6754)
* [Schema] Fix serialization of enums with json/avro schemas in python [#6808](https://github.com/apache/pulsar/pull/6808) 
* [Schema] Pulsar SQL Support Avro Schema `ByteBuffer` Type [#6925](https://github.com/apache/pulsar/pull/6925) 

##### CPP Client
* [CPP Client] Auto update topic partitions [#6732](https://github.com/apache/pulsar/pull/6732)
* [CPP Client] Subscription InitialPosition is not correctly set on regex consumers. [#6810](https://github.com/apache/pulsar/pull/6810)
* [CPP Client] Fix message id is always the default value in send callback [#6812](https://github.com/apache/pulsar/pull/6812)
* [CPP Client] Fix message id error if messages were sent to a partitioned topic [#6938](https://github.com/apache/pulsar/pull/6938)

##### Python Client
* [Python Client] Fix Python function protobuf missing field[#6641](https://github.com/apache/pulsar/pull/6641)

##### Pulsar Functions
* [Functions] Support function with format: Function<I, CompletableFuture<O>>[#6684](https://github.com/apache/pulsar/pull/6684)
* [Functions] Function endpoint admin/v3/functions/{tenant}/{namespace} always returns 404 [#6767](https://github.com/apache/pulsar/pull/6767)
* [Functions] Ensure that all dangling consumers are cleaned up during failures [#6778](https://github.com/apache/pulsar/pull/6778)
* [Functions] Fix localrunner netty dependency issue [#6779](https://github.com/apache/pulsar/pull/6779)
* [Functions] Fix SerDe validation of function's update [#6888](https://github.com/apache/pulsar/pull/6888)

##### Tiered Storage
* [Tiered Storage]  Extract common SerDe method in tiered storage to managed-ledger module [#6533](https://github.com/apache/pulsar/pull/6533)
* [Tiered Storage]  Make SchemaStorage accessible in Offloader [#6567](https://github.com/apache/pulsar/pull/6567)
* [Tiered Storage]  Avoid prefetch too much data causing OutOfMemory, when offloading data to HDFS [#6717](https://github.com/apache/pulsar/pull/6717)

##### Pulsar IO
* [IO] JDBC sink does not handle null in schema [#6848](https://github.com/apache/pulsar/pull/6848)


### 2.5.1 &mdash; 2020-04-20 <a id="2.5.1"></a>

#### Features

* PIP-55: Refresh Authentication Credentials [#6074](https://github.com/apache/pulsar/pull/6074)
* Namespace level support offloader [#6183](https://github.com/apache/pulsar/pull/6183)  
* Upgrade Avro to 1.9.1 [#5938](https://github.com/apache/pulsar/pull/5938)  
  * *Avro 1.9.1 enables the JSR310 datetimes by default, which might introduce some regression problems if users use generated source code by Avro compiler 1.8.x and contains datetimes fields. It's better to use Avro 1.9.x compiler to recompile.*
* Support `unload` all partitions of a partitioned topic [#6187](https://github.com/apache/pulsar/pull/6187)  
* Supports evenly distribute topics count when splits bundle [#6241](https://github.com/apache/pulsar/pull/6241)  
* KeyValue schema support for pulsar sql [#6325](https://github.com/apache/pulsar/pull/6325)  
* Bump netty version to 4.1.45.Final [#6424](https://github.com/apache/pulsar/pull/6424)  
* Support BouncyCastle FIPS provider [#6588](https://github.com/apache/pulsar/pull/6588)  
* Improve Key_Shared subscription message dispatching performance. [#6647](https://github.com/apache/pulsar/pull/6647)  
* Add JSR310 logical type conversion. [#6704](https://github.com/apache/pulsar/pull/6704)  
* Introduce maxMessagePublishBufferSizeInMB configuration to avoid broker OOM [#6178](https://github.com/apache/pulsar/pull/6178)  


#### Fixes

##### Broker
* [Broker] Fixed NPE occurs when getting partitioned topic stats [#6060](https://github.com/apache/pulsar/pull/6060)
* [Broker] Fixed zero queue consumer message redelivery [#6076](https://github.com/apache/pulsar/pull/6076)
* [Broker] Fixed message redelivery for zero queue consumer while using async api to receive messages [#6090](https://github.com/apache/pulsar/pull/6090)
* [broker] Fixed bug that backlog message that has not yet expired could be deleted due to TTL [#6211](https://github.com/apache/pulsar/pull/6211)
* [Broker] Remove problematic semicolon from conf [#6303](https://github.com/apache/pulsar/pull/6303)
* [Broker] Fixed broker to specify a list of bookie groups [#6349](https://github.com/apache/pulsar/pull/6349)
* [Broker] Fixed create consumer on partitioned topic while disable topic auto creation [#5572](https://github.com/apache/pulsar/pull/5572)
* [Broker] Fix maven broken link [#6068](https://github.com/apache/pulsar/pull/6068)
* [Broker] Fixed java code errors reported by lgtm. [#6398](https://github.com/apache/pulsar/pull/6398)
* [Broker] Fixed memory leak when running topic compaction. [#6485](https://github.com/apache/pulsar/pull/6485)
* [Broker] Fixed admin getLastMessageId return batchIndex [#6511](https://github.com/apache/pulsar/pull/6511)
* [Broker] Fixed topic with one partition cannot be updated [#6560](https://github.com/apache/pulsar/pull/6560)
* [Broker] Fixed negative un-ack messages in consumer stats [#5929](https://github.com/apache/pulsar/pull/5929)
* [broker] Fixed bug that tenants whose allowed clusters include global cannot be created/updated [#6275](https://github.com/apache/pulsar/pull/6275)
* [Broker] Fixed log compaction for flow control/empty topic/last deletion [#6237](https://github.com/apache/pulsar/pull/6237)
* [Broker] Fixed restore clusterDispatchRate policy for compatibility [#6176](https://github.com/apache/pulsar/pull/6176) 
* [Broker] Fix some async method problems at PersistentTopicsBase. [#6483](https://github.com/apache/pulsar/pull/6483) 
* [Broker] This "earlier" message should be avoided to emit when reset cursor.[#6393](https://github.com/apache/pulsar/pull/6393)
* [Broker] Change the permission level of managing subscription from super-user to tenant admin [#6122](https://github.com/apache/pulsar/pull/6122)

##### Managed Ledger
* [Managed Ledger] Fixed consumer received duplicated delayed messages upon restart [#6404](https://github.com/apache/pulsar/pull/6404)


##### Pulsar Proxy

* [Proxy] Fixed correct name for proxy thread executor name [#6460](https://github.com/apache/pulsar/pull/6460)
* [Proxy] Fixed logging for published messages [#6474](https://github.com/apache/pulsar/pull/6474)
* [Proxy] Fixed proxy routing to functions worker [#6486](https://github.com/apache/pulsar/pull/6486)


##### Zookeeper

* [Zookeeper] Fixed casting in ZooKeeperCache.getDataIfPresent() [#6313](https://github.com/apache/pulsar/pull/6313)

##### Pulsar Functions
* [Function] remove future.join() from PulsarSinkEffectivelyOnceProcessor [#6361](https://github.com/apache/pulsar/pull/6361)


##### Pulsar SQL
* [SQL] Fixed integration Pulsar SQL test failed [#6279](https://github.com/apache/pulsar/pull/6279)

##### Security
* Fixed publish buffer limit does not take effect [#6431](https://github.com/apache/pulsar/pull/6431)
* Fixed the bug of authenticationData is't initialized. [#6440](https://github.com/apache/pulsar/pull/6440)

##### Pulsar Schema

* [Schema] Fixed get schema version in HttpLookupService. [#6193](https://github.com/apache/pulsar/pull/6193) 
* [Schema] Fixed avro schema decode error `ClassCastException` in Pulsar Function [#6662](https://github.com/apache/pulsar/pull/6662)
* [Schema] Fixed channel write error handling for send get raw schema request [#6650](https://github.com/apache/pulsar/pull/6650)

##### Java client
* [Client] Fixed available permits may be greater than 1 even though queue size is 0. [#6106](https://github.com/apache/pulsar/pull/6106)
* [Client] Fixed broker client tls settings error [#6128](https://github.com/apache/pulsar/pull/6128)
* [Client]Fixed hasMessageAvailable() [#6362](https://github.com/apache/pulsar/pull/6362)
* [Client] Fixed duplicate key to send propertys [#6390](https://github.com/apache/pulsar/pull/6390)
* [Client] fixed deadlock on send failure [#6488](https://github.com/apache/pulsar/pull/6488)
* [Client] Fixed NPE while call getLastMessageId [#6562](https://github.com/apache/pulsar/pull/6562) 
* [Client] Fixed the max backoff configuration for lookups [#6444](https://github.com/apache/pulsar/pull/6444)


##### C++ client
* [C++] Fixed static linking on C++ lib on MacOS [#5581](https://github.com/apache/pulsar/pull/5581)
* [C++] Fixed memory corruption on ExecutorService destructor [#6270](https://github.com/apache/pulsar/pull/6270)
* [C++] Fixed handling of canceled timer events on NegativeAcksTracker [#6272](https://github.com/apache/pulsar/pull/6272)
* [C++] Fixed for possible deadlock when closing Pulsar client [#6277](https://github.com/apache/pulsar/pull/6277)
* [C++] Fixed Unacked Message Tracker by Using Time Partition on C++ [#6391](https://github.com/apache/pulsar/pull/6391)
* [C++] Fixed Redelivery of Messages on UnackedMessageTracker When Ack Messages . [#6498](https://github.com/apache/pulsar/pull/6498)

##### Python Client
* [Python Client]Fixed the enum34 package not found [#6401](https://github.com/apache/pulsar/pull/6401)

##### Pulsar Websocket
* [Websocket] Fixed Websocket doesn't set the correct cluster data [#6102](https://github.com/apache/pulsar/pull/6102)

##### Deployments
* [Helm] Autorecovery - Fixed could not find or load main class [#6373](https://github.com/apache/pulsar/pull/6373)
* [Helm]: Start proxy pods when at least one broker pod is running [#6158](https://github.com/apache/pulsar/pull/6158)

#### Enhancements

##### Pulsar Broker
* [Broker] close managed-ledgers before giving up bundle ownership to avoid bad zk-version [#5599](https://github.com/apache/pulsar/pull/5599)
* [Broker] Add timeout to search for web service URLs to avoid web threads getting stuck [#6124](https://github.com/apache/pulsar/pull/6124)
* [Broker] Flush the potential duplicated message when add messages to a batch. [#6326](https://github.com/apache/pulsar/pull/6326)
* [Broker] Avoid getting partition metadata while the topic name is a partition name. [#6339](https://github.com/apache/pulsar/pull/6339)
* [Broker] Fixed create partitioned topic with a substring of an existing topic name. [#6478](https://github.com/apache/pulsar/pull/6478)
* [Broker] Do not retry on authorization failure [#6577](https://github.com/apache/pulsar/pull/6577)
* [Broker]Handle BadVersionException thrown by updateSchemaLocator() [#6683](https://github.com/apache/pulsar/pull/6683)
* [Broker] Expose bookkeeper expose explicit lac configuration in broker.conf [#5822](https://github.com/apache/pulsar/pull/5822)
* [Broker] Allow to enable/disable delayed delivery for messages on namespace [#5915](https://github.com/apache/pulsar/pull/5915)
* [Broker] Prevent creation of regular topic with the same name as existing partitioned topic [#5943](https://github.com/apache/pulsar/pull/5943)
* [Broker] Reset cursor with a non-exists position [#6120](https://github.com/apache/pulsar/pull/6120)
* [Broker] Use fully qualified hostname as default to advertise brokers [#6235](https://github.com/apache/pulsar/pull/6235)
* [broker] Timeout API calls in BrokerService [#6489](https://github.com/apache/pulsar/pull/6489)
* [Broker] Start namespace service and schema registry service before start broker. [#6499](https://github.com/apache/pulsar/pull/6499)
* [Broker] Disable channel auto read when publish rate or publish buffer exceeded [#6550](https://github.com/apache/pulsar/pull/6550)
* [Broker] Resume some servercnx method to public [#6581](https://github.com/apache/pulsar/pull/6581)
* [Broker] Enable get precise backlog and backlog without delayed messages. [#6310](https://github.com/apache/pulsar/pull/6310)
* [Broker] Avoid using same OpAddEntry between different ledger handles [#5942](https://github.com/apache/pulsar/pull/5942)
* [Broker] Clean up closed producer to avoid publish-time for producer [#5988](https://github.com/apache/pulsar/pull/5988)
* [Broker] Support delete inactive topic when subscriptions caught up [#6077](https://github.com/apache/pulsar/pull/6077)
* [Broker] Add a message on how to make log refresh immediately when starting a component [#6078](https://github.com/apache/pulsar/pull/6078)
* [Pulsar Admin] allow tenant admin to manage subscription permission [#6122](https://github.com/apache/pulsar/pull/6122)
* [Broker] Output resource usage rate to log on broker [#6152](https://github.com/apache/pulsar/pull/6152)
* [Broker] Creating a topic does not wait for creating cursor of replicators [#6364](https://github.com/apache/pulsar/pull/6364)
* [Broker] Stop increase unacked messages for the consumer with Exclusive/Failover subscription mode. [#6558](https://github.com/apache/pulsar/pull/6558)
* [Broker] Not allow sub auto create by admin when disable topic auto create [#6685](https://github.com/apache/pulsar/pull/6685)


##### Zookeeper
* [Zookeeper] Close ZK before canceling future with exception [#6399](https://github.com/apache/pulsar/pull/6399)
* [ZooKeeper] Upgrade ZooKeeper to 3.5.7 [#6329](https://github.com/apache/pulsar/pull/6329)

##### Pulsar IO
* [IO] Adds integration test for RabbitMQ [#6033](https://github.com/apache/pulsar/pull/6033)

##### Pulsar Functions
* [Function] remove future.join() from PulsarSinkEffectivelyOnceProcessor [#6361](https://github.com/apache/pulsar/pull/6361)

##### Stats & Monitoring 
* [Broker] Add backlogSize in topicStats [#5914](https://github.com/apache/pulsar/pull/5914)
* [Broker] Expose lastConsumedTimestamp and lastAckedTimestamp to consumer stats [#6051](https://github.com/apache/pulsar/pull/6051)
* Improve backlogSize stats in the topic. [#6700](https://github.com/apache/pulsar/pull/6700)

##### Security
* Validate tokens for binary connections [#6233](https://github.com/apache/pulsar/pull/6233)
* Create namespace failed when TLS is enabled in PulsarStandalone [#6457](https://github.com/apache/pulsar/pull/6457) 
* Use more granular permissions for topics [#6504](https://github.com/apache/pulsar/pull/6504)


##### Pulsar Schema

* [Schema] Independent schema is set for each consumer generated by topic [#6356](https://github.com/apache/pulsar/pull/6356)
* [Schema] Extract an original avro schema from the "$SCHEMA" field using reflection. If it doesn't work, the process falls back generation of the schema from POJO.[#6406](https://github.com/apache/pulsar/pull/6406)
* [Schema] Add verification for SchemaDefinitionBuilderImpl.java [#6405](https://github.com/apache/pulsar/pull/6405)

##### Java client
* [Client] Start reader inside batch result in read first message in batch. [#6345](https://github.com/apache/pulsar/pull/6345)
* [Client] Stop shade snappy-java in pulsar-client-shaded [#6375](https://github.com/apache/pulsar/pull/6375)
* [Client] MultiTopics discovery is broken due to discovery task scheduled twice instead of pendingBatchReceiveTask [#6407](https://github.com/apache/pulsar/pull/6407)
* [Client] Make SubscriptionMode a member of ConsumerConfigurationData [#6337](https://github.com/apache/pulsar/pull/6337)
* [Client] Should set either start message id or start message from roll back duration. [#6392](https://github.com/apache/pulsar/pull/6392)
* [Client] BatchReceivePolicy implements Serializable. [#6423](https://github.com/apache/pulsar/pull/6423)
* [Client] Remove duplicate cnx method [#6490](https://github.com/apache/pulsar/pull/6490)
* [Client] Pulsar Java client: Use System.nanoTime() instead of System.currentTimeMillis() to measure elapsed time [#6454](https://github.com/apache/pulsar/pull/6454)
* [Client] Make tests more stable by using JSONAssert equals [#6247](https://github.com/apache/pulsar/pull/6247)
* [Client] make acker in BatchMessageIdImpl transient [#6064](https://github.com/apache/pulsar/pull/6064)


##### C++ client
* [C++] Windows CMake corrections [#6336](https://github.com/apache/pulsar/pull/6336)
* [C++] Avoid calling redeliverMessages() when message list is empty [#6480](https://github.com/apache/pulsar/pull/6480)
* [C++] Improve cpp-client-lib: provide another `libpulsarwithdeps.a` in dep/rpm [#6458](https://github.com/apache/pulsar/pull/6458)

##### Python Client
* [Python Client] Support generate pulsar-client for python3.8[#6741](https://github.com/apache/pulsar/pull/6741) 

##### Deployments
* [Helm] Explicit statement env-var 'BOOKIE_MEM' and 'BOOKIE_GC' for values-mini.yaml [#6340](https://github.com/apache/pulsar/pull/6340)
* [Helm] Add missing check to dashboard-ingress [#6160](https://github.com/apache/pulsar/pull/6160)
* Make kubernetes yamls for aws operational [#6192](https://github.com/apache/pulsar/pull/6192)
* Ensure JVM memory and GC options are set for bookie [#6201](https://github.com/apache/pulsar/pull/6201)
* Default functionAuthProvider when running in k8s [#6203](https://github.com/apache/pulsar/pull/6203)

##### Adaptors
* [Adaptor] Skip javadoc task for pulsar-client-kafka-compact modules [#5836](https://github.com/apache/pulsar/pull/5836)
* [Flink-Connector] Get PulsarClient from cache should always return an open instance [#6436](https://github.com/apache/pulsar/pull/6436)


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
