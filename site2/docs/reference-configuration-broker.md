# Broker
|Name|Description|Default|Dynamic|Category|
|---|---|---|---|---|
| zookeeperServers | The Zookeeper quorum connection string (as a comma-separated list). Deprecated in favour of metadataStoreUrl | null | false | Server | 
| metadataStoreUrl | The metadata store URL. <br /> Examples: <br />  * zk:my-zk-1:2181,my-zk-2:2181,my-zk-3:2181<br />  * my-zk-1:2181,my-zk-2:2181,my-zk-3:2181 (will default to ZooKeeper when the schema is not specified)<br />  * zk:my-zk-1:2181,my-zk-2:2181,my-zk-3:2181/my-chroot-path (to add a ZK chroot path)<br /> | null | false | Server | 
| globalZookeeperServers | Global Zookeeper quorum connection string (as a comma-separated list). Deprecated in favor of using `configurationStoreServers` | null | false | Server | 
| configurationStoreServers | Configuration store connection string (as a comma-separated list). Deprecated in favor of `configurationMetadataStoreUrl` | null | false | Server | 
| configurationMetadataStoreUrl | The metadata store URL for the configuration data. If empty, we fall back to use metadataStoreUrl | null | false | Server | 
| brokerServicePort | The port for serving binary protobuf requests. If set, defines a server binding for bindAddress:brokerServicePort. The Default value is 6650. | Optional[6650] | false | Server | 
| brokerServicePortTls | The port for serving TLS-secured binary protobuf requests. If set, defines a server binding for bindAddress:brokerServicePortTls. | Optional.empty | false | Server | 
| webServicePort | The port for serving http requests | Optional[8080] | false | Server | 
| webServicePortTls | The port for serving https requests | Optional.empty | false | Server | 
| webServiceTlsProvider | Specify the TLS provider for the web service: SunJSSE, Conscrypt and etc. | Conscrypt | false | Server | 
| webServiceTlsProtocols | Specify the tls protocols the proxy's web service will use to negotiate during TLS Handshake.<br /><br />Example:- [TLSv1.3, TLSv1.2] | [] | false | TLS | 
| webServiceTlsCiphers | Specify the tls cipher the proxy's web service will use to negotiate during TLS Handshake.<br /><br />Example:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256] | [] | false | TLS | 
| bindAddress | Hostname or IP address the service binds on | 0.0.0.0 | false | Server | 
| advertisedAddress | Hostname or IP address the service advertises to the outside world. If not set, the value of `InetAddress.getLocalHost().getHostname()` is used. | null | false | Server | 
| advertisedListeners | Used to specify multiple advertised listeners for the broker. The value must format as <listener_name\>:pulsar://<host\>:<port\>,multiple listeners should separate with commas.Do not use this configuration with advertisedAddress and brokerServicePort.The Default value is absent means use advertisedAddress and brokerServicePort. | null | false | Server | 
| internalListenerName | Used to specify the internal listener name for the broker.The listener name must contain in the advertisedListeners.The Default value is absent, the broker uses the first listener as the internal listener. | null | false | Server | 
| bindAddresses | Used to specify additional bind addresses for the broker. The value must format as <listener_name\>:<scheme\>://<host\>:<port\>, multiple bind addresses should be separated with commas. Associates each bind address with an advertised listener and protocol handler. Note that the brokerServicePort, brokerServicePortTls, webServicePort, and webServicePortTls properties define additional bindings. | null | false | Server | 
| haProxyProtocolEnabled | Enable or disable the proxy protocol. | false | false | Server | 
| numAcceptorThreads | Number of threads to use for Netty Acceptor. Default is set to `1` | 1 | false | Server | 
| numIOThreads | Number of threads to use for Netty IO. Default is set to `2 * Runtime.getRuntime().availableProcessors()` | 16 | false | Server | 
| numOrderedExecutorThreads | Number of threads to use for orderedExecutor. The ordered executor is used to operate with zookeeper, such as init zookeeper client, get namespace policies from zookeeper etc. It also used to split bundle. Default is 8 | 8 | false | Server | 
| numHttpServerThreads | Number of threads to use for HTTP requests processing Default is set to `2 * Runtime.getRuntime().availableProcessors()` | 16 | false | Server | 
| numExecutorThreadPoolSize | Number of threads to use for pulsar broker service. The executor in thread pool will do basic broker operation like load/unload bundle, update managedLedgerConfig, update topic/subscription/replicator message dispatch rate, do leader election etc. Default is set to 20  | 8 | false | Server | 
| numCacheExecutorThreadPoolSize | Number of thread pool size to use for pulsar zookeeper callback service.The cache executor thread pool is used for restarting global zookeeper session. Default is 10 | 10 | false | Server | 
| enableBusyWait | Option to enable busy-wait settings. Default is false. WARNING: This option will enable spin-waiting on executors and IO threads in order to reduce latency during context switches. The spinning will consume 100% CPU even when the broker is not doing any work. It is recommended to reduce the number of IO threads and BK client threads to only have few CPU cores busy. | false | false | Server | 
| maxConcurrentHttpRequests | Max concurrent web requests | 1024 | false | Server | 
| httpServerThreadPoolQueueSize | Capacity for thread pool queue in the HTTP server Default is set to 8192. | 8192 | false | Server | 
| httpServerAcceptQueueSize | Capacity for accept queue in the HTTP server Default is set to 8192. | 8192 | false | Server | 
| maxHttpServerConnections | Maximum number of inbound http connections. (0 to disable limiting) | 2048 | false | Server | 
| delayedDeliveryEnabled | Whether to enable the delayed delivery for messages. | true | false | Server | 
| delayedDeliveryTrackerFactoryClassName | Class name of the factory that implements the delayed deliver tracker | org.apache.pulsar.broker.delayed.InMemoryDelayedDeliveryTrackerFactory | false | Server | 
| delayedDeliveryTickTimeMillis | Control the tick time for when retrying on delayed delivery, affecting the accuracy of the delivery time compared to the scheduled time. Default is 1 second. Note that this time is used to configure the HashedWheelTimer's tick time for the InMemoryDelayedDeliveryTrackerFactory. | 1000 | false | Server | 
| isDelayedDeliveryDeliverAtTimeStrict | When using the InMemoryDelayedDeliveryTrackerFactory (the default DelayedDeliverTrackerFactory), whether the deliverAt time is strictly followed. When false (default), messages may be sent to consumers before the deliverAt time by as much as the tickTimeMillis. This can reduce the overhead on the broker of maintaining the delayed index for a potentially very short time period. When true, messages will not be sent to consumer until the deliverAt time has passed, and they may be as late as the deliverAt time plus the tickTimeMillis for the topic plus the delayedDeliveryTickTimeMillis. | false | false | Server | 
| acknowledgmentAtBatchIndexLevelEnabled | Whether to enable the acknowledge of batch local index | false | false | Server | 
| webSocketServiceEnabled | Enable the WebSocket API service in broker | false | false | WebSocket | 
| isRunningStandalone | Flag indicates whether to run broker in standalone mode | false | false | WebSocket | 
| clusterName | Name of the cluster to which this broker belongs to | null | false | Server | 
| maxTenants | The maximum number of tenants that each pulsar cluster can create.This configuration is not precise control, in a concurrent scenario, the threshold will be exceeded. | 0 | true | Server | 
| failureDomainsEnabled | Enable cluster's failure-domain which can distribute brokers into logical region | false | true | Server | 
| metadataStoreSessionTimeoutMillis | Metadata store session timeout in milliseconds. | 30000 | false | Server | 
| metadataStoreOperationTimeoutSeconds | Metadata store operation timeout in seconds. | 30 | false | Server | 
| metadataStoreCacheExpirySeconds | Metadata store cache expiry time in seconds. | 300 | false | Server | 
| zooKeeperSessionTimeoutMillis | ZooKeeper session timeout in milliseconds. @deprecated - Use metadataStoreSessionTimeoutMillis instead. | -1 | false | Server | 
| zooKeeperOperationTimeoutSeconds | ZooKeeper operation timeout in seconds. @deprecated - Use metadataStoreOperationTimeoutSeconds instead. | -1 | false | Server | 
| zooKeeperCacheExpirySeconds | ZooKeeper cache expiry time in seconds. @deprecated - Use metadataStoreCacheExpirySeconds instead. | -1 | false | Server | 
| brokerShutdownTimeoutMs | Time to wait for broker graceful shutdown. After this time elapses, the process will be killed | 60000 | true | Server | 
| skipBrokerShutdownOnOOM | Flag to skip broker shutdown when broker handles Out of memory error | false | true | Server | 
| topicLoadTimeoutSeconds | Amount of seconds to timeout when loading a topic. In situations with many geo-replicated clusters, this may need raised. | 60 | false | Server | 
| metadataStoreBatchingEnabled | Whether we should enable metadata operations batching | true | false | Server | 
| metadataStoreBatchingMaxDelayMillis | Maximum delay to impose on batching grouping | 5 | false | Server | 
| metadataStoreBatchingMaxOperations | Maximum number of operations to include in a singular batch | 1000 | false | Server | 
| metadataStoreBatchingMaxSizeKb | Maximum size of a batch | 128 | false | Server | 
| metadataStoreConfigPath | Configuration file path for local metadata store. It's supported by RocksdbMetadataStore for now. | null | false | Server | 
| backlogQuotaCheckEnabled | Enable backlog quota check. Enforces actions on topic when the quota is reached | true | false | Policies | 
| preciseTimeBasedBacklogQuotaCheck | Whether to enable precise time based backlog quota check. Enabling precise time based backlog quota check will cause broker to read first entry in backlog of the slowest cursor on a ledger which will mostly result in reading entry from BookKeeper's disk which can have negative impact on overall performance. Disabling precise time based backlog quota check will just use the timestamp indicating when a ledger was closed, which is of coarser granularity. | false | false | Policies | 
| backlogQuotaCheckIntervalInSeconds | How often to check for topics that have reached the quota. It only takes effects when `backlogQuotaCheckEnabled` is true | 60 | false | Policies | 
| backlogQuotaDefaultLimitGB | @deprecated - Use backlogQuotaDefaultLimitByte instead." | -1.0 | false | Policies | 
| backlogQuotaDefaultLimitBytes | Default per-topic backlog quota limit by size, less than 0 means no limitation. default is -1. Increase it if you want to allow larger msg backlog | -1 | false | Policies | 
| backlogQuotaDefaultLimitSecond | Default per-topic backlog quota limit by time in second, less than 0 means no limitation. default is -1. Increase it if you want to allow larger msg backlog | -1 | false | Policies | 
| backlogQuotaDefaultRetentionPolicy | Default backlog quota retention policy. Default is producer_request_hold<br /><br />'producer_request_hold' Policy which holds producer's send request until theresource becomes available (or holding times out)<br />'producer_exception' Policy which throws javax.jms.ResourceAllocationException to the producer<br />'consumer_backlog_eviction' Policy which evicts the oldest message from the slowest consumer's backlog | producer_request_hold | false | Policies | 
| ttlDurationDefaultInSeconds | Default ttl for namespaces if ttl is not already configured at namespace policies. (disable default-ttl with value 0) | 0 | false | Policies | 
| brokerDeleteInactiveTopicsEnabled | Enable the deletion of inactive topics.<br />If only enable this option, will not clean the metadata of partitioned topic. | true | true | Policies | 
| brokerDeleteInactivePartitionedTopicMetadataEnabled | Metadata of inactive partitioned topic will not be automatically cleaned up by default.<br />Note: If `allowAutoTopicCreation` and this option are enabled at the same time,<br />it may appear that a partitioned topic has just been deleted but is automatically created as a non-partitioned topic. | false | true | Policies | 
| brokerDeleteInactiveTopicsFrequencySeconds | How often to check for inactive topics | 60 | true | Policies | 
| brokerDeleteInactiveTopicsMode | Set the inactive topic delete mode. Default is delete_when_no_subscriptions<br />'delete_when_no_subscriptions' mode only delete the topic which has no subscriptions and no active producers<br />'delete_when_subscriptions_caught_up' mode only delete the topic that all subscriptions has no backlogs(caught up) and no active producers/consumers | delete_when_no_subscriptions | true | Policies | 
| brokerDeleteInactiveTopicsMaxInactiveDurationSeconds | Max duration of topic inactivity in seconds, default is not present<br />If not present, 'brokerDeleteInactiveTopicsFrequencySeconds' will be used<br />Topics that are inactive for longer than this value will be deleted | null | true | Policies | 
| forceDeleteTenantAllowed | Allow forced deletion of tenants. Default is false. | false | false | Policies | 
| forceDeleteNamespaceAllowed | Allow forced deletion of namespaces. Default is false. | false | false | Policies | 
| maxPendingPublishRequestsPerConnection | Max pending publish requests per connection to avoid keeping large number of pending requests in memory. Default: 1000 | 1000 | false | Policies | 
| messageExpiryCheckIntervalInMinutes | How frequently to proactively check and purge expired messages | 5 | false | Policies | 
| activeConsumerFailoverDelayTimeMillis | How long to delay rewinding cursor and dispatching messages when active consumer is changed | 1000 | false | Policies | 
| subscriptionExpirationTimeMinutes | How long to delete inactive subscriptions from last consuming. When it is 0, inactive subscriptions are not deleted automatically | 0 | false | Policies | 
| subscriptionRedeliveryTrackerEnabled | Enable subscription message redelivery tracker to send redelivery count to consumer (default is enabled) | true | true | Policies | 
| subscriptionExpiryCheckIntervalInMinutes | How frequently to proactively check and purge expired subscription | 5 | false | Policies | 
| subscriptionTypesEnabled | Enable subscription types (default is all type enabled) | [Failover, Shared, Key_Shared, Exclusive] | true | Policies | 
| subscriptionKeySharedEnable | Enable Key_Shared subscription (default is enabled) | true | true | Policies | 
| subscriptionKeySharedUseConsistentHashing | On KeyShared subscriptions, with default AUTO_SPLIT mode, use splitting ranges or consistent hashing to reassign keys to new consumers (default is consistent hashing) | true | false | Policies | 
| subscriptionKeySharedConsistentHashingReplicaPoints | On KeyShared subscriptions, number of points in the consistent-hashing ring. The higher the number, the more equal the assignment of keys to consumers | 100 | false | Policies | 
| brokerDeduplicationEnabled | Set the default behavior for message deduplication in the broker.<br /><br />This can be overridden per-namespace. If enabled, broker will reject messages that were already stored in the topic | false | false | Policies | 
| brokerDeduplicationMaxNumberOfProducers | Maximum number of producer information that it's going to be persisted for deduplication purposes | 10000 | false | Policies | 
| brokerDeduplicationSnapshotFrequencyInSeconds | How often is the thread pool scheduled to check whether a snapshot needs to be taken.(disable with value 0) | 120 | false | Policies | 
| brokerDeduplicationSnapshotIntervalSeconds | If this time interval is exceeded, a snapshot will be taken.It will run simultaneously with `brokerDeduplicationEntriesInterval` | 120 | false | Policies | 
| brokerDeduplicationEntriesInterval | Number of entries after which a dedup info snapshot is taken.<br /><br />A bigger interval will lead to less snapshots being taken though it would increase the topic recovery time, when the entries published after the snapshot need to be replayed | 1000 | false | Policies | 
| brokerDeduplicationProducerInactivityTimeoutMinutes | Time of inactivity after which the broker will discard the deduplication information relative to a disconnected producer. Default is 6 hours. | 360 | false | Policies | 
| defaultNumberOfNamespaceBundles | When a namespace is created without specifying the number of bundle, this value will be used as the default | 4 | false | Policies | 
| maxNamespacesPerTenant | The maximum number of namespaces that each tenant can create.This configuration is not precise control, in a concurrent scenario, the threshold will be exceeded | 0 | true | Policies | 
| maxTopicsPerNamespace | Max number of topics allowed to be created in the namespace. When the topics reach the max topics of the namespace, the broker should reject the new topic request(include topic auto-created by the producer or consumer) until the number of connected consumers decrease.  Using a value of 0, is disabling maxTopicsPerNamespace-limit check. | 0 | true | Policies | 
| brokerMaxConnections | The maximum number of connections in the broker. If it exceeds, new connections are rejected. | 0 | false | Policies | 
| brokerMaxConnectionsPerIp | The maximum number of connections per IP. If it exceeds, new connections are rejected. | 0 | false | Policies | 
| isAllowAutoUpdateSchemaEnabled | Allow schema to be auto updated at broker level. User can override this by 'is_allow_auto_update_schema' of namespace policy. This is enabled by default. | true | true | Policies | 
| autoShrinkForConsumerPendingAcksMap | Whether to enable the automatic shrink of pendingAcks map, the default is false, which means it is not enabled. When there are a large number of share or key share consumers in the cluster, it can be enabled to reduce the memory consumption caused by pendingAcks. | false | false | Server | 
| clientLibraryVersionCheckEnabled | Enable check for minimum allowed client library version | false | true | Server | 
| statusFilePath | Path for the file used to determine the rotation status for the broker when responding to service discovery health checks | null | false | Server | 
| maxUnackedMessagesPerConsumer | Max number of unacknowledged messages allowed to receive messages by a consumer on a shared subscription.<br /><br /> Broker will stop sending messages to consumer once, this limit reaches until consumer starts acknowledging messages back and unack count reaches to `maxUnackedMessagesPerConsumer/2`. Using a value of 0, it is disabling  unackedMessage-limit check and consumer can receive messages without any restriction | 50000 | false | Policies | 
| maxUnackedMessagesPerSubscription | Max number of unacknowledged messages allowed per shared subscription. <br /><br /> Broker will stop dispatching messages to all consumers of the subscription once this  limit reaches until consumer starts acknowledging messages back and unack count reaches to `limit/2`. Using a value of 0, is disabling unackedMessage-limit check and dispatcher can dispatch messages without any restriction | 200000 | false | Policies | 
| maxUnackedMessagesPerBroker | Max number of unacknowledged messages allowed per broker. <br /><br /> Once this limit reaches, broker will stop dispatching messages to all shared subscription  which has higher number of unack messages until subscriptions start acknowledging messages  back and unack count reaches to `limit/2`. Using a value of 0, is disabling unackedMessage-limit check and broker doesn't block dispatchers | 0 | false | Policies | 
| maxUnackedMessagesPerSubscriptionOnBrokerBlocked | Once broker reaches maxUnackedMessagesPerBroker limit, it blocks subscriptions which has higher  unacked messages than this percentage limit and subscription will not receive any new messages  until that subscription acks back `limit/2` messages | 0.16 | false | Policies | 
| maxConsumerMetadataSize | Maximum size of Consumer metadata | 1024 | false | Policies | 
| unblockStuckSubscriptionEnabled | Broker periodically checks if subscription is stuck and unblock if flag is enabled. (Default is disabled) | false | true | Policies | 
| topicPublisherThrottlingTickTimeMillis | Tick time to schedule task that checks topic publish rate limiting across all topics  Reducing to lower value can give more accuracy while throttling publish but it uses more CPU to perform frequent check. (Disable publish throttling with value 0) | 10 | true | Policies | 
| preciseTopicPublishRateLimiterEnable | Enable precise rate limit for topic publish | false | false | Server | 
| brokerPublisherThrottlingTickTimeMillis | Tick time to schedule task that checks broker publish rate limiting across all topics  Reducing to lower value can give more accuracy while throttling publish but it uses more CPU to perform frequent check. (Disable publish throttling with value 0) | 50 | true | Server | 
| brokerPublisherThrottlingMaxMessageRate | Max Rate(in 1 seconds) of Message allowed to publish for a broker when broker publish rate limiting enabled. (Disable message rate limit with value 0) | 0 | true | Server | 
| brokerPublisherThrottlingMaxByteRate | Max Rate(in 1 seconds) of Byte allowed to publish for a broker when broker publish rate limiting enabled. (Disable byte rate limit with value 0) | 0 | true | Server | 
| dispatchThrottlingRateInMsg | Default messages per second dispatch throttling-limit for whole broker. Using a value of 0, is disabling default message-byte dispatch-throttling | 0 | true | Server | 
| dispatchThrottlingRateInByte | Default bytes per second dispatch throttling-limit for whole broker. Using a value of 0, is disabling default message-byte dispatch-throttling | 0 | true | Server | 
| maxPublishRatePerTopicInMessages | Max Rate(in 1 seconds) of Message allowed to publish for a topic when topic publish rate limiting enabled. (Disable byte rate limit with value 0) | 0 | true | Server | 
| maxPublishRatePerTopicInBytes | Max Rate(in 1 seconds) of Byte allowed to publish for a topic when topic publish rate limiting enabled. (Disable byte rate limit with value 0) | 0 | true | Server | 
| subscribeThrottlingRatePerConsumer | Too many subscribe requests from a consumer can cause broker rewinding consumer cursors  and loading data from bookies, hence causing high network bandwidth usage When the positive value is set, broker will throttle the subscribe requests for one consumer. Otherwise, the throttling will be disabled. The default value of this setting is 0 - throttling is disabled. | 0 | true | Policies | 
| subscribeRatePeriodPerConsumerInSecond | Rate period for {subscribeThrottlingRatePerConsumer}. Default is 30s. | 30 | true | Policies | 
| dispatchThrottlingRatePerTopicInMsg | Default number of message dispatching throttling-limit for every topic. <br /><br />Using a value of 0, is disabling default message dispatch-throttling | 0 | true | Policies | 
| dispatchThrottlingRatePerTopicInByte | Default number of message-bytes dispatching throttling-limit for every topic. <br /><br />Using a value of 0, is disabling default message-byte dispatch-throttling | 0 | true | Policies | 
| dispatchThrottlingOnBatchMessageEnabled | Apply dispatch rate limiting on batch message instead individual messages with in batch message. (Default is disabled) | false | true | Policies | 
| dispatchThrottlingRatePerSubscriptionInMsg | Default number of message dispatching throttling-limit for a subscription. <br /><br />Using a value of 0, is disabling default message dispatch-throttling. | 0 | true | Policies | 
| dispatchThrottlingRatePerSubscriptionInByte | Default number of message-bytes dispatching throttling-limit for a subscription. <br /><br />Using a value of 0, is disabling default message-byte dispatch-throttling. | 0 | true | Policies | 
| dispatchThrottlingRatePerReplicatorInMsg | Default number of message dispatching throttling-limit for every replicator in replication. <br /><br />Using a value of 0, is disabling replication message dispatch-throttling | 0 | true | Policies | 
| dispatchThrottlingRatePerReplicatorInByte | Default number of message-bytes dispatching throttling-limit for every replicator in replication. <br /><br />Using a value of 0, is disabling replication message-byte dispatch-throttling | 0 | true | Policies | 
| dispatchThrottlingRateRelativeToPublishRate | Dispatch rate-limiting relative to publish rate. (Enabling flag will make broker to dynamically update dispatch-rate relatively to publish-rate: throttle-dispatch-rate = (publish-rate + configured dispatch-rate)  | false | true | Policies | 
| dispatchThrottlingOnNonBacklogConsumerEnabled | Default dispatch-throttling is disabled for consumers which already caught-up with published messages and don't have backlog. This enables dispatch-throttling for  non-backlog consumers as well. | true | true | Policies | 
| resourceUsageTransportClassName | Default policy for publishing usage reports to system topic is disabled.This enables publishing of usage reports |  | false | Policies | 
| resourceUsageTransportPublishIntervalInSecs | Default interval to publish usage reports if resourceUsagePublishToTopic is enabled. | 60 | true | Policies | 
| enableBrokerSideSubscriptionPatternEvaluation | Enables evaluating subscription pattern on broker side. | true | false | Policies | 
| subscriptionPatternMaxLength | Max length of subscription pattern | 50 | false | Policies | 
| dispatcherMaxReadBatchSize | Max number of entries to read from bookkeeper. By default it is 100 entries. | 100 | true | Server | 
| dispatcherMaxReadSizeBytes | Max size in bytes of entries to read from bookkeeper. By default it is 5MB. | 5242880 | true | Server | 
| dispatcherMinReadBatchSize | Min number of entries to read from bookkeeper. By default it is 1 entries.When there is an error occurred on reading entries from bookkeeper, the broker will backoff the batch size to this minimum number. | 1 | true | Server | 
| dispatcherReadFailureBackoffInitialTimeInMs | The read failure backoff initial time in milliseconds. By default it is 15s. | 15000 | true | Server | 
| dispatcherReadFailureBackoffMaxTimeInMs | The read failure backoff max time in milliseconds. By default it is 60s. | 60000 | true | Server | 
| dispatcherReadFailureBackoffMandatoryStopTimeInMs | The read failure backoff mandatory stop time in milliseconds. By default it is 0s. | 0 | true | Server | 
| dispatcherMaxRoundRobinBatchSize | Max number of entries to dispatch for a shared subscription. By default it is 20 entries. | 20 | true | Server | 
| preciseDispatcherFlowControl | Precise dispatcher flow control according to history message number of each entry | false | true | Server | 
| entryFilterNames |  Class name of pluggable entry filter that decides whether the entry needs to be filtered.You can use this class to decide which entries can be sent to consumers.Multiple names need to be separated by commas. | [] | true | Server | 
| entryFiltersDirectory |  The directory for all the entry filter implementations. |  | true | Server | 
| streamingDispatch | Whether to use streaming read dispatcher. Currently is in preview and can be changed in subsequent release. | false | false | Server | 
| maxConcurrentLookupRequest | Max number of concurrent lookup request broker allows to throttle heavy incoming lookup traffic | 50000 | true | Server | 
| maxConcurrentTopicLoadRequest | Max number of concurrent topic loading request broker allows to control number of zk-operations | 5000 | true | Server | 
| maxConcurrentNonPersistentMessagePerConnection | Max concurrent non-persistent message can be processed per connection | 1000 | false | Server | 
| numWorkerThreadsForNonPersistentTopic | Number of worker threads to serve non-persistent topic | 8 | false | Server | 
| enablePersistentTopics | Enable broker to load persistent topics | true | false | Server | 
| enableNonPersistentTopics | Enable broker to load non-persistent topics | true | false | Server | 
| enableRunBookieTogether | Enable to run bookie along with broker | false | false | Server | 
| enableRunBookieAutoRecoveryTogether | Enable to run bookie autorecovery along with broker | false | false | Server | 
| maxProducersPerTopic | Max number of producers allowed to connect to topic. <br /><br />Once this limit reaches, Broker will reject new producers until the number of connected producers decrease. Using a value of 0, is disabling maxProducersPerTopic-limit check. | 0 | false | Server | 
| maxSameAddressProducersPerTopic | Max number of producers with the same IP address allowed to connect to topic. <br /><br />Once this limit reaches, Broker will reject new producers until the number of connected producers with the same IP address decrease. Using a value of 0, is disabling maxSameAddressProducersPerTopic-limit check. | 0 | false | Server | 
| encryptionRequireOnProducer | Enforce producer to publish encrypted messages.(default disable). | false | false | Server | 
| maxConsumersPerTopic | Max number of consumers allowed to connect to topic. <br /><br />Once this limit reaches, Broker will reject new consumers until the number of connected consumers decrease. Using a value of 0, is disabling maxConsumersPerTopic-limit check. | 0 | false | Server | 
| maxSameAddressConsumersPerTopic | Max number of consumers with the same IP address allowed to connect to topic. <br /><br />Once this limit reaches, Broker will reject new consumers until the number of connected consumers with the same IP address decrease. Using a value of 0, is disabling maxSameAddressConsumersPerTopic-limit check. | 0 | false | Server | 
| maxSubscriptionsPerTopic | Max number of subscriptions allowed to subscribe to topic. <br /><br />Once this limit reaches,  broker will reject new subscription until the number of subscribed subscriptions decrease.<br /> Using a value of 0, is disabling maxSubscriptionsPerTopic limit check. | 0 | false | Server | 
| maxConsumersPerSubscription | Max number of consumers allowed to connect to subscription. <br /><br />Once this limit reaches, Broker will reject new consumers until the number of connected consumers decrease. Using a value of 0, is disabling maxConsumersPerSubscription-limit check. | 0 | false | Server | 
| maxMessageSize | Max size of messages. | 5242880 | false | Server | 
| enableReplicatedSubscriptions | Enable tracking of replicated subscriptions state across clusters. | true | false | Server | 
| replicatedSubscriptionsSnapshotFrequencyMillis | Frequency of snapshots for replicated subscriptions tracking. | 1000 | false | Server | 
| replicatedSubscriptionsSnapshotTimeoutSeconds | Timeout for building a consistent snapshot for tracking replicated subscriptions state.  | 30 | false | Server | 
| replicatedSubscriptionsSnapshotMaxCachedPerSubscription | Max number of snapshot to be cached per subscription. | 10 | false | Server | 
| maxMessagePublishBufferSizeInMB | Max memory size for broker handling messages sending from producers.<br /><br /> If the processing message size exceed this value, broker will stop read data from the connection. The processing messages means messages are sends to broker but broker have not send response to client, usually waiting to write to bookies.<br /><br /> It's shared across all the topics running in the same broker.<br /><br /> Use -1 to disable the memory limitation. Default is 1/2 of direct memory.<br /><br /> | 2048 | true | Server | 
| messagePublishBufferCheckIntervalInMillis | Interval between checks to see if message publish buffer size is exceed the max message publish buffer size | 100 | false | Server | 
| lazyCursorRecovery | Whether to recover cursors lazily when trying to recover a managed ledger backing a persistent topic. It can improve write availability of topics.<br />The caveat is now when recovered ledger is ready to write we're not sure if all old consumers last mark delete position can be recovered or not. | false | false | Server | 
| retentionCheckIntervalInSeconds | Check between intervals to see if consumed ledgers need to be trimmed | 120 | false | Server | 
| maxNumPartitionsPerPartitionedTopic | The number of partitions per partitioned topic.<br />If try to create or update partitioned topics by exceeded number of partitions, then fail. | 0 | true | Server | 
| brokerInterceptorsDirectory | The directory to locate broker interceptors | ./interceptors | false | Server | 
| brokerInterceptors | List of broker interceptor to load, which is a list of broker interceptor names | [] | false | Server | 
| disableBrokerInterceptors | Enable or disable the broker interceptor, which is only used for testing for now | true | false | Server | 
| brokerEntryPayloadProcessors | List of interceptors for payload processing. | [] | false | Server | 
| zookeeperSessionExpiredPolicy | There are two policies to apply when broker metadata session expires: session expired happens, "shutdown" or "reconnect". <br /><br /> With "shutdown", the broker will be restarted.<br /><br /> With "reconnect", the broker will keep serving the topics, while attempting to recreate a new session. | reconnect | false |  | 
| topicFencingTimeoutSeconds | If a topic remains fenced for this number of seconds, it will be closed forcefully.<br /> If it is set to 0 or a negative number, the fenced topic will not be closed. | 0 | false | Server | 
| protocolHandlerDirectory | The directory to locate messaging protocol handlers | ./protocols | false | Protocols | 
| useSeparateThreadPoolForProtocolHandlers | Use a separate ThreadPool for each Protocol Handler | true | false | Protocols | 
| messagingProtocols | List of messaging protocols to load, which is a list of protocol names | [] | false | Protocols | 
| systemTopicEnabled | Enable or disable system topic. | true | false | Server | 
| systemTopicSchemaCompatibilityStrategy | The schema compatibility strategy to use for system topics | ALWAYS_COMPATIBLE | false | Schema | 
| topicLevelPoliciesEnabled | Enable or disable topic level policies, topic level policies depends on the system topic, please enable the system topic first. | true | false | Server | 
| brokerEntryMetadataInterceptors | List of interceptors for entry metadata. | [] | false | Server | 
| exposingBrokerEntryMetadataToClientEnabled | Enable or disable exposing broker entry metadata to client. | false | false | Server | 
| enableNamespaceIsolationUpdateOnTime | Enable namespaceIsolation policy update take effect ontime or not, if set to ture, then the related namespaces will be unloaded after reset policy to make it take effect. | false | false | Server | 
| strictBookieAffinityEnabled | Enable or disable strict bookie affinity. | false | false | Server | 
| tlsEnabled | Enable TLS | false | false | TLS | 
| tlsCertRefreshCheckDurationSec | Tls cert refresh duration in seconds (set 0 to check on every new connection) | 300 | false | TLS | 
| tlsCertificateFilePath | Path for the TLS certificate file | null | false | TLS | 
| tlsKeyFilePath | Path for the TLS private key file | null | false | TLS | 
| tlsTrustCertsFilePath | Path for the trusted TLS certificate file |  | false | TLS | 
| tlsAllowInsecureConnection | Accept untrusted TLS certificate from client | false | false | TLS | 
| tlsProtocols | Specify the tls protocols the broker will use to negotiate during TLS Handshake.<br /><br />Example:- [TLSv1.3, TLSv1.2] | [] | false | TLS | 
| tlsCiphers | Specify the tls cipher the broker will use to negotiate during TLS Handshake.<br /><br />Example:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256] | [] | false | TLS | 
| tlsRequireTrustedClientCertOnConnect | Specify whether Client certificates are required for TLS Reject.<br />the Connection if the Client Certificate is not trusted | false | false | TLS | 
| authenticationEnabled | Enable authentication | false | false | Authentication | 
| authenticationProviders | Authentication provider name list, which is a list of class names | [] | false | Authentication | 
| authenticationRefreshCheckSeconds | Interval of time for checking for expired authentication credentials | 60 | false | Authentication | 
| authorizationEnabled | Enforce authorization | false | false | Authorization | 
| authorizationProvider | Authorization provider fully qualified class-name | org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider | false | Authorization | 
| superUserRoles | Role names that are treated as `super-user`, meaning they will be able to do all admin operations and publish/consume from all topics | [] | true | Authorization | 
| proxyRoles | Role names that are treated as `proxy roles`. <br /><br />If the broker sees a request with role as proxyRoles - it will demand to see the original client role or certificate. | [] | false | Authorization | 
| authenticateOriginalAuthData | If this flag is set then the broker authenticates the original Auth data else it just accepts the originalPrincipal and authorizes it (if required) | false | false | Authorization | 
| authorizationAllowWildcardsMatching | Allow wildcard matching in authorization<br /><br />(wildcard matching only applicable if wildcard-char: * presents at first or last position eg: *.pulsar.service, pulsar.service.*) | false | false | Authorization | 
| brokerClientAuthenticationPlugin | Authentication settings of the broker itself. <br /><br />Used when the broker connects to other brokers, either in same or other clusters. Default uses plugin which disables authentication | org.apache.pulsar.client.impl.auth.AuthenticationDisabled | true | Authentication | 
| brokerClientAuthenticationParameters | Authentication parameters of the authentication plugin the broker is using to connect to other brokers |  | true | Authentication | 
| brokerClientTrustCertsFilePath | Path for the trusted TLS certificate file for outgoing connection to a server (broker) |  | false | Authentication | 
| anonymousUserRole | When this parameter is not empty, unauthenticated users perform as anonymousUserRole | null | false | Authorization | 
| httpMaxRequestSize | If \>0, it will reject all HTTP requests with bodies larged than the configured limit | -1 | false | HTTP | 
| disableHttpDebugMethods | If true, the broker will reject all HTTP requests using the TRACE and TRACK verbs.<br /> This setting may be necessary if the broker is deployed into an environment that uses http port<br /> scanning and flags web servers allowing the TRACE method as insecure. | false | false | HTTP | 
| httpRequestsLimitEnabled | Enable the enforcement of limits on the incoming HTTP requests | false | false | HTTP | 
| httpRequestsMaxPerSecond | Max HTTP requests per seconds allowed. The excess of requests will be rejected with HTTP code 429 (Too many requests) | 100.0 | false | HTTP | 
| saslJaasClientAllowedIds | This is a regexp, which limits the range of possible ids which can connect to the Broker using SASL.<br /> Default value is: ".*pulsar.*", so only clients whose id contains 'pulsar' are allowed to connect. | .*pulsar.* | false | SASL Authentication Provider | 
| saslJaasServerSectionName | Service Principal, for login context name. Default value is "PulsarBroker". | PulsarBroker | false | SASL Authentication Provider | 
| saslJaasServerRoleTokenSignerSecretPath | Path to file containing the secret to be used to SaslRoleTokenSigner<br />The secret can be specified like:<br />saslJaasServerRoleTokenSignerSecretPath=file:///my/saslRoleTokenSignerSecret.key. | null | false | SASL Authentication Provider | 
| kinitCommand | kerberos kinit command. | /usr/bin/kinit | false | SASL Authentication Provider | 
| bookkeeperMetadataServiceUri | Metadata service uri that bookkeeper is used for loading corresponding metadata driver and resolving its metadata service location | null | false | Storage (BookKeeper) | 
| bookkeeperClientAuthenticationPlugin | Authentication plugin to use when connecting to bookies | null | false | Storage (BookKeeper) | 
| bookkeeperClientAuthenticationParametersName | BookKeeper auth plugin implementation specifics parameters name and values | null | false | Storage (BookKeeper) | 
| bookkeeperClientAuthenticationParameters | Parameters for bookkeeper auth plugin | null | false | Storage (BookKeeper) | 
| bookkeeperClientTimeoutInSeconds | Timeout for BK add / read operations | 30 | false | Storage (BookKeeper) | 
| bookkeeperClientSpeculativeReadTimeoutInMillis | Speculative reads are initiated if a read request doesn't complete within a certain time Using a value of 0, is disabling the speculative reads | 0 | false | Storage (BookKeeper) | 
| bookkeeperNumberOfChannelsPerBookie | Number of channels per bookie | 16 | false | Storage (BookKeeper) | 
| bookkeeperUseV2WireProtocol | Use older Bookkeeper wire protocol with bookie | true | true | Storage (BookKeeper) | 
| bookkeeperClientHealthCheckEnabled | Enable bookies health check. <br /><br /> Bookies that have more than the configured number of failure within the interval will be quarantined for some time. During this period, new ledgers won't be created on these bookies | true | false | Storage (BookKeeper) | 
| bookkeeperClientHealthCheckIntervalSeconds | Bookies health check interval in seconds | 60 | false | Storage (BookKeeper) | 
| bookkeeperClientHealthCheckErrorThresholdPerInterval | Bookies health check error threshold per check interval | 5 | false | Storage (BookKeeper) | 
| bookkeeperClientHealthCheckQuarantineTimeInSeconds | Bookie health check quarantined time in seconds | 1800 | false | Storage (BookKeeper) | 
| bookkeeperClientQuarantineRatio | bookie quarantine ratio to avoid all clients quarantine the high pressure bookie servers at the same time | 1.0 | false | Storage (BookKeeper) | 
| bookkeeperClientRackawarePolicyEnabled | Enable rack-aware bookie selection policy. <br /><br />BK will chose bookies from different racks when forming a new bookie ensemble | true | false | Storage (BookKeeper) | 
| bookkeeperClientRegionawarePolicyEnabled | Enable region-aware bookie selection policy. <br /><br />BK will chose bookies from different regions and racks when forming a new bookie ensemble | false | false | Storage (BookKeeper) | 
| bookkeeperClientMinNumRacksPerWriteQuorum | Minimum number of racks per write quorum. <br /><br />BK rack-aware bookie selection policy will try to get bookies from at least 'bookkeeperClientMinNumRacksPerWriteQuorum' racks for a write quorum. | 2 | false | Storage (BookKeeper) | 
| bookkeeperClientEnforceMinNumRacksPerWriteQuorum | Enforces rack-aware bookie selection policy to pick bookies from 'bookkeeperClientMinNumRacksPerWriteQuorum' racks for  a writeQuorum. <br /><br />If BK can't find bookie then it would throw BKNotEnoughBookiesException instead of picking random one. | false | false | Storage (BookKeeper) | 
| bookkeeperClientReorderReadSequenceEnabled | Enable/disable reordering read sequence on reading entries | false | false | Storage (BookKeeper) | 
| bookkeeperClientIsolationGroups | Enable bookie isolation by specifying a list of bookie groups to choose from. <br /><br />Any bookie outside the specified groups will not be used by the broker | null | false | Storage (BookKeeper) | 
| bookkeeperClientSecondaryIsolationGroups | Enable bookie secondary-isolation group if bookkeeperClientIsolationGroups doesn't have enough bookie available. | null | false | Storage (BookKeeper) | 
| bookkeeperClientGetBookieInfoIntervalSeconds | Set the interval to periodically check bookie info | 86400 | false | Storage (BookKeeper) | 
| bookkeeperClientGetBookieInfoRetryIntervalSeconds | Set the interval to retry a failed bookie info lookup | 60 | false | Storage (BookKeeper) | 
| bookkeeperEnableStickyReads | Enable/disable having read operations for a ledger to be sticky to a single bookie.<br />If this flag is enabled, the client will use one single bookie (by preference) to read all entries for a ledger. | true | false | Storage (BookKeeper) | 
| bookkeeperTLSProviderFactoryClass | Set the client security provider factory class name. Default: org.apache.bookkeeper.tls.TLSContextFactory | org.apache.bookkeeper.tls.TLSContextFactory | false | Storage (BookKeeper) | 
| bookkeeperTLSClientAuthentication | Enable tls authentication with bookie | false | false | Storage (BookKeeper) | 
| bookkeeperTLSKeyFileType | Supported type: PEM, JKS, PKCS12. Default value: PEM | PEM | false | Storage (BookKeeper) | 
| bookkeeperTLSTrustCertTypes | Supported type: PEM, JKS, PKCS12. Default value: PEM | PEM | false | Storage (BookKeeper) | 
| bookkeeperTLSKeyStorePasswordPath | Path to file containing keystore password, if the client keystore is password protected. | null | false | Storage (BookKeeper) | 
| bookkeeperTLSTrustStorePasswordPath | Path to file containing truststore password, if the client truststore is password protected. | null | false | Storage (BookKeeper) | 
| bookkeeperTLSKeyFilePath | Path for the TLS private key file | null | false | Storage (BookKeeper) | 
| bookkeeperTLSCertificateFilePath | Path for the TLS certificate file | null | false | Storage (BookKeeper) | 
| bookkeeperTLSTrustCertsFilePath | Path for the trusted TLS certificate file | null | false | Storage (BookKeeper) | 
| bookkeeperTlsCertFilesRefreshDurationSeconds | Tls cert refresh duration at bookKeeper-client in seconds (0 to disable check) | 300 | false | Storage (BookKeeper) | 
| bookkeeperDiskWeightBasedPlacementEnabled | Enable/disable disk weight based placement. Default is false | false | false | Storage (BookKeeper) | 
| bookkeeperExplicitLacIntervalInMills | Set the interval to check the need for sending an explicit LAC | 0 | false | Storage (BookKeeper) | 
| bookkeeperClientExposeStatsToPrometheus | whether expose managed ledger client stats to prometheus | false | false | Storage (BookKeeper) | 
| bookkeeperClientThrottleValue | Throttle value for bookkeeper client | 0 | false | Storage (BookKeeper) | 
| bookkeeperClientNumWorkerThreads | Number of BookKeeper client worker threads. Default is Runtime.getRuntime().availableProcessors() | 8 | false | Storage (BookKeeper) | 
| bookkeeperClientNumIoThreads | Number of BookKeeper client IO threads. Default is Runtime.getRuntime().availableProcessors() * 2 | 16 | false | Storage (BookKeeper) | 
| bookkeeperClientSeparatedIoThreadsEnabled | Use separated IO threads for BookKeeper client. Default is false, which will use Pulsar IO threads | false | false | Storage (BookKeeper) | 
| managedLedgerDefaultEnsembleSize | Number of bookies to use when creating a ledger | 2 | false | Storage (Managed Ledger) | 
| managedLedgerDefaultWriteQuorum | Number of copies to store for each message | 2 | false | Storage (Managed Ledger) | 
| managedLedgerDefaultAckQuorum | Number of guaranteed copies (acks to wait before write is complete) | 2 | false | Storage (Managed Ledger) | 
| managedLedgerCursorPositionFlushSeconds | How frequently to flush the cursor positions that were accumulated due to rate limiting. (seconds). Default is 60 seconds | 60 | false | Storage (Managed Ledger) | 
| managedLedgerStatsPeriodSeconds | How frequently to refresh the stats. (seconds). Default is 60 seconds | 60 | false | Storage (Managed Ledger) | 
| managedLedgerDigestType | Default type of checksum to use when writing to BookKeeper. <br /><br />Default is `CRC32C`. Other possible options are `CRC32`, `MAC` or `DUMMY` (no checksum). | CRC32C | false | Storage (Managed Ledger) | 
| managedLedgerPassword | Default  password to use when writing to BookKeeper. <br /><br />Default is ``. |  | false | Storage (Managed Ledger) | 
| managedLedgerMaxEnsembleSize | Max number of bookies to use when creating a ledger | 5 | false | Storage (Managed Ledger) | 
| managedLedgerMaxWriteQuorum | Max number of copies to store for each message | 5 | false | Storage (Managed Ledger) | 
| managedLedgerMaxAckQuorum | Max number of guaranteed copies (acks to wait before write is complete) | 5 | false | Storage (Managed Ledger) | 
| managedLedgerCacheSizeMB | Amount of memory to use for caching data payload in managed ledger. <br /><br />This memory is allocated from JVM direct memory and it's shared across all the topics running in the same broker. By default, uses 1/5th of available direct memory | 819 | true | Storage (Managed Ledger) | 
| managedLedgerCacheCopyEntries | Whether we should make a copy of the entry payloads when inserting in cache | false | false | Storage (Managed Ledger) | 
| managedLedgerCacheEvictionWatermark | Threshold to which bring down the cache level when eviction is triggered | 0.9 | true | Storage (Managed Ledger) | 
| managedLedgerCacheEvictionFrequency | Configure the cache eviction frequency for the managed ledger cache. Default is 100/s | 100.0 | false | Storage (Managed Ledger) | 
| managedLedgerCacheEvictionTimeThresholdMillis | All entries that have stayed in cache for more than the configured time, will be evicted | 1000 | true | Storage (Managed Ledger) | 
| managedLedgerCursorBackloggedThreshold | Configure the threshold (in number of entries) from where a cursor should be considered 'backlogged' and thus should be set as inactive. | 1000 | false | Storage (Managed Ledger) | 
| managedLedgerDefaultMarkDeleteRateLimit | Rate limit the amount of writes per second generated by consumer acking the messages | 1.0 | false | Storage (Managed Ledger) | 
| allowAutoTopicCreation | Allow automated creation of topics if set to true (default value). | true | true | Storage (Managed Ledger) | 
| allowAutoTopicCreationType | The type of topic that is allowed to be automatically created.(partitioned/non-partitioned) | non-partitioned | true | Storage (Managed Ledger) | 
| allowAutoSubscriptionCreation | Allow automated creation of subscriptions if set to true (default value). | true | true | Storage (Managed Ledger) | 
| defaultNumPartitions | The number of partitioned topics that is allowed to be automatically createdif allowAutoTopicCreationType is partitioned. | 1 | true | Storage (Managed Ledger) | 
| managedLedgerStorageClassName | The class of the managed ledger storage | org.apache.pulsar.broker.ManagedLedgerClientFactory | false | Storage (Managed Ledger) | 
| managedLedgerNumSchedulerThreads | Number of threads to be used for managed ledger scheduled tasks | 8 | false | Storage (Managed Ledger) | 
| managedLedgerMaxEntriesPerLedger | Max number of entries to append to a ledger before triggering a rollover.<br /><br />A ledger rollover is triggered after the min rollover time has passed and one of the following conditions is true: the max rollover time has been reached, the max entries have been written to the ledger, or the max ledger size has been written to the ledger | 50000 | false | Storage (Managed Ledger) | 
| managedLedgerMinLedgerRolloverTimeMinutes | Minimum time between ledger rollover for a topic | 10 | false | Storage (Managed Ledger) | 
| managedLedgerMaxLedgerRolloverTimeMinutes | Maximum time before forcing a ledger rollover for a topic | 240 | false | Storage (Managed Ledger) | 
| managedLedgerMaxSizePerLedgerMbytes | Maximum ledger size before triggering a rollover for a topic (MB) | 2048 | false | Storage (Managed Ledger) | 
| managedLedgerOffloadDeletionLagMs | Delay between a ledger being successfully offloaded to long term storage, and the ledger being deleted from bookkeeper | 14400000 | false | Storage (Ledger Offloading) | 
| managedLedgerOffloadAutoTriggerSizeThresholdBytes | The number of bytes before triggering automatic offload to long term storage | -1 | false | Storage (Ledger Offloading) | 
| managedLedgerCursorMaxEntriesPerLedger | Max number of entries to append to a cursor ledger | 50000 | false | Storage (Managed Ledger) | 
| managedLedgerCursorRolloverTimeInSeconds | Max time before triggering a rollover on a cursor ledger | 14400 | false | Storage (Managed Ledger) | 
| managedLedgerMaxUnackedRangesToPersist | Max number of `acknowledgment holes` that are going to be persistently stored.<br /><br />When acknowledging out of order, a consumer will leave holes that are supposed to be quickly filled by acking all the messages. The information of which messages are acknowledged is persisted by compressing in `ranges` of messages that were acknowledged. After the max number of ranges is reached, the information will only be tracked in memory and messages will be redelivered in case of crashes. | 10000 | false | Storage (Managed Ledger) | 
| persistentUnackedRangesWithMultipleEntriesEnabled | If enabled, the maximum "acknowledgment holes" will not be limited and "acknowledgment holes" are stored in multiple entries. | false | false | Storage (Managed Ledger) | 
| managedLedgerMaxUnackedRangesToPersistInZooKeeper | Max number of `acknowledgment holes` that can be stored in Zookeeper.<br /><br />If number of unack message range is higher than this limit then broker will persist unacked ranges into bookkeeper to avoid additional data overhead into zookeeper. | -1 | false | Storage (Managed Ledger) | 
| managedLedgerMaxUnackedRangesToPersistInMetadataStore | Max number of `acknowledgment holes` that can be stored in MetadataStore.<br /><br />If number of unack message range is higher than this limit then broker will persist unacked ranges into bookkeeper to avoid additional data overhead into MetadataStore. | 1000 | false | Storage (Managed Ledger) | 
| managedLedgerUnackedRangesOpenCacheSetEnabled | Use Open Range-Set to cache unacked messages (it is memory efficient but it can take more cpu) | true | false | Storage (Ledger Offloading) | 
| autoSkipNonRecoverableData | Skip reading non-recoverable/unreadable data-ledger under managed-ledger's list.<br /><br /> It helps when data-ledgers gets corrupted at bookkeeper and managed-cursor is stuck at that ledger. | false | true | Storage (Managed Ledger) | 
| managedLedgerMetadataOperationsTimeoutSeconds | operation timeout while updating managed-ledger metadata. | 60 | false | Storage (Managed Ledger) | 
| managedLedgerReadEntryTimeoutSeconds | Read entries timeout when broker tries to read messages from bookkeeper (0 to disable it) | 0 | false | Storage (Managed Ledger) | 
| managedLedgerAddEntryTimeoutSeconds | Add entry timeout when broker tries to publish message to bookkeeper.(0 to disable it) | 0 | false | Storage (Managed Ledger) | 
| managedLedgerPrometheusStatsLatencyRolloverSeconds | Managed ledger prometheus stats latency rollover seconds | 60 | false | Storage (Managed Ledger) | 
| managedLedgerTraceTaskExecution | Whether trace managed ledger task execution time | true | true | Storage (Managed Ledger) | 
| managedLedgerNewEntriesCheckDelayInMillis | New entries check delay for the cursor under the managed ledger. <br />If no new messages in the topic, the cursor will try to check again after the delay time. <br />For consumption latency sensitive scenario, can set to a smaller value or set to 0.<br />Of course, this may degrade consumption throughput. Default is 10ms. | 10 | false | Storage (Managed Ledger) | 
| managedLedgerDataReadPriority | Read priority when ledgers exists in both bookkeeper and the second layer storage. | tiered-storage-first | false | Storage (Managed Ledger) | 
| managedLedgerInfoCompressionType | ManagedLedgerInfo compression type, option values (NONE, LZ4, ZLIB, ZSTD, SNAPPY). <br />If value is invalid or NONE, then save the ManagedLedgerInfo bytes data directly. | NONE | false | Storage (Managed Ledger) | 
| managedCursorInfoCompressionType | ManagedCursorInfo compression type, option values (NONE, LZ4, ZLIB, ZSTD, SNAPPY). <br />If value is NONE, then save the ManagedCursorInfo bytes data directly. | NONE | false | Storage (Managed Ledger) | 
| loadBalancerEnabled | Enable load balancer | true | false | Load Balancer | 
| loadBalancerPlacementStrategy | load placement strategy[weightedRandomSelection/leastLoadedServer] (only used by SimpleLoadManagerImpl) | leastLoadedServer | false | Load Balancer | 
| loadBalancerLoadSheddingStrategy | load balance load shedding strategy (It requires broker restart if value is changed using dynamic config). Default is ThresholdShedder since 2.10.0 | org.apache.pulsar.broker.loadbalance.impl.ThresholdShedder | true | Load Balancer | 
| loadBalancerLoadPlacementStrategy | load balance placement strategy | org.apache.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate | false | Load Balancer | 
| loadBalancerReportUpdateThresholdPercentage | Percentage of change to trigger load report update | 10 | true | Load Balancer | 
| loadBalancerReportUpdateMaxIntervalMinutes | maximum interval to update load report | 15 | true | Load Balancer | 
| loadBalancerHostUsageCheckIntervalMinutes | Frequency of report to collect, in minutes | 1 | true | Load Balancer | 
| loadBalancerSheddingEnabled | Enable/disable automatic bundle unloading for load-shedding | true | true | Load Balancer | 
| loadBalancerSheddingIntervalMinutes | Load shedding interval. <br /><br />Broker periodically checks whether some traffic should be offload from some over-loaded broker to other under-loaded brokers | 1 | true | Load Balancer | 
| loadBalancerDistributeBundlesEvenlyEnabled | enable/disable distribute bundles evenly | true | true | Load Balancer | 
| loadBalancerSheddingGracePeriodMinutes | Prevent the same topics to be shed and moved to other broker more than once within this timeframe | 30 | true | Load Balancer | 
| loadBalancerBrokerUnderloadedThresholdPercentage | Usage threshold to determine a broker as under-loaded (only used by SimpleLoadManagerImpl) | 50 | false | Load Balancer | 
| loadBalancerBrokerMaxTopics | Usage threshold to allocate max number of topics to broker | 50000 | true | Load Balancer | 
| loadBalancerBrokerOverloadedThresholdPercentage | Usage threshold to determine a broker as over-loaded | 85 | true | Load Balancer | 
| loadBalancerBrokerThresholdShedderPercentage | Usage threshold to determine a broker whether to start threshold shedder | 10 | true | Load Balancer | 
| loadBalancerAverageResourceUsageDifferenceThresholdPercentage | Average resource usage difference threshold to determine a broker whether to be a best candidate in LeastResourceUsageWithWeight.(eg: broker1 with 10% resource usage with weight and broker2 with 30% and broker3 with 80% will have 40% average resource usage. The placement strategy can select broker1 and broker2 as best candidates.) | 10 | true | Load Balancer | 
| loadBalancerMsgRateDifferenceShedderThreshold | Message-rate percentage threshold between highest and least loaded brokers for uniform load shedding. (eg: broker1 with 50K msgRate and broker2 with 30K msgRate will have 66% msgRate difference and load balancer can unload bundles from broker-1 to broker-2) | 50.0 | true | Load Balancer | 
| loadBalancerMsgThroughputMultiplierDifferenceShedderThreshold | Message-throughput threshold between highest and least loaded brokers for uniform load shedding. (eg: broker1 with 450MB msgRate and broker2 with 100MB msgRate will have 4.5 times msgThroughout difference and load balancer can unload bundles from broker-1 to broker-2) | 4.0 | true | Load Balancer | 
| loadBalancerHistoryResourcePercentage | Resource history Usage Percentage When adding new resource usage info | 0.9 | true | Load Balancer | 
| loadBalancerBandwithInResourceWeight | BandwithIn Resource Usage Weight | 1.0 | true | Load Balancer | 
| loadBalancerBandwithOutResourceWeight | BandwithOut Resource Usage Weight | 1.0 | true | Load Balancer | 
| loadBalancerCPUResourceWeight | CPU Resource Usage Weight | 1.0 | true | Load Balancer | 
| loadBalancerMemoryResourceWeight | Memory Resource Usage Weight | 1.0 | true | Load Balancer | 
| loadBalancerDirectMemoryResourceWeight | Direct Memory Resource Usage Weight | 1.0 | true | Load Balancer | 
| loadBalancerBundleUnloadMinThroughputThreshold | Bundle unload minimum throughput threshold (MB) | 10.0 | true | Load Balancer | 
| loadBalancerResourceQuotaUpdateIntervalMinutes | Interval to flush dynamic resource quota to ZooKeeper | 15 | false | Load Balancer | 
| loadBalancerBrokerComfortLoadLevelPercentage | Usage threshold to determine a broker is having just right level of load (only used by SimpleLoadManagerImpl) | 65 | false | Load Balancer | 
| loadBalancerAutoBundleSplitEnabled | enable/disable automatic namespace bundle split | true | true | Load Balancer | 
| loadBalancerAutoUnloadSplitBundlesEnabled | enable/disable automatic unloading of split bundles | true | true | Load Balancer | 
| loadBalancerNamespaceBundleMaxTopics | maximum topics in a bundle, otherwise bundle split will be triggered | 1000 | true | Load Balancer | 
| loadBalancerNamespaceBundleMaxSessions | maximum sessions (producers + consumers) in a bundle, otherwise bundle split will be triggered(disable threshold check with value -1) | 1000 | true | Load Balancer | 
| loadBalancerNamespaceBundleMaxMsgRate | maximum msgRate (in + out) in a bundle, otherwise bundle split will be triggered | 30000 | true | Load Balancer | 
| loadBalancerNamespaceBundleMaxBandwidthMbytes | maximum bandwidth (in + out) in a bundle, otherwise bundle split will be triggered | 100 | true | Load Balancer | 
| loadBalancerNamespaceMaximumBundles | maximum number of bundles in a namespace | 128 | true | Load Balancer | 
| loadManagerClassName | Name of load manager to use | org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl | true | Load Balancer | 
| supportedNamespaceBundleSplitAlgorithms | Supported algorithms name for namespace bundle split | [range_equally_divide, topic_count_equally_divide, specified_positions_divide] | true | Load Balancer | 
| defaultNamespaceBundleSplitAlgorithm | Default algorithm name for namespace bundle split | range_equally_divide | true | Load Balancer | 
| loadBalancerOverrideBrokerNicSpeedGbps | Option to override the auto-detected network interfaces max speed | Optional.empty | false | Load Balancer | 
| namespaceBundleUnloadingTimeoutMs | Time to wait for the unloading of a namespace bundle | 60000 | true | Load Balancer | 
| replicationMetricsEnabled | Enable replication metrics | true | false | Replication | 
| replicationConnectionsPerBroker | Max number of connections to open for each broker in a remote cluster.<br /><br />More connections host-to-host lead to better throughput over high-latency links | 16 | false | Replication | 
| replicatorPrefix | replicator prefix used for replicator producer name and cursor name | pulsar.repl | false | Replication | 
| replicationProducerQueueSize | Replicator producer queue size. When dynamically modified, it only takes effect for the newly added replicators | 1000 | true | Replication | 
| replicationPolicyCheckDurationSeconds | Duration to check replication policy to avoid replicator inconsistency due to missing ZooKeeper watch (disable with value 0) | 600 | false | Replication | 
| replicationTlsEnabled | @deprecated - Use brokerClientTlsEnabled instead. | false | false | Replication | 
| brokerClientTlsEnabled | Enable TLS when talking with other brokers in the same cluster (admin operation) or different clusters (replication) | false | true | Replication | 
| defaultRetentionTimeInMinutes | Default message retention time | 0 | false | Policies | 
| defaultRetentionSizeInMB | Default retention size | 0 | false | Policies | 
| keepAliveIntervalSeconds | How often to check pulsar connection is still alive | 30 | false | Server | 
| brokerServicePurgeInactiveFrequencyInSeconds | How often broker checks for inactive topics to be deleted (topics with no subscriptions and no one connected) Deprecated in favor of using `brokerDeleteInactiveTopicsFrequencySeconds` | 60 | false | Policies | 
| bootstrapNamespaces | A comma-separated list of namespaces to bootstrap | [] | false | Server | 
| preferLaterVersions | If true, (and ModularLoadManagerImpl is being used), the load manager will attempt to use only brokers running the latest software version (to minimize impact to bundles) | false | true | Server | 
| brokerServiceCompactionMonitorIntervalInSeconds | Interval between checks to see if topics with compaction policies need to be compacted | 60 | false | Server | 
| brokerServiceCompactionThresholdInBytes | The estimated backlog size is greater than this threshold, compression will be triggered.<br />Using a value of 0, is disabling compression check. | 0 | false | Server | 
| brokerServiceCompactionPhaseOneLoopTimeInSeconds | Timeout for the compaction phase one loop, If the execution time of the compaction phase one loop exceeds this time, the compaction will not proceed. | 30 | false | Server | 
| isSchemaValidationEnforced | Enforce schema validation on following cases:<br /><br /> - if a producer without a schema attempts to produce to a topic with schema, the producer will be<br />   failed to connect. PLEASE be carefully on using this, since non-java clients don't support schema.<br />   if you enable this setting, it will cause non-java clients failed to produce. | false | false | Schema | 
| schemaRegistryStorageClassName | The schema storage implementation used by this broker | org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorageFactory | false | Schema | 
| schemaRegistryCompatibilityCheckers | The list compatibility checkers to be used in schema registry | [org.apache.pulsar.broker.service.schema.ProtobufNativeSchemaCompatibilityCheck, org.apache.pulsar.broker.service.schema.JsonSchemaCompatibilityCheck, org.apache.pulsar.broker.service.schema.AvroSchemaCompatibilityCheck] | false | Schema | 
| schemaCompatibilityStrategy | The schema compatibility strategy in broker level | FULL | false | Schema | 
| webSocketNumIoThreads | Number of IO threads in Pulsar Client used in WebSocket proxy | 8 | false | WebSocket | 
| webSocketNumServiceThreads | Number of threads used by Websocket service | 20 | false | WebSocket | 
| webSocketConnectionsPerBroker | Number of connections per Broker in Pulsar Client used in WebSocket proxy | 8 | false | WebSocket | 
| webSocketSessionIdleTimeoutMillis | Time in milliseconds that idle WebSocket session times out | 300000 | false | WebSocket | 
| webSocketMaxTextFrameSize | The maximum size of a text message during parsing in WebSocket proxy. | 1048576 | false | WebSocket | 
| authenticateMetricsEndpoint | Whether the '/metrics' endpoint requires authentication. Defaults to false.'authenticationEnabled' must also be set for this to take effect. | false | false | Metrics | 
| exposeTopicLevelMetricsInPrometheus | If true, export topic level metrics otherwise namespace level | true | false | Metrics | 
| metricsBufferResponse | If true, export buffered metrics | false | false | Metrics | 
| exposeConsumerLevelMetricsInPrometheus | If true, export consumer level metrics otherwise namespace level | false | false | Metrics | 
| exposeProducerLevelMetricsInPrometheus | If true, export producer level metrics otherwise namespace level | false | false | Metrics | 
| exposeManagedLedgerMetricsInPrometheus | If true, export managed ledger metrics (aggregated by namespace) | true | false | Metrics | 
| exposeManagedCursorMetricsInPrometheus | If true, export managed cursor metrics | false | false | Metrics | 
| jvmGCMetricsLoggerClassName | Classname of Pluggable JVM GC metrics logger that can log GC specific metrics | null | false | Metrics | 
| exposePreciseBacklogInPrometheus | Enable expose the precise backlog stats.<br /> Set false to use published counter and consumed counter to calculate,<br /> this would be more efficient but may be inaccurate. Default is false. | false | false | Metrics | 
| metricsServletTimeoutMs | Time in milliseconds that metrics endpoint would time out. Default is 30s.<br /> Increase it if there are a lot of topics to expose topic-level metrics.<br /> Set it to 0 to disable timeout. | 30000 | false | Metrics | 
| exposeSubscriptionBacklogSizeInPrometheus | Enable expose the backlog size for each subscription when generating stats.<br /> Locking is used for fetching the status so default to false. | false | false | Metrics | 
| splitTopicAndPartitionLabelInPrometheus | Enable splitting topic and partition label in Prometheus.<br /> If enabled, a topic name will split into 2 parts, one is topic name without partition index,<br /> another one is partition index, e.g. (topic=xxx, partition=0).<br /> If the topic is a non-partitioned topic, -1 will be used for the partition index.<br /> If disabled, one label to represent the topic and partition, e.g. (topic=xxx-partition-0)<br /> Default is false. | false | false | Metrics | 
| exposeBundlesMetricsInPrometheus | Enable expose the broker bundles metrics. | false | false | Metrics | 
| functionsWorkerEnabled | Flag indicates enabling or disabling function worker on brokers | false | false | Functions | 
| functionsWorkerServiceNarPackage | The nar package for the function worker service |  | false | Functions | 
| functionsWorkerEnablePackageManagement | Flag indicates enabling or disabling function worker using unified PackageManagement service. | false | false | Functions | 
| exposePublisherStats | If true, export publisher stats when returning topics stats from the admin rest api | true | false | Metrics | 
| statsUpdateFrequencyInSecs | Stats update frequency in seconds | 60 | false | Metrics | 
| statsUpdateInitialDelayInSecs | Stats update initial delay in seconds | 60 | false | Metrics | 
| aggregatePublisherStatsByProducerName | If true, aggregate publisher stats of PartitionedTopicStats by producerName | false | false | Metrics | 
| offloadersDirectory | The directory to locate offloaders | ./offloaders | false | Storage (Ledger Offloading) | 
| managedLedgerOffloadDriver | Driver to use to offload old data to long term storage | null | false | Storage (Ledger Offloading) | 
| managedLedgerOffloadMaxThreads | Maximum number of thread pool threads for ledger offloading | 2 | false | Storage (Ledger Offloading) | 
| narExtractionDirectory | The directory where nar Extraction of offloaders happens | /var/folders/0y/136crjnx0sb33_71mj2b33nh0000gn/T/ | false | Storage (Ledger Offloading) | 
| managedLedgerOffloadPrefetchRounds | Maximum prefetch rounds for ledger reading for offloading | 1 | false | Storage (Ledger Offloading) | 
| managedLedgerInactiveLedgerRolloverTimeSeconds | Time to rollover ledger for inactive topic (duration without any publish on that topic). Disable rollover with value 0 (Default value 0) | 0 | true | Storage (Managed Ledger) | 
| cacheEvictionByMarkDeletedPosition | Evicting cache data by the slowest markDeletedPosition or readPosition. The default is to evict through readPosition. | false | false | Storage (Managed Ledger) | 
| transactionCoordinatorEnabled | Enable transaction coordinator in broker | false | false | Transaction | 
| transactionMetadataStoreProviderClassName | Class name for transaction metadata store provider | org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStoreProvider | false | Transaction | 
| transactionBufferProviderClassName | Class name for transaction buffer provider | org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferProvider | false | Transaction | 
| transactionPendingAckStoreProviderClassName | Class name for transaction pending ack store provider | org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStoreProvider | false | Transaction | 
| numTransactionReplayThreadPoolSize | Number of threads to use for pulsar transaction replay PendingAckStore or TransactionBuffer.Default is 5 | 8 | false | Transaction | 
| transactionBufferSnapshotMaxTransactionCount | Transaction buffer take snapshot transaction count | 1000 | false | Transaction | 
| transactionBufferSnapshotMinTimeInMillis | Transaction buffer take snapshot min interval time | 5000 | false | Transaction | 
| transactionBufferClientMaxConcurrentRequests | The max concurrent requests for transaction buffer client. | 1000 | false | Transaction | 
| transactionBufferClientOperationTimeoutInMills | The transaction buffer client's operation timeout in milliseconds. | 3000 | false | Transaction | 
| maxActiveTransactionsPerCoordinator | The max active transactions per transaction coordinator, default value 0 indicates no limit. | 0 | false | Transaction | 
| transactionPendingAckLogIndexMinLag | MLPendingAckStore maintain a ConcurrentSkipListMap pendingAckLogIndex`,it store the position in pendingAckStore as value and save a position used to determinewhether the previous data can be cleaned up as a key.transactionPendingAckLogIndexMinLag is used to configure the minimum lag between indexes | 500 | false | Transaction | 
| tlsEnabledWithKeyStore | Enable TLS with KeyStore type configuration in broker | false | false | KeyStoreTLS | 
| tlsProvider | Specify the TLS provider for the broker service: <br />When using TLS authentication with CACert, the valid value is either OPENSSL or JDK.<br />When using TLS authentication with KeyStore, available values can be SunJSSE, Conscrypt and etc. | null | false | KeyStoreTLS | 
| tlsKeyStoreType | TLS KeyStore type configuration in broker: JKS, PKCS12 | JKS | false | KeyStoreTLS | 
| tlsKeyStore | TLS KeyStore path in broker | null | false | KeyStoreTLS | 
| tlsKeyStorePassword | TLS KeyStore password for broker | null | false | KeyStoreTLS | 
| tlsTrustStoreType | TLS TrustStore type configuration in broker: JKS, PKCS12 | JKS | false | KeyStoreTLS | 
| tlsTrustStore | TLS TrustStore path in broker | null | false | KeyStoreTLS | 
| tlsTrustStorePassword | TLS TrustStore password for broker, null means empty password. | null | false | KeyStoreTLS | 
| brokerClientTlsEnabledWithKeyStore | Whether internal client use KeyStore type to authenticate with other Pulsar brokers | false | false | KeyStoreTLS | 
| brokerClientSslProvider | The TLS Provider used by internal client to authenticate with other Pulsar brokers | null | false | KeyStoreTLS | 
| brokerClientTlsTrustStoreType | TLS TrustStore type configuration for internal client: JKS, PKCS12  used by the internal client to authenticate with Pulsar brokers | JKS | false | KeyStoreTLS | 
| brokerClientTlsTrustStore | TLS TrustStore path for internal client,  used by the internal client to authenticate with Pulsar brokers | null | false | KeyStoreTLS | 
| brokerClientTlsTrustStorePassword | TLS TrustStore password for internal client,  used by the internal client to authenticate with Pulsar brokers | null | false | KeyStoreTLS | 
| brokerClientTlsCiphers | Specify the tls cipher the internal client will use to negotiate during TLS Handshake (a comma-separated list of ciphers).<br /><br />Examples:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256].<br /> used by the internal client to authenticate with Pulsar brokers | [] | false | KeyStoreTLS | 
| brokerClientTlsProtocols | Specify the tls protocols the broker will use to negotiate during TLS handshake (a comma-separated list of protocol names).<br /><br />Examples:- [TLSv1.3, TLSv1.2] <br /> used by the internal client to authenticate with Pulsar brokers | [] | false | KeyStoreTLS | 
| enablePackagesManagement | Enable the packages management service or not | false | false | Packages Management | 
| packagesManagementStorageProvider | The packages management service storage service provider | org.apache.pulsar.packages.management.storage.bookkeeper.BookKeeperPackagesStorageProvider | false | Packages Management | 
| packagesReplicas | When the packages storage provider is bookkeeper, you can use this configuration to<br />control the number of replicas for storing the package | 1 | false | Packages Management | 
| packagesManagementLedgerRootPath | The bookkeeper ledger root path | /ledgers | false | Packages Management | 
| additionalServletDirectory | The directory to locate broker additional servlet | ./brokerAdditionalServlet | false | Broker Plugin | 
| additionalServlets | List of broker additional servlet to load, which is a list of broker additional servlet names | [] | false | Broker Plugin | 

