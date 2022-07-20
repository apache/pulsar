# Standalone

## Optional

### authenticateOriginalAuthData

If this flag is set to `true`, the broker authenticates the original Auth data; else it just accepts the originalPrincipal and authorizes it (if required).

**Default**: false

### metadataStoreUrl

The quorum connection string for local metadata store

**Default**:

### metadataStoreCacheExpirySeconds

**Default**: Metadata store cache expiry time in seconds

**Default**: 300

### configurationMetadataStoreUrl

Configuration store connection string (as a comma-separated list)

**Default**:

### brokerServicePort

The port on which the standalone broker listens for connections

**Default**: 6650

### webServicePort

The port used by the standalone broker for HTTP requests

**Default**: 8080

### webServiceTlsProvider

The TLS provider for the web service. Available values: `SunJSSE`, `Conscrypt`, and so on.

**Default**: Conscrypt

### bindAddress

The hostname or IP address on which the standalone service binds

**Default**: 0.0.0.0

### bindAddresses

Additional Hostname or IP addresses the service binds on: `listener_name:scheme://host:port,...`.

**Default**:

### advertisedAddress

The hostname or IP address that the standalone service advertises to the outside world. If not set, the value of `InetAddress.getLocalHost().getHostName()` is used.

**Default**:

### numAcceptorThreads

Number of threads to use for Netty Acceptor

**Default**: 1

### numIOThreads

Number of threads to use for Netty IO

**Default**: 2 \* Runtime.getRuntime().availableProcessors()

### numHttpServerThreads

Number of threads to use for HTTP requests processing

**Default**: 2 \* Runtime.getRuntime().availableProcessors()

### isRunningStandalone

This flag controls features that are meant to be used when running in standalone mode.

**Default**: N/A

### clusterName

The name of the cluster that this broker belongs to.

**Default**: standalone

### failureDomainsEnabled

Enable cluster's failure-domain which can distribute brokers into logical region.

**Default**: false

### metadataStoreSessionTimeoutMillis

Metadata store session timeout, in milliseconds.

**Default**: 30000

### metadataStoreOperationTimeoutSeconds

Metadata store operation timeout in seconds.

**Default**: 30

### brokerShutdownTimeoutMs

The time to wait for graceful broker shutdown. After this time elapses, the process will be killed.

**Default**: 60000

### skipBrokerShutdownOnOOM

Flag to skip broker shutdown when broker handles Out of memory error.

**Default**: false

### backlogQuotaCheckEnabled

Enable the backlog quota check, which enforces a specified action when the quota is reached.

**Default**: true

### backlogQuotaCheckIntervalInSeconds

How often to check for topics that have reached the backlog quota.

**Default**: 60

### backlogQuotaDefaultLimitBytes

The default per-topic backlog quota limit. Being less than 0 means no limitation. By default, it is -1.

**Default**: -1

### ttlDurationDefaultInSeconds

The default Time to Live (TTL) for namespaces if the TTL is not configured at namespace policies. When the value is set to `0`, TTL is disabled. By default, TTL is disabled.

**Default**: 0

### brokerDeleteInactiveTopicsEnabled

Enable the deletion of inactive topics. If topics are not consumed for some while, these inactive topics might be cleaned up. Deleting inactive topics is enabled by default. The default period is 1 minute.

**Default**: true

### brokerDeleteInactiveTopicsFrequencySeconds

How often to check for inactive topics, in seconds.

**Default**: 60

### maxPendingPublishRequestsPerConnection

Maximum pending publish requests per connection to avoid keeping large number of pending requests in memory

**Default**: 1000

### messageExpiryCheckIntervalInMinutes

How often to proactively check and purged expired messages.

**Default**: 5

### activeConsumerFailoverDelayTimeMillis

How long to delay rewinding cursor and dispatching messages when active consumer is changed.

**Default**: 1000

### subscriptionExpirationTimeMinutes

How long to delete inactive subscriptions from last consumption. When it is set to 0, inactive subscriptions are not deleted automatically

**Default**: 0

### subscriptionRedeliveryTrackerEnabled

Enable subscription message redelivery tracker to send redelivery count to consumer.

**Default**: true

### subscriptionKeySharedUseConsistentHashing

In Key\*Shared subscription type, with default AUTO_SPLIT mode, use splitting ranges or consistent hashing to reassign keys to new consumers.

**Default**: false

### subscriptionKeySharedConsistentHashingReplicaPoints

In Key_Shared subscription type, the number of points in the consistent-hashing ring. The greater the number, the more equal the assignment of keys to consumers.

**Default**: 100

### subscriptionExpiryCheckIntervalInMinutes

How frequently to proactively check and purge expired subscription

**Default**: 5

### brokerDeduplicationEnabled

Set the default behavior for message deduplication in the broker. This can be overridden per-namespace. If it is enabled, the broker rejects messages that are already stored in the topic.

**Default**: false

### brokerDeduplicationMaxNumberOfProducers

Maximum number of producer information that it's going to be persisted for deduplication purposes

**Default**: 10000

### brokerDeduplicationEntriesInterval

Number of entries after which a deduplication information snapshot is taken. A greater interval leads to less snapshots being taken though it would increase the topic recovery time, when the entries published after the snapshot need to be replayed.

**Default**: 1000

### brokerDeduplicationProducerInactivityTimeoutMinutes

The time of inactivity (in minutes) after which the broker discards deduplication information related to a disconnected producer.

**Default**: 360

### defaultNumberOfNamespaceBundles

When a namespace is created without specifying the number of bundles, this value is used as the default setting.

**Default**: 4

### clientLibraryVersionCheckEnabled

Enable checks for minimum allowed client library version.

**Default**: false

### clientLibraryVersionCheckAllowUnversioned

Allow client libraries with no version information

**Default**: true

### statusFilePath

The path for the file used to determine the rotation status for the broker when responding to service discovery health checks

**Default**: /usr/local/apache/htdocs

### maxUnackedMessagesPerConsumer

The maximum number of unacknowledged messages allowed to be received by consumers on a shared subscription. The broker will stop sending messages to a consumer once this limit is reached or until the consumer begins acknowledging messages. A value of 0 disables the unacked message limit check and thus allows consumers to receive messages without any restrictions.

**Default**: 50000

### maxUnackedMessagesPerSubscription

The same as above, except per subscription rather than per consumer.

**Default**: 200000

### maxUnackedMessagesPerBroker

Maximum number of unacknowledged messages allowed per broker. Once this limit reaches, the broker stops dispatching messages to all shared subscriptions which has a higher number of unacknowledged messages until subscriptions start acknowledging messages back and unacknowledged messages count reaches to limit/2. When the value is set to 0, unacknowledged message limit check is disabled and broker does not block dispatchers.

**Default**: 0

### maxUnackedMessagesPerSubscriptionOnBrokerBlocked

Once the broker reaches maxUnackedMessagesPerBroker limit, it blocks subscriptions which have higher unacknowledged messages than this percentage limit and subscription does not receive any new messages until that subscription acknowledges messages back.

**Default**: 0.16

### unblockStuckSubscriptionEnabled

Broker periodically checks if subscription is stuck and unblock if flag is enabled.

**Default**: false

### topicPublisherThrottlingTickTimeMillis

Tick time to schedule task that checks topic publish rate limiting across all topics. A lower value can improve accuracy while throttling publish but it uses more CPU to perform frequent check. (Disable publish throttling with value 0)

**Default**: 10

### brokerPublisherThrottlingTickTimeMillis

Tick time to schedule task that checks broker publish rate limiting across all topics. A lower value can improve accuracy while throttling publish but it uses more CPU to perform frequent check. When the value is set to 0, publish throttling is disabled.

**Default**: 50

### brokerPublisherThrottlingMaxMessageRate

Maximum rate (in 1 second) of messages allowed to publish for a broker if the message rate limiting is enabled. When the value is set to 0, message rate limiting is disabled.

**Default**: 0

### brokerPublisherThrottlingMaxByteRate

Maximum rate (in 1 second) of bytes allowed to publish for a broker if the byte rate limiting is enabled. When the value is set to 0, the byte rate limiting is disabled.

**Default**: 0

### subscribeThrottlingRatePerConsumer

Too many subscribe requests from a consumer can cause broker rewinding consumer cursors and loading data from bookies, hence causing high network bandwidth usage. When the positive value is set, broker will throttle the subscribe requests for one consumer. Otherwise, the throttling will be disabled. By default, throttling is disabled.

**Default**: 0

### subscribeRatePeriodPerConsumerInSecond

Rate period for {subscribeThrottlingRatePerConsumer}. By default, it is 30s.

**Default**: 30

### dispatchThrottlingRateInMsg

Dispatch throttling-limit of messages for a broker (per second). 0 means the dispatch throttling-limit is disabled.

**Default**: 0

### dispatchThrottlingRateInByte

Dispatch throttling-limit of bytes for a broker (per second). 0 means the dispatch throttling-limit is disabled.

**Default**: 0

### dispatchThrottlingRatePerTopicInMsg

Default messages (per second) dispatch throttling-limit for every topic. When the value is set to 0, default message dispatch throttling-limit is disabled.

**Default**: 0

### dispatchThrottlingRatePerTopicInByte

Default byte (per second) dispatch throttling-limit for every topic. When the value is set to 0, default byte dispatch throttling-limit is disabled.

**Default**: 0

### dispatchThrottlingOnBatchMessageEnabled

Apply dispatch rate limiting on batch message instead individual messages with in batch message. (Default is disabled).

**Default**: false

### dispatchThrottlingRateRelativeToPublishRate

Enable dispatch rate-limiting relative to publish rate.

**Default**: false

### dispatchThrottlingRatePerSubscriptionInMsg

The defaulted number of message dispatching throttling-limit for a subscription. The value of 0 disables message dispatch-throttling.

**Default**: 0

### dispatchThrottlingRatePerSubscriptionInByte

The default number of message-bytes dispatching throttling-limit for a subscription. The value of 0 disables message-byte dispatch-throttling.

**Default**: 0

### dispatchThrottlingRatePerReplicatorInMsg

Dispatch throttling-limit of messages for every replicator in replication (per second). 0 means the dispatch throttling-limit in replication is disabled.

**Default**: 0

### dispatchThrottlingRatePerReplicatorInByte

Dispatch throttling-limit of bytes for every replicator in replication (per second). 0 means the dispatch throttling-limit is disabled.

**Default**: 0

### dispatchThrottlingOnNonBacklogConsumerEnabled

Enable dispatch-throttling for both caught up consumers as well as consumers who have backlogs.

**Default**: true

### dispatcherMaxReadBatchSize

The maximum number of entries to read from BookKeeper. By default, it is 100 entries.

**Default**: 100

### dispatcherMaxReadSizeBytes

The maximum size in bytes of entries to read from BookKeeper. By default, it is 5MB.

**Default**: 5242880

### dispatcherMinReadBatchSize

The minimum number of entries to read from BookKeeper. By default, it is 1 entry. When there is an error occurred on reading entries from bookkeeper, the broker will backoff the batch size to this minimum number.

**Default**: 1

### dispatcherMaxRoundRobinBatchSize

The maximum number of entries to dispatch for a shared subscription. By default, it is 20 entries.

**Default**: 20

### preciseDispatcherFlowControl

Precise dispathcer flow control according to history message number of each entry.

**Default**: false

### streamingDispatch

Whether to use streaming read dispatcher. It can be useful when there's a huge backlog to drain and instead of read with micro batch we can streamline the read from bookkeeper to make the most of consumer capacity till we hit bookkeeper read limit or consumer process limit, then we can use consumer flow control to tune the speed. This feature is currently in preview and can be changed in subsequent release.

**Default**: false

### maxConcurrentLookupRequest

Maximum number of concurrent lookup request that the broker allows to throttle heavy incoming lookup traffic.

**Default**: 50000

### maxConcurrentTopicLoadRequest

Maximum number of concurrent topic loading request that the broker allows to control the number of zk-operations.

**Default**: 5000

### maxConcurrentNonPersistentMessagePerConnection

Maximum number of concurrent non-persistent message that can be processed per connection.

**Default**: 1000

### numWorkerThreadsForNonPersistentTopic

Number of worker threads to serve non-persistent topic.

**Default**: 8

### enablePersistentTopics

Enable broker to load persistent topics.

**Default**: true

### enableNonPersistentTopics

Enable broker to load non-persistent topics.

**Default**: true

### maxSubscriptionsPerTopic

Maximum number of subscriptions allowed to subscribe to a topic. Once this limit reaches, the broker rejects new subscriptions until the number of subscriptions decreases. When the value is set to 0, the limit check is disabled.

**Default**: 0

### maxProducersPerTopic

Maximum number of producers allowed to connect to a topic. Once this limit reaches, the broker rejects new producers until the number of connected producers decreases. When the value is set to 0, the limit check is disabled.

**Default**: 0

### maxConsumersPerTopic

Maximum number of consumers allowed to connect to a topic. Once this limit reaches, the broker rejects new consumers until the number of connected consumers decreases. When the value is set to 0, the limit check is disabled.

**Default**: 0

### maxConsumersPerSubscription

Maximum number of consumers allowed to connect to a subscription. Once this limit reaches, the broker rejects new consumers until the number of connected consumers decreases. When the value is set to 0, the limit check is disabled.

**Default**: 0

### maxNumPartitionsPerPartitionedTopic

Maximum number of partitions per partitioned topic. When the value is set to a negative number or is set to 0, the check is disabled.

**Default**: 0

### metadataStoreBatchingEnabled

Enable metadata operations batching.

**Default**: true

### metadataStoreBatchingMaxDelayMillis

Maximum delay to impose on batching grouping.

**Default**: 5

### metadataStoreBatchingMaxOperations

Maximum number of operations to include in a singular batch.

**Default**: 1000

### metadataStoreBatchingMaxSizeKb

Maximum size of a batch.

**Default**: 128

### tlsCertRefreshCheckDurationSec

TLS certificate refresh duration in seconds. When the value is set to 0, check the TLS certificate on every new connection.

**Default**: 300

### tlsCertificateFilePath

Path for the TLS certificate file.

**Default**:

### tlsKeyFilePath

Path for the TLS private key file.

**Default**:

### tlsTrustCertsFilePath

Path for the trusted TLS certificate file.

**Default**:

### tlsAllowInsecureConnection

Accept untrusted TLS certificate from the client. If it is set to true, a client with a certificate which cannot be verified with the 'tlsTrustCertsFilePath' certificate is allowed to connect to the server, though the certificate is not be used for client authentication.

**Default**: false

### tlsProtocols

Specify the TLS protocols the broker uses to negotiate during TLS handshake.

**Default**:

### tlsCiphers

Specify the TLS cipher the broker uses to negotiate during TLS Handshake.

**Default**:

### tlsRequireTrustedClientCertOnConnect

Trusted client certificates are required for to connect TLS. Reject the Connection if the client certificate is not trusted. In effect, this requires that all connecting clients perform TLS client authentication.

**Default**: false

### tlsEnabledWithKeyStore

Enable TLS with KeyStore type configuration in broker.

**Default**: false

### tlsProvider

The TLS provider for the broker service.

When TLS authentication with CACert is used, the valid value is either `OPENSSL` or `JDK`.

When TLS authentication with KeyStore is used, available options can be `SunJSSE`, `Conscrypt` and so on.

**Default**: N/A

### tlsKeyStoreType

TLS KeyStore type configuration in the broker.<li>JKS </li><li>PKCS12 </li>

**Default**: JKS

### tlsKeyStore

TLS KeyStore path in the broker.

**Default**:

### tlsKeyStorePassword

TLS KeyStore password for the broker.

**Default**:

### tlsTrustStoreType

TLS TrustStore type configuration in the broker<li>JKS </li><li>PKCS12 </li>

**Default**: JKS

### tlsTrustStore

TLS TrustStore path in the broker.

**Default**:

### tlsTrustStorePassword

TLS TrustStore password for the broker.

**Default**:

### brokerClientTlsEnabledWithKeyStore

Configure whether the internal client uses the KeyStore type to authenticate with Pulsar brokers.

**Default**: false

### brokerClientSslProvider

The TLS Provider used by the internal client to authenticate with other Pulsar brokers.

**Default**:

### brokerClientTlsTrustStoreType

TLS TrustStore type configuration for the internal client to authenticate with Pulsar brokers. <li>JKS </li><li>PKCS12 </li>

**Default**: JKS

### brokerClientTlsTrustStore

TLS TrustStore path for the internal client to authenticate with Pulsar brokers.

**Default**:

### brokerClientTlsTrustStorePassword

TLS TrustStore password for the internal client to authenticate with Pulsar brokers.

**Default**:

### brokerClientTlsCiphers

Specify the TLS cipher that the internal client uses to negotiate during TLS Handshake.

**Default**:

### brokerClientTlsProtocols

Specify the TLS protocols that the broker uses to negotiate during TLS handshake.

**Default**:

### systemTopicEnabled

Enable/Disable system topics.

**Default**: false

### topicLevelPoliciesEnabled

Enable or disable topic level policies. Topic level policies depends on the system topic. Please enable the system topic first.

**Default**: false

### topicFencingTimeoutSeconds

If a topic remains fenced for a certain time period (in seconds), it is closed forcefully. If set to 0 or a negative number, the fenced topic is not closed.

**Default**: 0

### proxyRoles

Role names that are treated as "proxy roles". If the broker receives a request from a proxy role, it demands to authenticate its client role. Note that client role and proxy role cannot use the same name.

**Default**:

### authenticationEnabled

Enable authentication for the broker.

**Default**: false

### authenticationProviders

A comma-separated list of class names for authentication providers.

**Default**: false

### authorizationEnabled

Enforce authorization in brokers.

**Default**: false

### authorizationProvider

Authorization provider fully qualified class-name.

**Default**: org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider

### authorizationAllowWildcardsMatching

Allow wildcard matching in authorization. Wildcard matching is applicable only when the wildcard-character (\*) presents at the **first** or **last** position.

**Default**: false

### superUserRoles

Role names that are treated as “superusers.” Superusers are authorized to perform all admin tasks.

**Default**:

### brokerClientAuthenticationPlugin

The authentication settings of the broker itself. Used when the broker connects to other brokers either in the same cluster or from other clusters.

**Default**:

### brokerClientAuthenticationParameters

The parameters that go along with the plugin specified using brokerClientAuthenticationPlugin.

**Default**:

### athenzDomainNames

Supported Athenz authentication provider domain names as a comma-separated list.

**Default**:

### anonymousUserRole

When this parameter is not empty, unauthenticated users perform as anonymousUserRole.

**Default**:

### tokenSettingPrefix

Configure the prefix of the token related setting like `tokenSecretKey`, `tokenPublicKey`, `tokenAuthClaim`, `tokenPublicAlg`, `tokenAudienceClaim`, and `tokenAudience`.

**Default**:

### tokenSecretKey

Configure the secret key to be used to validate auth tokens. The key can be specified like: `tokenSecretKey=data:;base64,xxxxxxxxx` or `tokenSecretKey=file:///my/secret.key`. Note: key file must be DER-encoded.

**Default**:

### tokenPublicKey

Configure the public key to be used to validate auth tokens. The key can be specified like: `tokenPublicKey=data:;base64,xxxxxxxxx` or `tokenPublicKey=file:///my/secret.key`. Note: key file must be DER-encoded.

**Default**:

### tokenAuthClaim

Specify the token claim that will be used as the authentication "principal" or "role". The "subject" field will be used if this is left blank

**Default**:

### tokenAudienceClaim

The token audience "claim" name, e.g. "aud". It is used to get the audience from token. If it is not set, the audience is not verified.

**Default**:

### tokenAudience

The token audience stands for this broker. The field `tokenAudienceClaim` of a valid token need contains this parameter.

**Default**:

### saslJaasClientAllowedIds

This is a regexp, which limits the range of possible ids which can connect to the Broker using SASL. By default, it is set to `SaslConstants.JAAS_CLIENT_ALLOWED_IDS_DEFAULT`, which is ".\_pulsar.\*", so only clients whose id contains 'pulsar' are allowed to connect.

**Default**: N/A

### saslJaasServerSectionName

Service Principal, for login context name. By default, it is set to `SaslConstants.JAAS_DEFAULT_BROKER_SECTION_NAME`, which is "Broker".

**Default**: N/A

### httpMaxRequestSize

If the value is larger than 0, it rejects all HTTP requests with bodies larged than the configured limit.

**Default**: -1

### exposePreciseBacklogInPrometheus

Enable expose the precise backlog stats, set false to use published counter and consumed counter to calculate, this would be more efficient but may be inaccurate.

**Default**: false

### bookkeeperMetadataServiceUri

Metadata service uri is what BookKeeper used for loading corresponding metadata driver and resolving its metadata service location. This value can be fetched using `bookkeeper shell whatisinstanceid` command in BookKeeper cluster. For example: `zk+hierarchical://localhost:2181/ledgers`. The metadata service uri list can also be semicolon separated values like: `zk+hierarchical://zk1:2181;zk2:2181;zk3:2181/ledgers`.

**Default**: N/A

### bookkeeperClientAuthenticationPlugin

Authentication plugin to be used when connecting to bookies (BookKeeper servers).

**Default**:

### bookkeeperClientAuthenticationParametersName

BookKeeper authentication plugin implementation parameters and values.

**Default**:

### bookkeeperClientAuthenticationParameters

Parameters associated with the bookkeeperClientAuthenticationParametersName

**Default**:

### bookkeeperClientNumWorkerThreads

Number of BookKeeper client worker threads. Default is Runtime.getRuntime().availableProcessors()

**Default**:

### bookkeeperClientTimeoutInSeconds

Timeout for BookKeeper add and read operations.

**Default**: 30

### bookkeeperClientSpeculativeReadTimeoutInMillis

Speculative reads are initiated if a read request doesn’t complete within a certain time. A value of 0 disables speculative reads.

**Default**: 0

### bookkeeperUseV2WireProtocol

Use older Bookkeeper wire protocol with bookie.

**Default**: true

### bookkeeperClientHealthCheckEnabled

Enable bookie health checks.

**Default**: true

### bookkeeperClientHealthCheckIntervalSeconds

The time interval, in seconds, at which health checks are performed. New ledgers are not created during health checks.

**Default**: 60

### bookkeeperClientHealthCheckErrorThresholdPerInterval

Error threshold for health checks.

**Default**: 5

### bookkeeperClientHealthCheckQuarantineTimeInSeconds

If bookies have more than the allowed number of failures within the time interval specified by bookkeeperClientHealthCheckIntervalSeconds

**Default**: 1800

### bookkeeperClientGetBookieInfoIntervalSeconds

Specify options for the GetBookieInfo check. This setting helps ensure the list of bookies that are up to date on the brokers.

**Default**: 86400

### bookkeeperClientGetBookieInfoRetryIntervalSeconds

Specify options for the GetBookieInfo check. This setting helps ensure the list of bookies that are up to date on the brokers.

**Default**: 60

### bookkeeperClientRackawarePolicyEnabled

**Default**: true

### bookkeeperClientRegionawarePolicyEnabled

**Default**: false

### bookkeeperClientMinNumRacksPerWriteQuorum

**Default**: 2

### bookkeeperClientMinNumRacksPerWriteQuorum

**Default**: false

### bookkeeperClientReorderReadSequenceEnabled

**Default**: false

### bookkeeperClientIsolationGroups

**Default**:

### bookkeeperClientSecondaryIsolationGroups

Enable bookie secondary-isolation group if bookkeeperClientIsolationGroups doesn't have enough bookie available.

**Default**:

### bookkeeperClientMinAvailableBookiesInIsolationGroups

Minimum bookies that should be available as part of bookkeeperClientIsolationGroups else broker will include bookkeeperClientSecondaryIsolationGroups bookies in isolated list.

**Default**:

### bookkeeperTLSProviderFactoryClass

Set the client security provider factory class name.

**Default**: org.apache.bookkeeper.tls.TLSContextFactory

### bookkeeperTLSClientAuthentication

Enable TLS authentication with bookie.

**Default**: false

### bookkeeperTLSKeyFileType

Supported type: PEM, JKS, PKCS12.

**Default**: PEM

### bookkeeperTLSTrustCertTypes

Supported type: PEM, JKS, PKCS12.

**Default**: PEM

### bookkeeperTLSKeyStorePasswordPath

Path to file containing keystore password, if the client keystore is password protected.

**Default**:

### bookkeeperTLSTrustStorePasswordPath

Path to file containing truststore password, if the client truststore is password protected.

**Default**:

### bookkeeperTLSKeyFilePath

Path for the TLS private key file.

**Default**:

### bookkeeperTLSCertificateFilePath

Path for the TLS certificate file.

**Default**:

### bookkeeperTLSTrustCertsFilePath

Path for the trusted TLS certificate file.

**Default**:

### bookkeeperTlsCertFilesRefreshDurationSeconds

Tls cert refresh duration at bookKeeper-client in seconds (0 to disable check).

**Default**:

### bookkeeperDiskWeightBasedPlacementEnabled

Enable/Disable disk weight based placement.

**Default**: false

### bookkeeperExplicitLacIntervalInMills

Set the interval to check the need for sending an explicit LAC. When the value is set to 0, no explicit LAC is sent.

**Default**: 0

### bookkeeperClientExposeStatsToPrometheus

Expose BookKeeper client managed ledger stats to Prometheus.

**Default**: false

### managedLedgerDefaultEnsembleSize

**Default**: 1

### managedLedgerDefaultWriteQuorum

**Default**: 1

### managedLedgerDefaultAckQuorum

**Default**: 1

### managedLedgerDigestType

Default type of checksum to use when writing to BookKeeper.

**Default**: CRC32C

### managedLedgerNumSchedulerThreads

Number of threads to be used for managed ledger scheduled tasks.

**Default**: Runtime.getRuntime().availableProcessors()

### managedLedgerCacheSizeMB

**Default**: N/A

### managedLedgerCacheCopyEntries

Whether to copy the entry payloads when inserting in cache.

**Default**: false

### managedLedgerCacheEvictionWatermark

**Default**: 0.9

### managedLedgerCacheEvictionFrequency

Configure the cache eviction frequency for the managed ledger cache (evictions/sec)

**Default**: 100.0

### managedLedgerCacheEvictionTimeThresholdMillis

All entries that have stayed in cache for more than the configured time, will be evicted

**Default**: 1000

### managedLedgerCursorBackloggedThreshold

Configure the threshold (in number of entries) from where a cursor should be considered 'backlogged' and thus should be set as inactive.

**Default**: 1000

### managedLedgerUnackedRangesOpenCacheSetEnabled

Use Open Range-Set to cache unacknowledged messages

**Default**: true

### managedLedgerDefaultMarkDeleteRateLimit

**Default**: 0.1

### managedLedgerMaxEntriesPerLedger

**Default**: 50000

### managedLedgerMinLedgerRolloverTimeMinutes

**Default**: 10

### managedLedgerMaxLedgerRolloverTimeMinutes

**Default**: 240

### managedLedgerCursorMaxEntriesPerLedger

**Default**: 50000

### managedLedgerCursorRolloverTimeInSeconds

**Default**: 14400

### managedLedgerMaxSizePerLedgerMbytes

Maximum ledger size before triggering a rollover for a topic.

**Default**: 2048

### managedLedgerMaxUnackedRangesToPersist

Maximum number of "acknowledgment holes" that are going to be persistently stored. When acknowledging out of order, a consumer leaves holes that are supposed to be quickly filled by acknowledging all the messages. The information of which messages are acknowledged is persisted by compressing in "ranges" of messages that were acknowledged. After the max number of ranges is reached, the information is only tracked in memory and messages are redelivered in case of crashes.

**Default**: 10000

### managedLedgerMaxUnackedRangesToPersistInMetadataStore

Maximum number of "acknowledgment holes" that can be stored in metadata store. If the number of unacknowledged message range is higher than this limit, the broker persists unacknowledged ranges into BookKeeper to avoid additional data overhead into metadata store.

**Default**: 1000

### autoSkipNonRecoverableData

**Default**: false

### managedLedgerMetadataOperationsTimeoutSeconds

Operation timeout while updating managed-ledger metadata.

**Default**: 60

### managedLedgerReadEntryTimeoutSeconds

Read entries timeout when the broker tries to read messages from BookKeeper.

**Default**: 0

### managedLedgerAddEntryTimeoutSeconds

Add entry timeout when the broker tries to publish messages to BookKeeper.

**Default**: 0

### managedLedgerNewEntriesCheckDelayInMillis

New entries check delay for the cursor under the managed ledger. If no new messages in the topic, the cursor tries to check again after the delay time. For consumption latency sensitive scenarios, you can set the value to a smaller value or 0. Of course, a smaller value may degrade consumption throughput.

**Default**: 10

### managedLedgerPrometheusStatsLatencyRolloverSeconds

Managed ledger prometheus stats latency rollover seconds.

**Default**: 60

### managedLedgerTraceTaskExecution

Whether to trace managed ledger task execution time.

**Default**: true

### loadBalancerEnabled

**Default**: false

### loadBalancerPlacementStrategy

**Default**: weightedRandomSelection

### loadBalancerReportUpdateThresholdPercentage

**Default**: 10

### loadBalancerReportUpdateMaxIntervalMinutes

**Default**: 15

### loadBalancerHostUsageCheckIntervalMinutes

**Default**: 1

### loadBalancerSheddingIntervalMinutes

**Default**: 30

### loadBalancerSheddingGracePeriodMinutes

**Default**: 30

### loadBalancerBrokerMaxTopics

**Default**: 50000

### loadBalancerBrokerUnderloadedThresholdPercentage

**Default**: 1

### loadBalancerBrokerOverloadedThresholdPercentage

**Default**: 85

### loadBalancerResourceQuotaUpdateIntervalMinutes

**Default**: 15

### loadBalancerBrokerComfortLoadLevelPercentage

**Default**: 65

### loadBalancerAutoBundleSplitEnabled

**Default**: false

### loadBalancerAutoUnloadSplitBundlesEnabled

Enable/Disable automatic unloading of split bundles.

**Default**: true

### loadBalancerNamespaceBundleMaxTopics

**Default**: 1000

### loadBalancerNamespaceBundleMaxSessions

Maximum sessions (producers + consumers) in a bundle, otherwise bundle split will be triggered.

To disable the threshold check, set the value to -1.

**Default**: 1000

### loadBalancerNamespaceBundleMaxMsgRate

**Default**: 1000

### loadBalancerNamespaceBundleMaxBandwidthMbytes

**Default**: 100

### loadBalancerNamespaceMaximumBundles

**Default**: 128

### loadBalancerBrokerThresholdShedderPercentage

The broker resource usage threshold. When the broker resource usage is greater than the pulsar cluster average resource usage, the threshold shedder is triggered to offload bundles from the broker. It only takes effect in the ThresholdShedder strategy.

**Default**: 10

### loadBalancerMsgRateDifferenceShedderThreshold

Message-rate percentage threshold between highest and least loaded brokers for uniform load shedding.

**Default**: 50

### loadBalancerMsgThroughputMultiplierDifferenceShedderThreshold

Message-throughput threshold between highest and least loaded brokers for uniform load shedding.

**Default**: 4

### loadBalancerHistoryResourcePercentage

The history usage when calculating new resource usage. It only takes effect in the ThresholdShedder strategy.

**Default**: 0.9

### loadBalancerBandwithInResourceWeight

The BandWithIn usage weight when calculating new resource usage. It only takes effect in the ThresholdShedder strategy.

**Default**: 1.0

### loadBalancerBandwithOutResourceWeight

The BandWithOut usage weight when calculating new resource usage. It only takes effect in the ThresholdShedder strategy.

**Default**: 1.0

### loadBalancerCPUResourceWeight

The CPU usage weight when calculating new resource usage. It only takes effect in the ThresholdShedder strategy.

**Default**: 1.0

### loadBalancerMemoryResourceWeight

The heap memory usage weight when calculating new resource usage. It only takes effect in the ThresholdShedder strategy.

**Default**: 1.0

### loadBalancerDirectMemoryResourceWeight

The direct memory usage weight when calculating new resource usage. It only takes effect in the ThresholdShedder strategy.

**Default**: 1.0

### loadBalancerBundleUnloadMinThroughputThreshold

Bundle unload minimum throughput threshold. Avoid bundle unload frequently. It only takes effect in the ThresholdShedder strategy.

**Default**: 10

### namespaceBundleUnloadingTimeoutMs

Time to wait for the unloading of a namespace bundle in milliseconds.

**Default**: 60000

### replicationMetricsEnabled

**Default**: true

### replicationConnectionsPerBroker

**Default**: 16

### replicationProducerQueueSize

**Default**: 1000

### replicationPolicyCheckDurationSeconds

Duration to check replication policy to avoid replicator inconsistency due to missing ZooKeeper watch. When the value is set to 0, disable checking replication policy.

**Default**: 600

### transactionCoordinatorEnabled

Whether to enable transaction coordinator in broker.

**Default**: false

### transactionMetadataStoreProviderClassName

The class name of transactionMetadataStoreProvider.

**Default**: org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStoreProvider

### transactionBufferClientOperationTimeoutInMills

The transaction buffer client's operation timeout in milliseconds.

**Default**: 3000

### transactionBufferSnapshotMaxTransactionCount

Transaction buffer takes a snapshot after the number of transaction operations reaches this value.

**Default**: 1000

### transactionBufferSnapshotMinTimeInMillis

The interval between two snapshots that the transaction buffer takes (in milliseconds).

**Default**: 5000

### defaultRetentionTimeInMinutes

**Default**: 0

### defaultRetentionSizeInMB

**Default**: 0

### keepAliveIntervalSeconds

**Default**: 30

### haProxyProtocolEnabled

Enable or disable the [HAProxy](http://www.haproxy.org/) protocol.

**Default**: false

### bookieId

If you want to custom a bookie ID or use a dynamic network address for a bookie, you can set the `bookieId`.

Bookie advertises itself using the `bookieId` rather than the `BookieSocketAddress` (`hostname:port` or `IP:port`).

The `bookieId` is a non-empty string that can contain ASCII digits and letters ([a-zA-Z9-0]), colons, dashes, and dots.

For more information about `bookieId`, see [here](http://bookkeeper.apache.org/bps/BP-41-bookieid/).

**Default**: /

### maxTopicsPerNamespace

The maximum number of persistent topics that can be created in the namespace. When the number of topics reaches this threshold, the broker rejects the request of creating a new topic, including the auto-created topics by the producer or consumer, until the number of connected consumers decreases. The default value 0 disables the check.

**Default**: 0

### metadataStoreConfigPath

The configuration file path of the local metadata store. Standalone Pulsar uses [RocksDB](http://rocksdb.org/) as the local metadata store. The format is `/xxx/xx/rocksdb.ini`.

**Default**: N/A

### schemaRegistryStorageClassName

The schema storage implementation used by this broker.

**Default**: org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorageFactory

### isSchemaValidationEnforced

Whether to enable schema validation, when schema validation is enabled, if a producer without a schema attempts to produce the message to a topic with schema, the producer is rejected and disconnected.

**Default**: false

### isAllowAutoUpdateSchemaEnabled

Allow schema to be auto updated at broker level.

**Default**: true

### schemaRegistryCompatibilityCheckers

Deploy the schema compatibility checker for a specific schema type to enforce schema compatibility check.

**Default**: org.apache.pulsar.broker.service.schema.JsonSchemaCompatibilityCheck,org.apache.pulsar.broker.service.schema.AvroSchemaCompatibilityCheck,org.apache.pulsar.broker.service.schema.ProtobufNativeSchemaCompatibilityCheck

### schemaCompatibilityStrategy

The schema compatibility strategy at broker level, see [here](schema-evolution-compatibility.md#schema-compatibility-check-strategy) for available values.

**Default**: FULL

### systemTopicSchemaCompatibilityStrategy

The schema compatibility strategy is used for system topics, see [here](schema-evolution-compatibility.md#schema-compatibility-check-strategy) for available values.

**Default**: ALWAYS_COMPATIBLE

### managedCursorInfoCompressionType

The compression type of managed cursor information.

Available options are `NONE`, `LZ4`, `ZLIB`, `ZSTD`, and `SNAPPY`).

If this value is `NONE`, managed cursor information is not compressed.

**Default**: NONE

## Deprecated

The following parameters have been deprecated in the `conf/standalone.conf` file.

### zookeeperServers

The quorum connection string for local metadata store. Use `metadataStoreUrl` instead.

**Default**: N/A

### configurationStoreServers

Configuration store connection string (as a comma-separated list). Use `configurationMetadataStoreUrl` instead.

**Default**: N/A

### zooKeeperOperationTimeoutSeconds

ZooKeeper operation timeout in seconds. Use `metadataStoreOperationTimeoutSeconds` instead.

**Default**: 30

### zooKeeperCacheExpirySeconds

ZooKeeper cache expiry time in seconds. Use `metadataStoreCacheExpirySeconds` instead.

**Default**: 300

### zooKeeperSessionTimeoutMillis

The ZooKeeper session timeout, in milliseconds. Use `metadataStoreSessionTimeoutMillis` instead.

**Default**: 30000

### managedLedgerMaxUnackedRangesToPersistInZooKeeper

Maximum number of "acknowledgment holes" that can be stored in ZooKeeper. Use `managedLedgerMaxUnackedRangesToPersistInMetadataStore` instead.

**Default**: 1000
