/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.api.DigestType;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

import com.google.common.collect.Sets;

/**
 * Pulsar service configuration object.
 */
public class ServiceConfiguration implements PulsarConfiguration {

    /***** --- pulsar configuration --- ****/
    // Zookeeper quorum connection string
    @FieldContext(required = true)
    private String zookeeperServers;
    // Global Zookeeper quorum connection string
    @Deprecated
    @FieldContext(required = false)
    private String globalZookeeperServers;
    // Configuration Store connection string
    @FieldContext(required = false)
    private String configurationStoreServers;
    private int brokerServicePort = 6650;
    private int brokerServicePortTls = 6651;
    // Port to use to server HTTP request
    private int webServicePort = 8080;
    // Port to use to server HTTPS request
    private int webServicePortTls = 8443;

    // Hostname or IP address the service binds on.
    private String bindAddress = "0.0.0.0";

    // Controls which hostname is advertised to the discovery service via ZooKeeper.
    private String advertisedAddress;

    // Enable the WebSocket API service
    private boolean webSocketServiceEnabled = false;

    // Flag to control features that are meant to be used when running in standalone mode
    private boolean isRunningStandalone = false;

    // Name of the cluster to which this broker belongs to
    @FieldContext(required = true)
    private String clusterName;
    // Enable cluster's failure-domain which can distribute brokers into logical region
    @FieldContext(dynamic = true)
    private boolean failureDomainsEnabled = false;
    // Zookeeper session timeout in milliseconds
    private long zooKeeperSessionTimeoutMillis = 30000;
    // Time to wait for broker graceful shutdown. After this time elapses, the
    // process will be killed
    @FieldContext(dynamic = true)
    private long brokerShutdownTimeoutMs = 3000;
    // Enable backlog quota check. Enforces action on topic when the quota is
    // reached
    private boolean backlogQuotaCheckEnabled = true;
    // How often to check for topics that have reached the quota
    private int backlogQuotaCheckIntervalInSeconds = 60;
    // Default per-topic backlog quota limit
    private long backlogQuotaDefaultLimitGB = 50;
    // Enable the deletion of inactive topics
    private boolean brokerDeleteInactiveTopicsEnabled = true;
    // How often to check for inactive topics
    private long brokerDeleteInactiveTopicsFrequencySeconds = 60;
    // How frequently to proactively check and purge expired messages
    private int messageExpiryCheckIntervalInMinutes = 5;
    // How long to delay rewinding cursor and dispatching messages when active consumer is changed
    private int activeConsumerFailoverDelayTimeMillis = 1000;
    // How long to delete inactive subscriptions from last consuming
    // When it is 0, inactive subscriptions are not deleted automatically
    private long subscriptionExpirationTimeMinutes = 0;
    // How frequently to proactively check and purge expired subscription
    private long subscriptionExpiryCheckIntervalInMinutes = 5;

    // Set the default behavior for message deduplication in the broker
    // This can be overridden per-namespace. If enabled, broker will reject
    // messages that were already stored in the topic
    private boolean brokerDeduplicationEnabled = false;

    // Maximum number of producer information that it's going to be
    // persisted for deduplication purposes
    private int brokerDeduplicationMaxNumberOfProducers = 10000;

    // Number of entries after which a dedup info snapshot is taken.
    // A bigger interval will lead to less snapshots being taken though it would
    // increase the topic recovery time, when the entries published after the
    // snapshot need to be replayed
    private int brokerDeduplicationEntriesInterval = 1000;

    // Time of inactivity after which the broker will discard the deduplication information
    // relative to a disconnected producer. Default is 6 hours.
    private int brokerDeduplicationProducerInactivityTimeoutMinutes = 360;

    // When a namespace is created without specifying the number of bundle, this
    // value will be used as the default
    private int defaultNumberOfNamespaceBundles = 4;

    // Enable check for minimum allowed client library version
    @FieldContext(dynamic = true)
    private boolean clientLibraryVersionCheckEnabled = false;
    // Path for the file used to determine the rotation status for the broker
    // when responding to service discovery health checks
    private String statusFilePath;
    // Max number of unacknowledged messages allowed to receive messages by a consumer on a shared subscription. Broker
    // will stop sending messages to consumer once, this limit reaches until consumer starts acknowledging messages back
    // and unack count reaches to maxUnackedMessagesPerConsumer/2 Using a value of 0, is disabling unackedMessage-limit
    // check and consumer can receive messages without any restriction
    private int maxUnackedMessagesPerConsumer = 50000;
    // Max number of unacknowledged messages allowed per shared subscription. Broker will stop dispatching messages to
    // all consumers of the subscription once this limit reaches until consumer starts acknowledging messages back and
    // unack count reaches to limit/2. Using a value of 0, is disabling unackedMessage-limit
    // check and dispatcher can dispatch messages without any restriction
    private int maxUnackedMessagesPerSubscription = 4 * 50000;
    // Max number of unacknowledged messages allowed per broker. Once this limit reaches, broker will stop dispatching
    // messages to all shared subscription which has higher number of unack messages until subscriptions start
    // acknowledging messages back and unack count reaches to limit/2. Using a value of 0, is disabling
    // unackedMessage-limit check and broker doesn't block dispatchers
    private int maxUnackedMessagesPerBroker = 0;
    // Once broker reaches maxUnackedMessagesPerBroker limit, it blocks subscriptions which has higher unacked messages
    // than this percentage limit and subscription will not receive any new messages until that subscription acks back
    // limit/2 messages
    private double maxUnackedMessagesPerSubscriptionOnBrokerBlocked = 0.16;
    // Default number of message dispatching throttling-limit for every topic. Using a value of 0, is disabling default
    // message dispatch-throttling
    @FieldContext(dynamic = true)
    private int dispatchThrottlingRatePerTopicInMsg = 0;
    // Default number of message-bytes dispatching throttling-limit for every topic. Using a value of 0, is disabling
    // default message-byte dispatch-throttling
    @FieldContext(dynamic = true)
    private long dispatchThrottlingRatePerTopicInByte = 0;
    // Default number of message dispatching throttling-limit for a subscription.
    // Using a value of 0, is disabling.
    @FieldContext(dynamic = true)
    private int dispatchThrottlingRatePerSubscriptionInMsg = 0;
    // Default number of message-bytes dispatching throttling-limit for a subscription.
    // Using a value of 0, is disabling.
    @FieldContext(dynamic = true)
    private long dispatchThrottlingRatePerSubscribeInByte = 0;
    // Default dispatch-throttling is disabled for consumers which already caught-up with published messages and
    // don't have backlog. This enables dispatch-throttling for non-backlog consumers as well.
    @FieldContext(dynamic = true)
    private boolean dispatchThrottlingOnNonBacklogConsumerEnabled = false;
    // Max number of concurrent lookup request broker allows to throttle heavy incoming lookup traffic
    @FieldContext(dynamic = true)
    private int maxConcurrentLookupRequest = 10000;
    // Max number of concurrent topic loading request broker allows to control number of zk-operations
    @FieldContext(dynamic = true)
    private int maxConcurrentTopicLoadRequest = 5000;
    // Max concurrent non-persistent message can be processed per connection
    private int maxConcurrentNonPersistentMessagePerConnection = 1000;
    // Number of worker threads to serve non-persistent topic
    private int numWorkerThreadsForNonPersistentTopic = 8;

    // Enable broker to load persistent topics
    private boolean enablePersistentTopics = true;

    // Enable broker to load non-persistent topics
    private boolean enableNonPersistentTopics = true;

    // Enable to run bookie along with broker
    private boolean enableRunBookieTogether = false;

    // Enable to run bookie autorecovery along with broker
    private boolean enableRunBookieAutoRecoveryTogether = false;

    // Max number of producers allowed to connect to topic. Once this limit reaches, Broker will reject new producers
    // until the number of connected producers decrease.
    // Using a value of 0, is disabling maxProducersPerTopic-limit check.
    private int maxProducersPerTopic = 0;

    // Max number of consumers allowed to connect to topic. Once this limit reaches, Broker will reject new consumers
    // until the number of connected consumers decrease.
    // Using a value of 0, is disabling maxConsumersPerTopic-limit check.
    private int maxConsumersPerTopic = 0;

    // Max number of consumers allowed to connect to subscription. Once this limit reaches, Broker will reject new consumers
    // until the number of connected consumers decrease.
    // Using a value of 0, is disabling maxConsumersPerSubscription-limit check.
    private int maxConsumersPerSubscription = 0;

    /***** --- TLS --- ****/
    // Enable TLS
    private boolean tlsEnabled = false;
    // Path for the TLS certificate file
    private String tlsCertificateFilePath;
    // Path for the TLS private key file
    private String tlsKeyFilePath;
    // Path for the trusted TLS certificate file
    private String tlsTrustCertsFilePath = "";
    // Accept untrusted TLS certificate from client
    private boolean tlsAllowInsecureConnection = false;
    // Specify the tls protocols the broker will use to negotiate during TLS Handshake.
    // Example:- [TLSv1.2, TLSv1.1, TLSv1]
    private Set<String> tlsProtocols = Sets.newTreeSet();
    // Specify the tls cipher the broker will use to negotiate during TLS Handshake.
    // Example:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]
    private Set<String> tlsCiphers = Sets.newTreeSet();
    // Specify whether Client certificates are required for TLS
    // Reject the Connection if the Client Certificate is not trusted.
    private boolean tlsRequireTrustedClientCertOnConnect = false;

    /***** --- Authentication --- ****/
    // Enable authentication
    private boolean authenticationEnabled = false;
    // Autentication provider name list, which is a list of class names
    private Set<String> authenticationProviders = Sets.newTreeSet();

    // Enforce authorization
    private boolean authorizationEnabled = false;
    // Authorization provider fully qualified class-name
    private String authorizationProvider = PulsarAuthorizationProvider.class.getName();

    // Role names that are treated as "super-user", meaning they will be able to
    // do all admin operations and publish/consume from all topics
    private Set<String> superUserRoles = Sets.newTreeSet();

    // Role names that are treated as "proxy roles". If the broker sees a request with
    // role as proxyRoles - it will demand to see the original client role or certificate.
    private Set<String> proxyRoles = Sets.newTreeSet();

    // If this flag is set then the broker authenticates the original Auth data
    // else it just accepts the originalPrincipal and authorizes it (if required).
    private boolean authenticateOriginalAuthData = false;

    // Allow wildcard matching in authorization
    // (wildcard matching only applicable if wildcard-char:
    // * presents at first or last position eg: *.pulsar.service, pulsar.service.*)
    private boolean authorizationAllowWildcardsMatching = false;

    // Authentication settings of the broker itself. Used when the broker connects
    // to other brokers, either in same or other clusters. Default uses plugin which disables authentication
    private String brokerClientAuthenticationPlugin = "org.apache.pulsar.client.impl.auth.AuthenticationDisabled";
    private String brokerClientAuthenticationParameters = "";
    // Path for the trusted TLS certificate file for outgoing connection to a server (broker)
    private String brokerClientTrustCertsFilePath = "";

    // When this parameter is not empty, unauthenticated users perform as anonymousUserRole
    private String anonymousUserRole = null;

    /**** --- BookKeeper Client --- ****/
    // Authentication plugin to use when connecting to bookies
    private String bookkeeperClientAuthenticationPlugin;
    // BookKeeper auth plugin implementatation specifics parameters name and values
    private String bookkeeperClientAuthenticationParametersName;
    private String bookkeeperClientAuthenticationParameters;

    // Timeout for BK add / read operations
    private long bookkeeperClientTimeoutInSeconds = 30;
    // Speculative reads are initiated if a read request doesn't complete within
    // a certain time Using a value of 0, is disabling the speculative reads
    private int bookkeeperClientSpeculativeReadTimeoutInMillis = 0;
    // Enable bookies health check. Bookies that have more than the configured
    // number of failure within the interval will be quarantined for some time.
    // During this period, new ledgers won't be created on these bookies
    private boolean bookkeeperClientHealthCheckEnabled = true;
    private long bookkeeperClientHealthCheckIntervalSeconds = 60;
    private long bookkeeperClientHealthCheckErrorThresholdPerInterval = 5;
    private long bookkeeperClientHealthCheckQuarantineTimeInSeconds = 1800;
    // Enable rack-aware bookie selection policy. BK will chose bookies from
    // different racks when forming a new bookie ensemble
    private boolean bookkeeperClientRackawarePolicyEnabled = true;
    // Enable bookie isolation by specifying a list of bookie groups to choose
    // from. Any bookie outside the specified groups will not be used by the
    // broker
    @FieldContext(required = false)
    private String bookkeeperClientIsolationGroups;

    /**** --- Managed Ledger --- ****/
    // Number of bookies to use when creating a ledger
    @FieldContext(minValue = 1)
    private int managedLedgerDefaultEnsembleSize = 1;
    // Number of copies to store for each message
    @FieldContext(minValue = 1)
    private int managedLedgerDefaultWriteQuorum = 1;
    // Number of guaranteed copies (acks to wait before write is complete)
    @FieldContext(minValue = 1)
    private int managedLedgerDefaultAckQuorum = 1;

    // Default type of checksum to use when writing to BookKeeper. Default is "CRC32C"
    // Other possible options are "CRC32", "MAC" or "DUMMY" (no checksum).
    private DigestType managedLedgerDigestType = DigestType.CRC32C;

    // Max number of bookies to use when creating a ledger
    @FieldContext(minValue = 1)
    private int managedLedgerMaxEnsembleSize = 5;
    // Max number of copies to store for each message
    @FieldContext(minValue = 1)
    private int managedLedgerMaxWriteQuorum = 5;
    // Max number of guaranteed copies (acks to wait before write is complete)
    @FieldContext(minValue = 1)
    private int managedLedgerMaxAckQuorum = 5;
    // Amount of memory to use for caching data payload in managed ledger. This
    // memory
    // is allocated from JVM direct memory and it's shared across all the topics
    // running in the same broker
    private int managedLedgerCacheSizeMB = 1024;
    // Threshold to which bring down the cache level when eviction is triggered
    private double managedLedgerCacheEvictionWatermark = 0.9f;
    // Rate limit the amount of writes per second generated by consumer acking the messages
    private double managedLedgerDefaultMarkDeleteRateLimit = 1.0;
    // Max number of entries to append to a ledger before triggering a rollover
    // A ledger rollover is triggered on these conditions Either the max
    // rollover time has been reached or max entries have been written to the
    // ledged and at least min-time has passed
    private int managedLedgerMaxEntriesPerLedger = 50000;
    // Minimum time between ledger rollover for a topic
    private int managedLedgerMinLedgerRolloverTimeMinutes = 10;
    // Maximum time before forcing a ledger rollover for a topic
    private int managedLedgerMaxLedgerRolloverTimeMinutes = 240;
    // Delay between a ledger being successfully offloaded to long term storage
    // and the ledger being deleted from bookkeeper
    private long managedLedgerOffloadDeletionLagMs = TimeUnit.HOURS.toMillis(4);
    // Max number of entries to append to a cursor ledger
    private int managedLedgerCursorMaxEntriesPerLedger = 50000;
    // Max time before triggering a rollover on a cursor ledger
    private int managedLedgerCursorRolloverTimeInSeconds = 14400;
    // Max number of "acknowledgment holes" that are going to be persistently stored.
    // When acknowledging out of order, a consumer will leave holes that are supposed
    // to be quickly filled by acking all the messages. The information of which
    // messages are acknowledged is persisted by compressing in "ranges" of messages
    // that were acknowledged. After the max number of ranges is reached, the information
    // will only be tracked in memory and messages will be redelivered in case of
    // crashes.
    private int managedLedgerMaxUnackedRangesToPersist = 10000;
    // Max number of "acknowledgment holes" that can be stored in Zookeeper. If number of unack message range is higher
    // than this limit then broker will persist unacked ranges into bookkeeper to avoid additional data overhead into
    // zookeeper.
    private int managedLedgerMaxUnackedRangesToPersistInZooKeeper = 1000;
    // Skip reading non-recoverable/unreadable data-ledger under managed-ledger's list. It helps when data-ledgers gets
    // corrupted at bookkeeper and managed-cursor is stuck at that ledger.
    @FieldContext(dynamic = true)
    private boolean autoSkipNonRecoverableData = false;

    /*** --- Load balancer --- ****/
    // Enable load balancer
    private boolean loadBalancerEnabled = true;
    // load placement strategy[weightedRandomSelection/leastLoadedServer] (only used by SimpleLoadManagerImpl)
    @Deprecated
    private String loadBalancerPlacementStrategy = "leastLoadedServer"; // weighted random selection
    // Percentage of change to trigger load report update
    @FieldContext(dynamic = true)
    private int loadBalancerReportUpdateThresholdPercentage = 10;
    // maximum interval to update load report
    @FieldContext(dynamic = true)
    private int loadBalancerReportUpdateMaxIntervalMinutes = 15;
    // Frequency of report to collect
    private int loadBalancerHostUsageCheckIntervalMinutes = 1;
    // Enable/disable automatic bundle unloading for load-shedding
    @FieldContext(dynamic = true)
    private boolean loadBalancerSheddingEnabled = true;
    // Load shedding interval. Broker periodically checks whether some traffic should be offload from some over-loaded
    // broker to other under-loaded brokers
    private int loadBalancerSheddingIntervalMinutes = 1;
    // Prevent the same topics to be shed and moved to other broker more that
    // once within this timeframe
    private long loadBalancerSheddingGracePeriodMinutes = 30;
    // Usage threshold to determine a broker as under-loaded (only used by SimpleLoadManagerImpl)
    @Deprecated
    private int loadBalancerBrokerUnderloadedThresholdPercentage = 50;
    // Usage threshold to allocate max number of topics to broker
    @FieldContext(dynamic = true)
    private int loadBalancerBrokerMaxTopics = 50000;

    // Usage threshold to determine a broker as over-loaded
    @FieldContext(dynamic = true)
    private int loadBalancerBrokerOverloadedThresholdPercentage = 85;
    // Interval to flush dynamic resource quota to ZooKeeper
    private int loadBalancerResourceQuotaUpdateIntervalMinutes = 15;
    // Usage threshold to determine a broker is having just right level of load (only used by SimpleLoadManagerImpl)
    @Deprecated
    private int loadBalancerBrokerComfortLoadLevelPercentage = 65;
    // enable/disable automatic namespace bundle split
    @FieldContext(dynamic = true)
    private boolean loadBalancerAutoBundleSplitEnabled = true;
    // enable/disable automatic unloading of split bundles
    @FieldContext(dynamic = true)
    private boolean loadBalancerAutoUnloadSplitBundlesEnabled = true;
    // maximum topics in a bundle, otherwise bundle split will be triggered
    private int loadBalancerNamespaceBundleMaxTopics = 1000;
    // maximum sessions (producers + consumers) in a bundle, otherwise bundle split will be triggered
    private int loadBalancerNamespaceBundleMaxSessions = 1000;
    // maximum msgRate (in + out) in a bundle, otherwise bundle split will be triggered
    private int loadBalancerNamespaceBundleMaxMsgRate = 30000;
    // maximum bandwidth (in + out) in a bundle, otherwise bundle split will be triggered
    private int loadBalancerNamespaceBundleMaxBandwidthMbytes = 100;
    // maximum number of bundles in a namespace
    private int loadBalancerNamespaceMaximumBundles = 128;
    // Name of load manager to use
    @FieldContext(dynamic = true)
    private String loadManagerClassName = "org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl";

    // Option to override the auto-detected network interfaces max speed
    private Double loadBalancerOverrideBrokerNicSpeedGbps;

    /**** --- Replication --- ****/
    // Enable replication metrics
    private boolean replicationMetricsEnabled = false;
    // Max number of connections to open for each broker in a remote cluster
    // More connections host-to-host lead to better throughput over high-latency
    // links.
    private int replicationConnectionsPerBroker = 16;
    @FieldContext(required = false)
    // replicator prefix used for replicator producer name and cursor name
    private String replicatorPrefix = "pulsar.repl";
    // Replicator producer queue size;
    private int replicationProducerQueueSize = 1000;
    // Enable TLS when talking with other clusters to replicate messages
    private boolean replicationTlsEnabled = false;

    // Default message retention time
    private int defaultRetentionTimeInMinutes = 0;
    // Default retention size
    private int defaultRetentionSizeInMB = 0;
    // How often to check pulsar connection is still alive
    private int keepAliveIntervalSeconds = 30;
    // How often broker checks for inactive topics to be deleted (topics with no subscriptions and no one connected)
    private int brokerServicePurgeInactiveFrequencyInSeconds = 60;
    private List<String> bootstrapNamespaces = new ArrayList<String>();
    private Properties properties = new Properties();
    // If true, (and ModularLoadManagerImpl is being used), the load manager will attempt to
    // use only brokers running the latest software version (to minimize impact to bundles)
    @FieldContext(dynamic = true)
    private boolean preferLaterVersions = false;

    // Interval between checks to see if topics with compaction policies need to be compacted
    private int brokerServiceCompactionMonitorIntervalInSeconds = 60;

    private String schemaRegistryStorageClassName = "org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorageFactory";
    private Set<String> schemaRegistryCompatibilityCheckers = Sets.newHashSet(
            "org.apache.pulsar.broker.service.schema.JsonSchemaCompatibilityCheck",
            "org.apache.pulsar.broker.service.schema.AvroSchemaCompatibilityCheck"
    );

    /**** --- WebSocket --- ****/
    // Number of IO threads in Pulsar Client used in WebSocket proxy
    private int webSocketNumIoThreads = Runtime.getRuntime().availableProcessors();
    // Number of connections per Broker in Pulsar Client used in WebSocket proxy
    private int webSocketConnectionsPerBroker = Runtime.getRuntime().availableProcessors();
    // Time in milliseconds that idle WebSocket session times out
    private int webSocketSessionIdleTimeoutMillis = 300000;

    /**** --- Metrics --- ****/
    // If true, export topic level metrics otherwise namespace level
    private boolean exposeTopicLevelMetricsInPrometheus = true;

    /**** --- Functions --- ****/
    private boolean functionsWorkerEnabled = false;

    /**** --- Broker Web Stats --- ****/
    // If true, export publisher stats when returning topics stats from the admin rest api
    private boolean exposePublisherStats = true;

    /**** --- Ledger Offloading --- ****/
    // Driver to use to offload old data to long term storage
    private String managedLedgerOffloadDriver = null;

    // Maximum number of thread pool threads for ledger offloading
    private int managedLedgerOffloadMaxThreads = 2;

    // For Amazon S3 ledger offload, AWS region
    private String s3ManagedLedgerOffloadRegion = null;

    // For Amazon S3 ledger offload, Bucket to place offloaded ledger into
    private String s3ManagedLedgerOffloadBucket = null;

    // For Amazon S3 ledger offload, Alternative endpoint to connect to (useful for testing)
    private String s3ManagedLedgerOffloadServiceEndpoint = null;

    // For Amazon S3 ledger offload, Max block size in bytes.
    @FieldContext(minValue = 5242880) // 5MB
    private int s3ManagedLedgerOffloadMaxBlockSizeInBytes = 64 * 1024 * 1024; // 64MB

    // For Amazon S3 ledger offload, Read buffer size in bytes.
    @FieldContext(minValue = 1024)
    private int s3ManagedLedgerOffloadReadBufferSizeInBytes = 1024 * 1024; // 1MB

    public String getZookeeperServers() {
        return zookeeperServers;
    }

    public void setZookeeperServers(String zookeeperServers) {
        this.zookeeperServers = zookeeperServers;
    }

    /**
     * @deprecated See {@link #getConfigurationStoreServers}
     */
    @Deprecated
    public String getGlobalZookeeperServers() {
        if (this.globalZookeeperServers == null || this.globalZookeeperServers.isEmpty()) {
            // If the configuration is not set, assuming that the globalZK is not enabled and all data is in the same
            // ZooKeeper cluster
            return this.getZookeeperServers();
        }
        return globalZookeeperServers;
    }

    /**
     * @deprecated See {@link #setConfigurationStoreServers(String)}
     */
    @Deprecated
    public void setGlobalZookeeperServers(String globalZookeeperServers) {
        this.globalZookeeperServers = globalZookeeperServers;
    }

    public String getConfigurationStoreServers() {
        if (this.configurationStoreServers == null || this.configurationStoreServers.isEmpty()) {
            // If the configuration is not set, assuming that all data is in the same as globalZK cluster
            return this.getGlobalZookeeperServers();
        }
        return configurationStoreServers;
    }

    public void setConfigurationStoreServers(String configurationStoreServers) {
        this.configurationStoreServers = configurationStoreServers;
    }

    public int getBrokerServicePort() {
        return brokerServicePort;
    }

    public void setBrokerServicePort(int brokerServicePort) {
        this.brokerServicePort = brokerServicePort;
    }

    public int getBrokerServicePortTls() {
        return brokerServicePortTls;
    }

    public void setBrokerServicePortTls(int brokerServicePortTls) {
        this.brokerServicePortTls = brokerServicePortTls;
    }

    public int getWebServicePort() {
        return webServicePort;
    }

    public void setWebServicePort(int webServicePort) {
        this.webServicePort = webServicePort;
    }

    public int getWebServicePortTls() {
        return webServicePortTls;
    }

    public void setWebServicePortTls(int webServicePortTls) {
        this.webServicePortTls = webServicePortTls;
    }

    public String getBindAddress() {
        return this.bindAddress;
    }

    public void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    public String getAdvertisedAddress() {
        return this.advertisedAddress;
    }

    public void setAdvertisedAddress(String advertisedAddress) {
        this.advertisedAddress = advertisedAddress;
    }

    public boolean isWebSocketServiceEnabled() {
        return webSocketServiceEnabled;
    }

    public void setWebSocketServiceEnabled(boolean webSocketServiceEnabled) {
        this.webSocketServiceEnabled = webSocketServiceEnabled;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public boolean isFailureDomainsEnabled() {
        return failureDomainsEnabled;
    }

    public void setFailureDomainsEnabled(boolean failureDomainsEnabled) {
        this.failureDomainsEnabled = failureDomainsEnabled;
    }

    public long getBrokerShutdownTimeoutMs() {
        return brokerShutdownTimeoutMs;
    }

    public void setBrokerShutdownTimeoutMs(long brokerShutdownTimeoutMs) {
        this.brokerShutdownTimeoutMs = brokerShutdownTimeoutMs;
    }

    public boolean isBacklogQuotaCheckEnabled() {
        return backlogQuotaCheckEnabled;
    }

    public void setBacklogQuotaCheckEnabled(boolean backlogQuotaCheckEnabled) {
        this.backlogQuotaCheckEnabled = backlogQuotaCheckEnabled;
    }

    public int getBacklogQuotaCheckIntervalInSeconds() {
        return backlogQuotaCheckIntervalInSeconds;
    }

    public void setBacklogQuotaCheckIntervalInSeconds(int backlogQuotaCheckIntervalInSeconds) {
        this.backlogQuotaCheckIntervalInSeconds = backlogQuotaCheckIntervalInSeconds;
    }

    public long getBacklogQuotaDefaultLimitGB() {
        return backlogQuotaDefaultLimitGB;
    }

    public void setBacklogQuotaDefaultLimitGB(long backlogQuotaDefaultLimitGB) {
        this.backlogQuotaDefaultLimitGB = backlogQuotaDefaultLimitGB;
    }

    public boolean isBrokerDeleteInactiveTopicsEnabled() {
        return brokerDeleteInactiveTopicsEnabled;
    }

    public void setBrokerDeleteInactiveTopicsEnabled(boolean brokerDeleteInactiveTopicsEnabled) {
        this.brokerDeleteInactiveTopicsEnabled = brokerDeleteInactiveTopicsEnabled;
    }

    public int getBrokerDeduplicationMaxNumberOfProducers() {
        return brokerDeduplicationMaxNumberOfProducers;
    }

    public void setBrokerDeduplicationMaxNumberOfProducers(int brokerDeduplicationMaxNumberOfProducers) {
        this.brokerDeduplicationMaxNumberOfProducers = brokerDeduplicationMaxNumberOfProducers;
    }

    public int getBrokerDeduplicationEntriesInterval() {
        return brokerDeduplicationEntriesInterval;
    }

    public void setBrokerDeduplicationEntriesInterval(int brokerDeduplicationEntriesInterval) {
        this.brokerDeduplicationEntriesInterval = brokerDeduplicationEntriesInterval;
    }

    public int getBrokerDeduplicationProducerInactivityTimeoutMinutes() {
        return brokerDeduplicationProducerInactivityTimeoutMinutes;
    }

    public void setBrokerDeduplicationProducerInactivityTimeoutMinutes(
            int brokerDeduplicationProducerInactivityTimeoutMinutes) {
        this.brokerDeduplicationProducerInactivityTimeoutMinutes = brokerDeduplicationProducerInactivityTimeoutMinutes;
    }

    public int getDefaultNumberOfNamespaceBundles() {
        return defaultNumberOfNamespaceBundles;
    }

    public void setDefaultNumberOfNamespaceBundles(int defaultNumberOfNamespaceBundles) {
        this.defaultNumberOfNamespaceBundles = defaultNumberOfNamespaceBundles;
    }

    public long getBrokerDeleteInactiveTopicsFrequencySeconds() {
        return brokerDeleteInactiveTopicsFrequencySeconds;
    }

    public void setBrokerDeleteInactiveTopicsFrequencySeconds(long brokerDeleteInactiveTopicsFrequencySeconds) {
        this.brokerDeleteInactiveTopicsFrequencySeconds = brokerDeleteInactiveTopicsFrequencySeconds;
    }

    public int getMessageExpiryCheckIntervalInMinutes() {
        return messageExpiryCheckIntervalInMinutes;
    }

    public void setMessageExpiryCheckIntervalInMinutes(int messageExpiryCheckIntervalInMinutes) {
        this.messageExpiryCheckIntervalInMinutes = messageExpiryCheckIntervalInMinutes;
    }

    public boolean isBrokerDeduplicationEnabled() {
        return brokerDeduplicationEnabled;
    }

    public void setBrokerDeduplicationEnabled(boolean brokerDeduplicationEnabled) {
        this.brokerDeduplicationEnabled = brokerDeduplicationEnabled;
    }

    public int getActiveConsumerFailoverDelayTimeMillis() {
        return activeConsumerFailoverDelayTimeMillis;
    }

    public void setActiveConsumerFailoverDelayTimeMillis(int activeConsumerFailoverDelayTimeMillis) {
        this.activeConsumerFailoverDelayTimeMillis = activeConsumerFailoverDelayTimeMillis;
    }

    public long getSubscriptionExpirationTimeMinutes() {
        return subscriptionExpirationTimeMinutes;
    }

    public void setSubscriptionExpirationTimeMinutes(long subscriptionExpirationTimeMinutes) {
        this.subscriptionExpirationTimeMinutes = subscriptionExpirationTimeMinutes;
    }

    public long getSubscriptionExpiryCheckIntervalInMinutes() {
        return subscriptionExpiryCheckIntervalInMinutes;
    }

    public void setSubscriptionExpiryCheckIntervalInMinutes(long subscriptionExpiryCheckIntervalInMinutes) {
        this.subscriptionExpiryCheckIntervalInMinutes = subscriptionExpiryCheckIntervalInMinutes;
    }

    public boolean isClientLibraryVersionCheckEnabled() {
        return clientLibraryVersionCheckEnabled;
    }

    public void setClientLibraryVersionCheckEnabled(boolean clientLibraryVersionCheckEnabled) {
        this.clientLibraryVersionCheckEnabled = clientLibraryVersionCheckEnabled;
    }

    public String getStatusFilePath() {
        return statusFilePath;
    }

    public void setStatusFilePath(String statusFilePath) {
        this.statusFilePath = statusFilePath;
    }

    public int getMaxUnackedMessagesPerConsumer() {
        return maxUnackedMessagesPerConsumer;
    }

    public void setMaxUnackedMessagesPerConsumer(int maxUnackedMessagesPerConsumer) {
        this.maxUnackedMessagesPerConsumer = maxUnackedMessagesPerConsumer;
    }

    public int getMaxUnackedMessagesPerSubscription() {
        return maxUnackedMessagesPerSubscription;
    }

    public void setMaxUnackedMessagesPerSubscription(int maxUnackedMessagesPerSubscription) {
        this.maxUnackedMessagesPerSubscription = maxUnackedMessagesPerSubscription;
    }

    public int getMaxUnackedMessagesPerBroker() {
        return maxUnackedMessagesPerBroker;
    }

    public void setMaxUnackedMessagesPerBroker(int maxUnackedMessagesPerBroker) {
        this.maxUnackedMessagesPerBroker = maxUnackedMessagesPerBroker;
    }

    public double getMaxUnackedMessagesPerSubscriptionOnBrokerBlocked() {
        return maxUnackedMessagesPerSubscriptionOnBrokerBlocked;
    }

    public void setMaxUnackedMessagesPerSubscriptionOnBrokerBlocked(
            double maxUnackedMessagesPerSubscriptionOnBrokerBlocked) {
        this.maxUnackedMessagesPerSubscriptionOnBrokerBlocked = maxUnackedMessagesPerSubscriptionOnBrokerBlocked;
    }

    public int getDispatchThrottlingRatePerTopicInMsg() {
        return dispatchThrottlingRatePerTopicInMsg;
    }

    public void setDispatchThrottlingRatePerTopicInMsg(int dispatchThrottlingRatePerTopicInMsg) {
        this.dispatchThrottlingRatePerTopicInMsg = dispatchThrottlingRatePerTopicInMsg;
    }

    public long getDispatchThrottlingRatePerTopicInByte() {
        return dispatchThrottlingRatePerTopicInByte;
    }

    public void setDispatchThrottlingRatePerTopicInByte(long dispatchThrottlingRatePerTopicInByte) {
        this.dispatchThrottlingRatePerTopicInByte = dispatchThrottlingRatePerTopicInByte;
    }

    public int getDispatchThrottlingRatePerSubscriptionInMsg() {
        return dispatchThrottlingRatePerSubscriptionInMsg;
    }

    public void setDispatchThrottlingRatePerSubscriptionInMsg(int dispatchThrottlingRatePerSubscriptionInMsg) {
        this.dispatchThrottlingRatePerSubscriptionInMsg = dispatchThrottlingRatePerSubscriptionInMsg;
    }

    public long getDispatchThrottlingRatePerSubscribeInByte() {
        return dispatchThrottlingRatePerSubscribeInByte;
    }

    public void setDispatchThrottlingRatePerSubscribeInByte(long dispatchThrottlingRatePerSubscribeInByte) {
        this.dispatchThrottlingRatePerSubscribeInByte = dispatchThrottlingRatePerSubscribeInByte;
    }

    public boolean isDispatchThrottlingOnNonBacklogConsumerEnabled() {
        return dispatchThrottlingOnNonBacklogConsumerEnabled;
    }

    public void setDispatchThrottlingOnNonBacklogConsumerEnabled(boolean dispatchThrottlingOnNonBacklogConsumerEnabled) {
        this.dispatchThrottlingOnNonBacklogConsumerEnabled = dispatchThrottlingOnNonBacklogConsumerEnabled;
    }

    public int getMaxConcurrentLookupRequest() {
        return maxConcurrentLookupRequest;
    }

    public void setMaxConcurrentLookupRequest(int maxConcurrentLookupRequest) {
        this.maxConcurrentLookupRequest = maxConcurrentLookupRequest;
    }

    public int getMaxConcurrentTopicLoadRequest() {
        return maxConcurrentTopicLoadRequest;
    }

    public void setMaxConcurrentTopicLoadRequest(int maxConcurrentTopicLoadRequest) {
        this.maxConcurrentTopicLoadRequest = maxConcurrentTopicLoadRequest;
    }

    public int getMaxConcurrentNonPersistentMessagePerConnection() {
        return maxConcurrentNonPersistentMessagePerConnection;
    }

    public void setMaxConcurrentNonPersistentMessagePerConnection(int maxConcurrentNonPersistentMessagePerConnection) {
        this.maxConcurrentNonPersistentMessagePerConnection = maxConcurrentNonPersistentMessagePerConnection;
    }

    public int getNumWorkerThreadsForNonPersistentTopic() {
        return numWorkerThreadsForNonPersistentTopic;
    }

    public void setNumWorkerThreadsForNonPersistentTopic(int numWorkerThreadsForNonPersistentTopic) {
        this.numWorkerThreadsForNonPersistentTopic = numWorkerThreadsForNonPersistentTopic;
    }

    public boolean isEnablePersistentTopics() {
        return enablePersistentTopics;
    }

    public void setEnablePersistentTopics(boolean enablePersistentTopics) {
        this.enablePersistentTopics = enablePersistentTopics;
    }

    public boolean isEnableNonPersistentTopics() {
        return enableNonPersistentTopics;
    }

    public void setEnableNonPersistentTopics(boolean enableNonPersistentTopics) {
        this.enableNonPersistentTopics = enableNonPersistentTopics;
    }

    public boolean isEnableRunBookieTogether() {
        return enableRunBookieTogether;
    }

    public void setEnableRunBookieTogether(boolean enableRunBookieTogether) {
        this.enableRunBookieTogether = enableRunBookieTogether;
    }

    public boolean isEnableRunBookieAutoRecoveryTogether() {
        return enableRunBookieAutoRecoveryTogether;
    }

    public void setEnableRunBookieAutoRecoveryTogether(boolean enableRunBookieAutoRecoveryTogether) {
        this.enableRunBookieAutoRecoveryTogether = enableRunBookieAutoRecoveryTogether;
    }

    public int getMaxProducersPerTopic() {
        return maxProducersPerTopic;
    }

    public void setMaxProducersPerTopic(int maxProducersPerTopic) {
        this.maxProducersPerTopic = maxProducersPerTopic;
    }

    public int getMaxConsumersPerTopic() {
        return maxConsumersPerTopic;
    }

    public void setMaxConsumersPerTopic(int maxConsumersPerTopic) {
        this.maxConsumersPerTopic = maxConsumersPerTopic;
    }

    public int getMaxConsumersPerSubscription() {
        return maxConsumersPerSubscription;
    }

    public void setMaxConsumersPerSubscription(int maxConsumersPerSubscription) {
        this.maxConsumersPerSubscription = maxConsumersPerSubscription;
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    public void setTlsEnabled(boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
    }

    public String getTlsCertificateFilePath() {
        return tlsCertificateFilePath;
    }

    public void setTlsCertificateFilePath(String tlsCertificateFilePath) {
        this.tlsCertificateFilePath = tlsCertificateFilePath;
    }

    public String getTlsKeyFilePath() {
        return tlsKeyFilePath;
    }

    public void setTlsKeyFilePath(String tlsKeyFilePath) {
        this.tlsKeyFilePath = tlsKeyFilePath;
    }

    public String getTlsTrustCertsFilePath() {
        return tlsTrustCertsFilePath;
    }

    public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
    }

    public boolean isTlsAllowInsecureConnection() {
        return tlsAllowInsecureConnection;
    }

    public void setTlsAllowInsecureConnection(boolean tlsAllowInsecureConnection) {
        this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
    }

    public boolean isAuthenticationEnabled() {
        return authenticationEnabled;
    }

    public void setAuthenticationEnabled(boolean authenticationEnabled) {
        this.authenticationEnabled = authenticationEnabled;
    }

    public String getAuthorizationProvider() {
        return authorizationProvider;
    }

    public void setAuthorizationProvider(String authorizationProvider) {
        this.authorizationProvider = authorizationProvider;
    }

    public void setAuthenticationProviders(Set<String> providersClassNames) {
        authenticationProviders = providersClassNames;
    }

    public Set<String> getAuthenticationProviders() {
        return authenticationProviders;
    }

    public boolean isAuthorizationEnabled() {
        return authorizationEnabled;
    }

    public void setAuthorizationEnabled(boolean authorizationEnabled) {
        this.authorizationEnabled = authorizationEnabled;
    }

    public Set<String> getSuperUserRoles() {
        return superUserRoles;
    }

    public Set<String> getProxyRoles() {
        return proxyRoles;
    }

    public void setProxyRoles(Set<String> proxyRoles) {
        this.proxyRoles = proxyRoles;
    }

    public boolean getAuthorizationAllowWildcardsMatching() {
        return authorizationAllowWildcardsMatching;
    }

    public void setAuthorizationAllowWildcardsMatching(boolean authorizationAllowWildcardsMatching) {
        this.authorizationAllowWildcardsMatching = authorizationAllowWildcardsMatching;
    }

    public void setSuperUserRoles(Set<String> superUserRoles) {
        this.superUserRoles = superUserRoles;
    }

    public String getBrokerClientAuthenticationPlugin() {
        return brokerClientAuthenticationPlugin;
    }

    public void setBrokerClientAuthenticationPlugin(String brokerClientAuthenticationPlugin) {
        this.brokerClientAuthenticationPlugin = brokerClientAuthenticationPlugin;
    }

    public String getBrokerClientAuthenticationParameters() {
        return brokerClientAuthenticationParameters;
    }

    public void setBrokerClientAuthenticationParameters(String brokerClientAuthenticationParameters) {
        this.brokerClientAuthenticationParameters = brokerClientAuthenticationParameters;
    }

    public String getBrokerClientTrustCertsFilePath() {
        return brokerClientTrustCertsFilePath;
    }

    public void setBrokerClientTrustCertsFilePath(String brokerClientTrustCertsFilePath) {
        this.brokerClientTrustCertsFilePath = brokerClientTrustCertsFilePath;
    }

    public String getAnonymousUserRole() {
        return anonymousUserRole;
    }

    public void setAnonymousUserRole(String anonymousUserRole) {
        this.anonymousUserRole = anonymousUserRole;
    }

    public String getBookkeeperClientAuthenticationPlugin() {
        return bookkeeperClientAuthenticationPlugin;
    }

    public void setBookkeeperClientAuthenticationPlugin(String bookkeeperClientAuthenticationPlugin) {
        this.bookkeeperClientAuthenticationPlugin = bookkeeperClientAuthenticationPlugin;
    }

    public String getBookkeeperClientAuthenticationParametersName() {
        return bookkeeperClientAuthenticationParametersName;
    }

    public void setBookkeeperClientAuthenticationParametersName(String bookkeeperClientAuthenticationParametersName) {
        this.bookkeeperClientAuthenticationParametersName = bookkeeperClientAuthenticationParametersName;
    }

    public String getBookkeeperClientAuthenticationParameters() {
        return bookkeeperClientAuthenticationParameters;
    }

    public void setBookkeeperClientAuthenticationParameters(String bookkeeperClientAuthenticationParameters) {
        this.bookkeeperClientAuthenticationParameters = bookkeeperClientAuthenticationParameters;
    }

    public long getBookkeeperClientTimeoutInSeconds() {
        return bookkeeperClientTimeoutInSeconds;
    }

    public void setBookkeeperClientTimeoutInSeconds(long bookkeeperClientTimeoutInSeconds) {
        this.bookkeeperClientTimeoutInSeconds = bookkeeperClientTimeoutInSeconds;
    }

    public int getBookkeeperClientSpeculativeReadTimeoutInMillis() {
        return bookkeeperClientSpeculativeReadTimeoutInMillis;
    }

    public void setBookkeeperClientSpeculativeReadTimeoutInMillis(int bookkeeperClientSpeculativeReadTimeoutInMillis) {
        this.bookkeeperClientSpeculativeReadTimeoutInMillis = bookkeeperClientSpeculativeReadTimeoutInMillis;
    }

    public boolean isBookkeeperClientHealthCheckEnabled() {
        return bookkeeperClientHealthCheckEnabled;
    }

    public void setBookkeeperClientHealthCheckEnabled(boolean bookkeeperClientHealthCheckEnabled) {
        this.bookkeeperClientHealthCheckEnabled = bookkeeperClientHealthCheckEnabled;
    }

    public long getBookkeeperClientHealthCheckIntervalSeconds() {
        return bookkeeperClientHealthCheckIntervalSeconds;
    }

    public void setBookkeeperClientHealthCheckIntervalSeconds(long bookkeeperClientHealthCheckIntervalSeconds) {
        this.bookkeeperClientHealthCheckIntervalSeconds = bookkeeperClientHealthCheckIntervalSeconds;
    }

    public long getBookkeeperClientHealthCheckErrorThresholdPerInterval() {
        return bookkeeperClientHealthCheckErrorThresholdPerInterval;
    }

    public void setBookkeeperClientHealthCheckErrorThresholdPerInterval(
            long bookkeeperClientHealthCheckErrorThresholdPerInterval) {
        this.bookkeeperClientHealthCheckErrorThresholdPerInterval = bookkeeperClientHealthCheckErrorThresholdPerInterval;
    }

    public long getBookkeeperClientHealthCheckQuarantineTimeInSeconds() {
        return bookkeeperClientHealthCheckQuarantineTimeInSeconds;
    }

    public void setBookkeeperClientHealthCheckQuarantineTimeInSeconds(
            long bookkeeperClientHealthCheckQuarantineTimeInSeconds) {
        this.bookkeeperClientHealthCheckQuarantineTimeInSeconds = bookkeeperClientHealthCheckQuarantineTimeInSeconds;
    }

    public boolean isBookkeeperClientRackawarePolicyEnabled() {
        return bookkeeperClientRackawarePolicyEnabled;
    }

    public void setBookkeeperClientRackawarePolicyEnabled(boolean bookkeeperClientRackawarePolicyEnabled) {
        this.bookkeeperClientRackawarePolicyEnabled = bookkeeperClientRackawarePolicyEnabled;
    }

    public String getBookkeeperClientIsolationGroups() {
        return bookkeeperClientIsolationGroups;
    }

    public void setBookkeeperClientIsolationGroups(String bookkeeperClientIsolationGroups) {
        this.bookkeeperClientIsolationGroups = bookkeeperClientIsolationGroups;
    }

    public int getManagedLedgerDefaultEnsembleSize() {
        return managedLedgerDefaultEnsembleSize;
    }

    public void setManagedLedgerDefaultEnsembleSize(int managedLedgerDefaultEnsembleSize) {
        this.managedLedgerDefaultEnsembleSize = managedLedgerDefaultEnsembleSize;
    }

    public int getManagedLedgerDefaultWriteQuorum() {
        return managedLedgerDefaultWriteQuorum;
    }

    public void setManagedLedgerDefaultWriteQuorum(int managedLedgerDefaultWriteQuorum) {
        this.managedLedgerDefaultWriteQuorum = managedLedgerDefaultWriteQuorum;
    }

    public int getManagedLedgerDefaultAckQuorum() {
        return managedLedgerDefaultAckQuorum;
    }

    public void setManagedLedgerDefaultAckQuorum(int managedLedgerDefaultAckQuorum) {
        this.managedLedgerDefaultAckQuorum = managedLedgerDefaultAckQuorum;
    }

    public DigestType getManagedLedgerDigestType() {
        return managedLedgerDigestType;
    }

    public void setManagedLedgerDigestType(DigestType managedLedgerDigestType) {
        this.managedLedgerDigestType = managedLedgerDigestType;
    }

    public int getManagedLedgerMaxEnsembleSize() {
        return managedLedgerMaxEnsembleSize;
    }

    public void setManagedLedgerMaxEnsembleSize(int managedLedgerMaxEnsembleSize) {
        this.managedLedgerMaxEnsembleSize = managedLedgerMaxEnsembleSize;
    }

    public int getManagedLedgerMaxWriteQuorum() {
        return managedLedgerMaxWriteQuorum;
    }

    public void setManagedLedgerMaxWriteQuorum(int managedLedgerMaxWriteQuorum) {
        this.managedLedgerMaxWriteQuorum = managedLedgerMaxWriteQuorum;
    }

    public int getManagedLedgerMaxAckQuorum() {
        return managedLedgerMaxAckQuorum;
    }

    public void setManagedLedgerMaxAckQuorum(int managedLedgerMaxAckQuorum) {
        this.managedLedgerMaxAckQuorum = managedLedgerMaxAckQuorum;
    }

    public int getManagedLedgerCacheSizeMB() {
        return managedLedgerCacheSizeMB;
    }

    public void setManagedLedgerCacheSizeMB(int managedLedgerCacheSizeMB) {
        this.managedLedgerCacheSizeMB = managedLedgerCacheSizeMB;
    }

    public double getManagedLedgerCacheEvictionWatermark() {
        return managedLedgerCacheEvictionWatermark;
    }

    public void setManagedLedgerCacheEvictionWatermark(double managedLedgerCacheEvictionWatermark) {
        this.managedLedgerCacheEvictionWatermark = managedLedgerCacheEvictionWatermark;
    }

    public double getManagedLedgerDefaultMarkDeleteRateLimit() {
        return managedLedgerDefaultMarkDeleteRateLimit;
    }

    public void setManagedLedgerDefaultMarkDeleteRateLimit(double managedLedgerDefaultMarkDeleteRateLimit) {
        this.managedLedgerDefaultMarkDeleteRateLimit = managedLedgerDefaultMarkDeleteRateLimit;
    }

    public int getManagedLedgerMaxEntriesPerLedger() {
        return managedLedgerMaxEntriesPerLedger;
    }

    public void setManagedLedgerMaxEntriesPerLedger(int managedLedgerMaxEntriesPerLedger) {
        this.managedLedgerMaxEntriesPerLedger = managedLedgerMaxEntriesPerLedger;
    }

    public int getManagedLedgerMinLedgerRolloverTimeMinutes() {
        return managedLedgerMinLedgerRolloverTimeMinutes;
    }

    public void setManagedLedgerMinLedgerRolloverTimeMinutes(int managedLedgerMinLedgerRolloverTimeMinutes) {
        this.managedLedgerMinLedgerRolloverTimeMinutes = managedLedgerMinLedgerRolloverTimeMinutes;
    }

    public int getManagedLedgerMaxLedgerRolloverTimeMinutes() {
        return managedLedgerMaxLedgerRolloverTimeMinutes;
    }

    public void setManagedLedgerMaxLedgerRolloverTimeMinutes(int managedLedgerMaxLedgerRolloverTimeMinutes) {
        this.managedLedgerMaxLedgerRolloverTimeMinutes = managedLedgerMaxLedgerRolloverTimeMinutes;
    }

    public long getManagedLedgerOffloadDeletionLagMs() {
        return managedLedgerOffloadDeletionLagMs;
    }

    public void setManagedLedgerOffloadDeletionLag(long amount, TimeUnit unit) {
        this.managedLedgerOffloadDeletionLagMs = unit.toMillis(amount);
    }

    public int getManagedLedgerCursorMaxEntriesPerLedger() {
        return managedLedgerCursorMaxEntriesPerLedger;
    }

    public void setManagedLedgerCursorMaxEntriesPerLedger(int managedLedgerCursorMaxEntriesPerLedger) {
        this.managedLedgerCursorMaxEntriesPerLedger = managedLedgerCursorMaxEntriesPerLedger;
    }

    public int getManagedLedgerCursorRolloverTimeInSeconds() {
        return managedLedgerCursorRolloverTimeInSeconds;
    }

    public void setManagedLedgerCursorRolloverTimeInSeconds(int managedLedgerCursorRolloverTimeInSeconds) {
        this.managedLedgerCursorRolloverTimeInSeconds = managedLedgerCursorRolloverTimeInSeconds;
    }

    public int getManagedLedgerMaxUnackedRangesToPersist() {
        return managedLedgerMaxUnackedRangesToPersist;
    }

    public void setManagedLedgerMaxUnackedRangesToPersist(int managedLedgerMaxUnackedRangesToPersist) {
        this.managedLedgerMaxUnackedRangesToPersist = managedLedgerMaxUnackedRangesToPersist;
    }

    public int getManagedLedgerMaxUnackedRangesToPersistInZooKeeper() {
        return managedLedgerMaxUnackedRangesToPersistInZooKeeper;
    }

    public void setManagedLedgerMaxUnackedRangesToPersistInZooKeeper(
            int managedLedgerMaxUnackedRangesToPersistInZookeeper) {
        this.managedLedgerMaxUnackedRangesToPersistInZooKeeper = managedLedgerMaxUnackedRangesToPersistInZookeeper;
    }

    public boolean isAutoSkipNonRecoverableData() {
        return autoSkipNonRecoverableData;
    }

    public void setAutoSkipNonRecoverableData(boolean skipNonRecoverableLedger) {
        this.autoSkipNonRecoverableData = skipNonRecoverableLedger;
    }

    public boolean isLoadBalancerEnabled() {
        return loadBalancerEnabled;
    }

    public void setLoadBalancerEnabled(boolean loadBalancerEnabled) {
        this.loadBalancerEnabled = loadBalancerEnabled;
    }

    public void setLoadBalancerPlacementStrategy(String placementStrategy) {
        this.loadBalancerPlacementStrategy = placementStrategy;
    }

    public String getLoadBalancerPlacementStrategy() {
        return this.loadBalancerPlacementStrategy;
    }

    public int getLoadBalancerReportUpdateThresholdPercentage() {
        return loadBalancerReportUpdateThresholdPercentage;
    }

    public void setLoadBalancerReportUpdateThresholdPercentage(int loadBalancerReportUpdateThresholdPercentage) {
        this.loadBalancerReportUpdateThresholdPercentage = loadBalancerReportUpdateThresholdPercentage;
    }

    public int getLoadBalancerReportUpdateMaxIntervalMinutes() {
        return loadBalancerReportUpdateMaxIntervalMinutes;
    }

    public void setLoadBalancerReportUpdateMaxIntervalMinutes(int loadBalancerReportUpdateMaxIntervalMinutes) {
        this.loadBalancerReportUpdateMaxIntervalMinutes = loadBalancerReportUpdateMaxIntervalMinutes;
    }

    public int getLoadBalancerHostUsageCheckIntervalMinutes() {
        return loadBalancerHostUsageCheckIntervalMinutes;
    }

    public void setLoadBalancerHostUsageCheckIntervalMinutes(int loadBalancerHostUsageCheckIntervalMinutes) {
        this.loadBalancerHostUsageCheckIntervalMinutes = loadBalancerHostUsageCheckIntervalMinutes;
    }

    public boolean isLoadBalancerSheddingEnabled() {
        return loadBalancerSheddingEnabled;
    }

    public void setLoadBalancerSheddingEnabled(boolean loadBalancerSheddingEnabled) {
        this.loadBalancerSheddingEnabled = loadBalancerSheddingEnabled;
    }

    public int getLoadBalancerSheddingIntervalMinutes() {
        return loadBalancerSheddingIntervalMinutes;
    }

    public void setLoadBalancerSheddingIntervalMinutes(int loadBalancerSheddingIntervalMinutes) {
        this.loadBalancerSheddingIntervalMinutes = loadBalancerSheddingIntervalMinutes;
    }

    public long getLoadBalancerSheddingGracePeriodMinutes() {
        return loadBalancerSheddingGracePeriodMinutes;
    }

    public void setLoadBalancerSheddingGracePeriodMinutes(long loadBalancerSheddingGracePeriodMinutes) {
        this.loadBalancerSheddingGracePeriodMinutes = loadBalancerSheddingGracePeriodMinutes;
    }

    public int getLoadBalancerResourceQuotaUpdateIntervalMinutes() {
        return this.loadBalancerResourceQuotaUpdateIntervalMinutes;
    }

    public void setLoadBalancerResourceQuotaUpdateIntervalMinutes(int interval) {
        this.loadBalancerResourceQuotaUpdateIntervalMinutes = interval;
    }

    public int getLoadBalancerBrokerUnderloadedThresholdPercentage() {
        return loadBalancerBrokerUnderloadedThresholdPercentage;
    }

    public void setLoadBalancerBrokerUnderloadedThresholdPercentage(
            int loadBalancerBrokerUnderloadedThresholdPercentage) {
        this.loadBalancerBrokerUnderloadedThresholdPercentage = loadBalancerBrokerUnderloadedThresholdPercentage;
    }

    public int getLoadBalancerBrokerOverloadedThresholdPercentage() {
        return loadBalancerBrokerOverloadedThresholdPercentage;
    }

    public int getLoadBalancerBrokerMaxTopics() {
        return loadBalancerBrokerMaxTopics;
    }

    public void setLoadBalancerBrokerMaxTopics(int loadBalancerBrokerMaxTopics) {
        this.loadBalancerBrokerMaxTopics = loadBalancerBrokerMaxTopics;
    }

    public void setLoadBalancerNamespaceBundleMaxTopics(int topics) {
        this.loadBalancerNamespaceBundleMaxTopics = topics;
    }

    public int getLoadBalancerNamespaceBundleMaxTopics() {
        return this.loadBalancerNamespaceBundleMaxTopics;
    }

    public void setLoadBalancerNamespaceBundleMaxSessions(int sessions) {
        this.loadBalancerNamespaceBundleMaxSessions = sessions;
    }

    public int getLoadBalancerNamespaceBundleMaxSessions() {
        return this.loadBalancerNamespaceBundleMaxSessions;
    }

    public void setLoadBalancerNamespaceBundleMaxMsgRate(int msgRate) {
        this.loadBalancerNamespaceBundleMaxMsgRate = msgRate;
    }

    public int getLoadBalancerNamespaceBundleMaxMsgRate() {
        return this.loadBalancerNamespaceBundleMaxMsgRate;
    }

    public void setLoadBalancerNamespaceBundleMaxBandwidthMbytes(int bandwidth) {
        this.loadBalancerNamespaceBundleMaxBandwidthMbytes = bandwidth;
    }

    public int getLoadBalancerNamespaceBundleMaxBandwidthMbytes() {
        return this.loadBalancerNamespaceBundleMaxBandwidthMbytes;
    }

    public void setLoadBalancerBrokerOverloadedThresholdPercentage(
            int loadBalancerBrokerOverloadedThresholdPercentage) {
        this.loadBalancerBrokerOverloadedThresholdPercentage = loadBalancerBrokerOverloadedThresholdPercentage;
    }

    public int getLoadBalancerBrokerComfortLoadLevelPercentage() {
        return this.loadBalancerBrokerComfortLoadLevelPercentage;
    }

    public void setLoadBalancerBrokerComfortLoadLevelPercentage(int percentage) {
        this.loadBalancerBrokerComfortLoadLevelPercentage = percentage;
    }

    public boolean isLoadBalancerAutoBundleSplitEnabled() {
        return this.loadBalancerAutoBundleSplitEnabled;
    }

    public void setLoadBalancerAutoBundleSplitEnabled(boolean enabled) {
        this.loadBalancerAutoBundleSplitEnabled = enabled;
    }

    public boolean isLoadBalancerAutoUnloadSplitBundlesEnabled() {
        return loadBalancerAutoUnloadSplitBundlesEnabled;
    }

    public void setLoadBalancerAutoUnloadSplitBundlesEnabled(boolean loadBalancerAutoUnloadSplitBundlesEnabled) {
        this.loadBalancerAutoUnloadSplitBundlesEnabled = loadBalancerAutoUnloadSplitBundlesEnabled;
    }

    public void setLoadBalancerNamespaceMaximumBundles(int bundles) {
        this.loadBalancerNamespaceMaximumBundles = bundles;
    }

    public int getLoadBalancerNamespaceMaximumBundles() {
        return this.loadBalancerNamespaceMaximumBundles;
    }

    public Optional<Double> getLoadBalancerOverrideBrokerNicSpeedGbps() {
        return Optional.ofNullable(loadBalancerOverrideBrokerNicSpeedGbps);
    }

    public void setLoadBalancerOverrideBrokerNicSpeedGbps(double loadBalancerOverrideBrokerNicSpeedGbps) {
        this.loadBalancerOverrideBrokerNicSpeedGbps = loadBalancerOverrideBrokerNicSpeedGbps;
    }

    public boolean isReplicationMetricsEnabled() {
        return replicationMetricsEnabled;
    }

    public void setReplicationMetricsEnabled(boolean replicationMetricsEnabled) {
        this.replicationMetricsEnabled = replicationMetricsEnabled;
    }

    public int getReplicationConnectionsPerBroker() {
        return replicationConnectionsPerBroker;
    }

    public void setReplicationConnectionsPerBroker(int replicationConnectionsPerBroker) {
        this.replicationConnectionsPerBroker = replicationConnectionsPerBroker;
    }

    public int getReplicationProducerQueueSize() {
        return replicationProducerQueueSize;
    }

    public void setReplicationProducerQueueSize(int replicationProducerQueueSize) {
        this.replicationProducerQueueSize = replicationProducerQueueSize;
    }

    public boolean isReplicationTlsEnabled() {
        return replicationTlsEnabled;
    }

    public void setReplicationTlsEnabled(boolean replicationTlsEnabled) {
        this.replicationTlsEnabled = replicationTlsEnabled;
    }

    public List<String> getBootstrapNamespaces() {
        return bootstrapNamespaces;
    }

    public Object getProperty(String key) {
        return properties.get(key);
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public int getDefaultRetentionTimeInMinutes() {
        return defaultRetentionTimeInMinutes;
    }

    public void setDefaultRetentionTimeInMinutes(int defaultRetentionTimeInMinutes) {
        this.defaultRetentionTimeInMinutes = defaultRetentionTimeInMinutes;
    }

    public int getDefaultRetentionSizeInMB() {
        return defaultRetentionSizeInMB;
    }

    public void setDefaultRetentionSizeInMB(int defaultRetentionSizeInMB) {
        this.defaultRetentionSizeInMB = defaultRetentionSizeInMB;
    }

    public int getBookkeeperHealthCheckIntervalSec() {
        return (int) bookkeeperClientHealthCheckIntervalSeconds;
    }

    public void setBootstrapNamespaces(List<String> bootstrapNamespaces) {
        this.bootstrapNamespaces = bootstrapNamespaces;
    }

    public void setBrokerServicePurgeInactiveFrequencyInSeconds(int brokerServicePurgeInactiveFrequencyInSeconds) {
        this.brokerServicePurgeInactiveFrequencyInSeconds = brokerServicePurgeInactiveFrequencyInSeconds;
    }

    public int getKeepAliveIntervalSeconds() {
        return keepAliveIntervalSeconds;
    }

    public void setKeepAliveIntervalSeconds(int keepAliveIntervalSeconds) {
        this.keepAliveIntervalSeconds = keepAliveIntervalSeconds;
    }

    public int getBrokerServicePurgeInactiveFrequencyInSeconds() {
        return brokerServicePurgeInactiveFrequencyInSeconds;
    }

    public long getZooKeeperSessionTimeoutMillis() {
        return zooKeeperSessionTimeoutMillis;
    }

    public void setZooKeeperSessionTimeoutMillis(long zooKeeperSessionTimeoutMillis) {
        this.zooKeeperSessionTimeoutMillis = zooKeeperSessionTimeoutMillis;
    }

    public String getReplicatorPrefix() {
        return replicatorPrefix;
    }

    public void setReplicatorPrefix(String replicatorPrefix) {
        this.replicatorPrefix = replicatorPrefix;
    }

    public String getLoadManagerClassName() {
        return loadManagerClassName;
    }

    public void setLoadManagerClassName(String loadManagerClassName) {
        this.loadManagerClassName = loadManagerClassName;
    }

    public boolean isPreferLaterVersions() {
        return preferLaterVersions;
    }

    public void setPreferLaterVersions(boolean preferLaterVersions) {
        this.preferLaterVersions = preferLaterVersions;
    }

    public int getWebSocketNumIoThreads() { return webSocketNumIoThreads; }

    public void setWebSocketNumIoThreads(int webSocketNumIoThreads) { this.webSocketNumIoThreads = webSocketNumIoThreads; }

    public int getWebSocketConnectionsPerBroker() { return webSocketConnectionsPerBroker; }

    public void setWebSocketConnectionsPerBroker(int webSocketConnectionsPerBroker) { this.webSocketConnectionsPerBroker = webSocketConnectionsPerBroker; }

    public int getWebSocketSessionIdleTimeoutMillis() {
        return webSocketSessionIdleTimeoutMillis;
    }

    public void setWebSocketSessionIdleTimeoutMillis(int webSocketSessionIdleTimeoutMillis) {
        this.webSocketSessionIdleTimeoutMillis = webSocketSessionIdleTimeoutMillis;
    }

    public boolean exposeTopicLevelMetricsInPrometheus() {
        return exposeTopicLevelMetricsInPrometheus;
    }

    public void setExposeTopicLevelMetricsInPrometheus(boolean exposeTopicLevelMetricsInPrometheus) {
        this.exposeTopicLevelMetricsInPrometheus = exposeTopicLevelMetricsInPrometheus;
    }

    public String getSchemaRegistryStorageClassName() {
       return schemaRegistryStorageClassName;
    }

    public void setSchemaRegistryStorageClassName(String className) {
        schemaRegistryStorageClassName = className;
    }

    public Set<String> getSchemaRegistryCompatibilityCheckers() {
        return schemaRegistryCompatibilityCheckers;
    }

    public void setSchemaRegistryCompatibilityCheckers(Set<String> schemaRegistryCompatibilityCheckers) {
        this.schemaRegistryCompatibilityCheckers = schemaRegistryCompatibilityCheckers;
    }

    public boolean authenticateOriginalAuthData() {
        return authenticateOriginalAuthData;
    }

    public void setAuthenticateOriginalAuthData(boolean authenticateOriginalAuthData) {
        this.authenticateOriginalAuthData = authenticateOriginalAuthData;
    }

    public Set<String> getTlsProtocols() {
        return tlsProtocols;
    }

    public void setTlsProtocols(Set<String> tlsProtocols) {
        this.tlsProtocols = tlsProtocols;
    }

    public Set<String> getTlsCiphers() {
        return tlsCiphers;
    }

    public void setTlsCiphers(Set<String> tlsCiphers) {
        this.tlsCiphers = tlsCiphers;
    }

    public boolean getTlsRequireTrustedClientCertOnConnect() {
        return tlsRequireTrustedClientCertOnConnect;
    }

    public void setTlsRequireTrustedClientCertOnConnect(boolean tlsRequireTrustedClientCertOnConnect) {
        this.tlsRequireTrustedClientCertOnConnect = tlsRequireTrustedClientCertOnConnect;
    }
    /**** --- Function ---- ****/

    public void setFunctionsWorkerEnabled(boolean enabled) {
        this.functionsWorkerEnabled = enabled;
    }

    public boolean isFunctionsWorkerEnabled() {
        return functionsWorkerEnabled;
    }


    /**** --- Broker Web Stats ---- ****/

    public void setExposePublisherStats(boolean expose) {
        this.exposePublisherStats = expose;
    }

    public boolean exposePublisherStats() {
        return exposePublisherStats;
    }

    public boolean isRunningStandalone() {
        return isRunningStandalone;
    }

    public void setRunningStandalone(boolean isRunningStandalone) {
        this.isRunningStandalone = isRunningStandalone;
    }

    /**** --- Ledger Offload ---- ****/
    public void setManagedLedgerOffloadDriver(String driver) {
        this.managedLedgerOffloadDriver = driver;
    }

    public String getManagedLedgerOffloadDriver() {
        return this.managedLedgerOffloadDriver;
    }

    public void setManagedLedgerOffloadMaxThreads(int maxThreads) {
        this.managedLedgerOffloadMaxThreads = maxThreads;
    }

    public int getManagedLedgerOffloadMaxThreads() {
        return this.managedLedgerOffloadMaxThreads;
    }

    public void setS3ManagedLedgerOffloadRegion(String region) {
        this.s3ManagedLedgerOffloadRegion = region;
    }

    public String getS3ManagedLedgerOffloadRegion() {
        return this.s3ManagedLedgerOffloadRegion;
    }

    public void setS3ManagedLedgerOffloadBucket(String bucket) {
        this.s3ManagedLedgerOffloadBucket = bucket;
    }

    public String getS3ManagedLedgerOffloadBucket() {
        return this.s3ManagedLedgerOffloadBucket;
    }

    public void setS3ManagedLedgerOffloadServiceEndpoint(String endpoint) {
        this.s3ManagedLedgerOffloadServiceEndpoint = endpoint;
    }

    public String getS3ManagedLedgerOffloadServiceEndpoint() {
        return this.s3ManagedLedgerOffloadServiceEndpoint;
    }

    public void setS3ManagedLedgerOffloadMaxBlockSizeInBytes(int blockSizeInBytes) {
        this.s3ManagedLedgerOffloadMaxBlockSizeInBytes = blockSizeInBytes;
    }

    public int getS3ManagedLedgerOffloadMaxBlockSizeInBytes() {
        return this.s3ManagedLedgerOffloadMaxBlockSizeInBytes;
    }

    public void setS3ManagedLedgerOffloadReadBufferSizeInBytes(int readBufferSizeInBytes) {
        this.s3ManagedLedgerOffloadReadBufferSizeInBytes = readBufferSizeInBytes;
    }

    public int getS3ManagedLedgerOffloadReadBufferSizeInBytes() {
        return this.s3ManagedLedgerOffloadReadBufferSizeInBytes;
    }

    public void setBrokerServiceCompactionMonitorIntervalInSeconds(int interval) {
        this.brokerServiceCompactionMonitorIntervalInSeconds = interval;
    }

    public int getBrokerServiceCompactionMonitorIntervalInSeconds() {
        return this.brokerServiceCompactionMonitorIntervalInSeconds;
    }
}
