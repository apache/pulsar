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


import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.policies.data.BacklogQuota;

/**
 * Pulsar service configuration object.
 */
@Getter
@Setter
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
    private Integer brokerServicePort = 6650;
    private Integer brokerServicePortTls = null;
    // Port to use to server HTTP request
    private Integer webServicePort = 8080;
    // Port to use to server HTTPS request
    private Integer webServicePortTls = null;

    // Hostname or IP address the service binds on.
    private String bindAddress = "0.0.0.0";

    // Controls which hostname is advertised to the discovery service via ZooKeeper.
    private String advertisedAddress;

    // Number of threads to use for Netty IO
    private int numIOThreads = 2 * Runtime.getRuntime().availableProcessors();

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
    private long brokerShutdownTimeoutMs = 60000;
    // Enable backlog quota check. Enforces action on topic when the quota is
    // reached
    private boolean backlogQuotaCheckEnabled = true;
    // How often to check for topics that have reached the quota
    private int backlogQuotaCheckIntervalInSeconds = 60;
    // Default per-topic backlog quota limit
    private long backlogQuotaDefaultLimitGB = 50;
    //Default backlog quota retention policy. Default is producer_request_hold
    //'producer_request_hold' Policy which holds producer's send request until the resource becomes available (or holding times out)
    //'producer_exception' Policy which throws javax.jms.ResourceAllocationException to the producer
    //'consumer_backlog_eviction' Policy which evicts the oldest message from the slowest consumer's backlog
    private BacklogQuota.RetentionPolicy backlogQuotaDefaultRetentionPolicy = BacklogQuota.RetentionPolicy.producer_request_hold;
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
    // Too many subscribe requests from a consumer can cause broker rewinding consumer cursors and loading data from bookies,
    // hence causing high network bandwidth usage
    // When the positive value is set, broker will throttle the subscribe requests for one consumer.
    // Otherwise, the throttling will be disabled. The default value of this setting is 0 - throttling is disabled.
    @FieldContext(dynamic = true)
    private int subscribeThrottlingRatePerConsumer = 0;
    // Rate period for {subscribeThrottlingRatePerConsumer}. Default is 30s.
    @FieldContext(minValue = 1, dynamic = true)
    private int subscribeRatePeriodPerConsumerInSecond = 30;
    // Default number of message dispatching throttling-limit for every topic. Using a value of 0, is disabling default
    // message dispatch-throttling
    @FieldContext(dynamic = true)
    private int dispatchThrottlingRatePerTopicInMsg = 0;
    // Default number of message-bytes dispatching throttling-limit for every topic. Using a value of 0, is disabling
    // default message-byte dispatch-throttling
    @FieldContext(dynamic = true)
    private long dispatchThrottlingRatePerTopicInByte = 0;
    // Default number of message dispatching throttling-limit for a subscription.
    // Using a value of 0, is disabling default message dispatch-throttling.
    @FieldContext(dynamic = true)
    private int dispatchThrottlingRatePerSubscriptionInMsg = 0;
    // Default number of message-bytes dispatching throttling-limit for a subscription.
    // Using a value of 0, is disabling default message-byte dispatch-throttling.
    @FieldContext(dynamic = true)
    private long dispatchThrottlingRatePerSubscribeInByte = 0;
    // Default dispatch-throttling is disabled for consumers which already caught-up with published messages and
    // don't have backlog. This enables dispatch-throttling for non-backlog consumers as well.
    @FieldContext(dynamic = true)
    private boolean dispatchThrottlingOnNonBacklogConsumerEnabled = false;
    // Max number of concurrent lookup request broker allows to throttle heavy incoming lookup traffic
    @FieldContext(dynamic = true)
    private int maxConcurrentLookupRequest = 50000;
    // Max number of concurrent topic loading request broker allows to control number of zk-operations
    @FieldContext(dynamic = true)
    private int maxConcurrentTopicLoadRequest = 5000;
    // Max concurrent non-persistent message can be processed per connection
    private int maxConcurrentNonPersistentMessagePerConnection = 1000;
    // Number of worker threads to serve non-persistent topic
    private int numWorkerThreadsForNonPersistentTopic = Runtime.getRuntime().availableProcessors();;

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
    @Deprecated
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
    // Use older Bookkeeper wire protocol with bookie
    private boolean bookkeeperUseV2WireProtocol = true;
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
    // Enable region-aware bookie selection policy. BK will chose bookies from
    // different regions and racks when forming a new bookie ensemble
    private boolean bookkeeperClientRegionawarePolicyEnabled = false;
    // Enable/disable reordering read sequence on reading entries.
    private boolean bookkeeperClientReorderReadSequenceEnabled = false;
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

    // Number of threads to be used for managed ledger tasks dispatching
    private int managedLedgerNumWorkerThreads = Runtime.getRuntime().availableProcessors();
    // Number of threads to be used for managed ledger scheduled tasks
    private int managedLedgerNumSchedulerThreads = Runtime.getRuntime().availableProcessors();

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
    // operation timeout while updating managed-ledger metadata.
    private long managedLedgerMetadataOperationsTimeoutSeconds = 60;

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
    // @deprecated - Use brokerClientTlsEnabled instead.
    @Deprecated
    private boolean replicationTlsEnabled = false;
    // Enable TLS when talking with other brokers in the same cluster (admin operation) or different clusters (replication)
    private boolean brokerClientTlsEnabled = false;

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

    private boolean isSchemaValidationEnforced = false;
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
    private boolean exposeConsumerLevelMetricsInPrometheus = false;

    /**** --- Functions --- ****/
    private boolean functionsWorkerEnabled = false;

    /**** --- Broker Web Stats --- ****/
    // If true, export publisher stats when returning topics stats from the admin rest api
    private boolean exposePublisherStats = true;
    private int statsUpdateFrequencyInSecs = 60;
    private int statsUpdateInitialDelayInSecs = 60;

    /**** --- Ledger Offloading --- ****/
    /****
     * NOTES: all implementation related settings should be put in implementation package.
     *        only common settings like driver name, io threads can be added here.
     ****/
    // The directory to locate offloaders
    private String offloadersDirectory = "./offloaders";

    // Driver to use to offload old data to long term storage
    private String managedLedgerOffloadDriver = null;

    // Maximum number of thread pool threads for ledger offloading
    private int managedLedgerOffloadMaxThreads = 2;

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

    public Optional<Double> getLoadBalancerOverrideBrokerNicSpeedGbps() {
        return Optional.ofNullable(loadBalancerOverrideBrokerNicSpeedGbps);
    }

    public int getBookkeeperHealthCheckIntervalSec() {
        return (int) bookkeeperClientHealthCheckIntervalSeconds;
    }
    
    public Optional<Integer> getBrokerServicePort() {
        return Optional.ofNullable(brokerServicePort);
    }
    
    public Optional<Integer> getBrokerServicePortTls() {
        return Optional.ofNullable(brokerServicePortTls);
    }
    
    public Optional<Integer> getWebServicePort() {
        return Optional.ofNullable(webServicePort);
    }

    public Optional<Integer> getWebServicePortTls() {
        return Optional.ofNullable(webServicePortTls);
    }
}
