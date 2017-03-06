/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.Sets;
import com.yahoo.pulsar.client.impl.auth.AuthenticationDisabled;
import com.yahoo.pulsar.common.configuration.FieldContext;
import com.yahoo.pulsar.common.configuration.PulsarConfiguration;

/**
 *
 * Pulsar service configuration object.
 *
 */
public class ServiceConfiguration implements PulsarConfiguration{

    /***** --- pulsar configuration --- ****/
    // Zookeeper quorum connection string
    @FieldContext(required = true)
    private String zookeeperServers;
    // Global Zookeeper quorum connection string
    @FieldContext(required = false)
    private String globalZookeeperServers;
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

    // Name of the cluster to which this broker belongs to
    @FieldContext(required = true)
    private String clusterName;
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
    // Enable check for minimum allowed client library version
    private boolean clientLibraryVersionCheckEnabled = false;
    // Allow client libraries with no version information
    private boolean clientLibraryVersionCheckAllowUnversioned = true;
    // Path for the file used to determine the rotation status for the broker
    // when responding to service discovery health checks
    private String statusFilePath;
    // Max number of unacknowledged messages allowed to receive messages by a consumer on a shared subscription. Broker will stop sending
    // messages to consumer once, this limit reaches until consumer starts acknowledging messages back
    // Using a value of 0, is disabling unackedMessage-limit check and consumer can receive messages without any restriction
    private int maxUnackedMessagesPerConsumer = 50000;
    // Max number of concurrent lookup request broker allows to throttle heavy incoming lookup traffic
    @FieldContext(dynamic = true)
    private int maxConcurrentLookupRequest = 10000;

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

    /***** --- Authentication --- ****/
    // Enable authentication
    private boolean authenticationEnabled = false;
    // Autentication provider name list, which is a list of class names
    private Set<String> authenticationProviders = Sets.newTreeSet();

    // Enforce authorization
    private boolean authorizationEnabled = false;

    // Role names that are treated as "super-user", meaning they will be able to
    // do all admin operations and publish/consume from all topics
    private Set<String> superUserRoles = Sets.newTreeSet();

    // Authentication settings of the broker itself. Used when the broker connects
    // to other brokers, either in same or other clusters
    private String brokerClientAuthenticationPlugin = AuthenticationDisabled.class.getName();
    private String brokerClientAuthenticationParameters = "";

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
    // Amount of memory to use for caching data payload in managed ledger. This
    // memory
    // is allocated from JVM direct memory and it's shared across all the topics
    // running in the same broker
    private int managedLedgerCacheSizeMB = 1024;
    // Threshold to which bring down the cache level when eviction is triggered
    private double managedLedgerCacheEvictionWatermark = 0.9f;
    // Rate limit the amount of writes generated by consumer acking the messages
    private double managedLedgerDefaultMarkDeleteRateLimit = 0.1;
    // Max number of entries to append to a ledger before triggering a rollover
    // A ledger rollover is triggered on these conditions Either the max
    // rollover time has been reached or max entries have been written to the
    // ledged and at least min-time has passed
    private int managedLedgerMaxEntriesPerLedger = 50000;
    // Minimum time between ledger rollover for a topic
    private int managedLedgerMinLedgerRolloverTimeMinutes = 10;
    // Maximum time before forcing a ledger rollover for a topic
    private int managedLedgerMaxLedgerRolloverTimeMinutes = 240;
    // Max number of entries to append to a cursor ledger
    private int managedLedgerCursorMaxEntriesPerLedger = 50000;
    // Max time before triggering a rollover on a cursor ledger
    private int managedLedgerCursorRolloverTimeInSeconds = 14400;

    /*** --- Load balancer --- ****/
    // Enable load balancer
    private boolean loadBalancerEnabled = false;
    // load placement strategy
    private String loadBalancerPlacementStrategy = "weightedRandomSelection"; // weighted random selection
    // Percentage of change to trigger load report update
    private int loadBalancerReportUpdateThresholdPercentage = 10;
    // maximum interval to update load report
    private int loadBalancerReportUpdateMaxIntervalMinutes = 15;
    // Frequency of report to collect
    private int loadBalancerHostUsageCheckIntervalMinutes = 1;
    // Load shedding interval. Broker periodically checks whether some traffic
    // should be offload from
    // some over-loaded broker to other under-loaded brokers
    private int loadBalancerSheddingIntervalMinutes = 30;
    // Prevent the same topics to be shed and moved to other broker more that
    // once within this timeframe
    private long loadBalancerSheddingGracePeriodMinutes = 30;
    // Usage threshold to determine a broker as under-loaded
    private int loadBalancerBrokerUnderloadedThresholdPercentage = 50;
    // Usage threshold to determine a broker as over-loaded
    private int loadBalancerBrokerOverloadedThresholdPercentage = 85;
    // interval to flush dynamic resource quota to ZooKeeper
    private int loadBalancerResourceQuotaUpdateIntervalMinutes = 15;
    // Usage threshold to defermine a broker is having just right level of load
    private int loadBalancerBrokerComfortLoadLevelPercentage = 65;
    // enable/disable automatic namespace bundle split
    private boolean loadBalancerAutoBundleSplitEnabled = false;
    // maximum topics in a bundle, otherwise bundle split will be triggered
    private int loadBalancerNamespaceBundleMaxTopics = 1000;
    // maximum sessions (producers + consumers) in a bundle, otherwise bundle split will be triggered
    private int loadBalancerNamespaceBundleMaxSessions = 1000;
    // maximum msgRate (in + out) in a bundle, otherwise bundle split will be triggered
    private int loadBalancerNamespaceBundleMaxMsgRate = 1000;
    // maximum bandwidth (in + out) in a bundle, otherwise bundle split will be triggered
    private int loadBalancerNamespaceBundleMaxBandwidthMbytes = 100;
    // maximum number of bundles in a namespace
    private int loadBalancerNamespaceMaximumBundles = 128;

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

    public String getZookeeperServers() {
        return zookeeperServers;
    }

    public void setZookeeperServers(String zookeeperServers) {
        this.zookeeperServers = zookeeperServers;
    }

    public String getGlobalZookeeperServers() {
        if (this.globalZookeeperServers == null || this.globalZookeeperServers.isEmpty()) {
            // If the configuration is not set, assuming that the globalZK is not enabled and all data is in the same
            // ZooKeeper cluster
            return this.getZookeeperServers();
        }
        return globalZookeeperServers;
    }

    public void setGlobalZookeeperServers(String globalZookeeperServers) {
        this.globalZookeeperServers = globalZookeeperServers;
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

    public boolean isClientLibraryVersionCheckEnabled() {
        return clientLibraryVersionCheckEnabled;
    }

    public void setClientLibraryVersionCheckEnabled(boolean clientLibraryVersionCheckEnabled) {
        this.clientLibraryVersionCheckEnabled = clientLibraryVersionCheckEnabled;
    }

    public boolean isClientLibraryVersionCheckAllowUnversioned() {
        return clientLibraryVersionCheckAllowUnversioned;
    }

    public void setClientLibraryVersionCheckAllowUnversioned(boolean clientLibraryVersionCheckAllowUnversioned) {
        this.clientLibraryVersionCheckAllowUnversioned = clientLibraryVersionCheckAllowUnversioned;
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

    public int getMaxConcurrentLookupRequest() {
        return maxConcurrentLookupRequest;
    }

    public void setMaxConcurrentLookupRequest(int maxConcurrentLookupRequest) {
        this.maxConcurrentLookupRequest = maxConcurrentLookupRequest;
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

    public boolean getLoadBalancerAutoBundleSplitEnabled() {
        return this.loadBalancerAutoBundleSplitEnabled;
    }

    public void setLoadBalancerAutoBundleSplitEnabled(boolean enabled) {
        this.loadBalancerAutoBundleSplitEnabled = enabled;
    }

    public void setLoadBalancerNamespaceMaximumBundles(int bundles) {
        this.loadBalancerNamespaceMaximumBundles = bundles;
    }

    public int getLoadBalancerNamespaceMaximumBundles() {
        return this.loadBalancerNamespaceMaximumBundles;
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

    public List<String> getBootstrapNamespaces() {
        return bootstrapNamespaces;
    }

    public Object getProperty(String key) {
        return properties.get(key);
    }

    public Properties getProperties() {
        return properties;
    }

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
}
