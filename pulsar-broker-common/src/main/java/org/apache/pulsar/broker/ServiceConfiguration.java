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


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.netty.util.internal.PlatformDependent;

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
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.sasl.SaslConstants;

/**
 * Pulsar service configuration object.
 */
@Getter
@Setter
public class ServiceConfiguration implements PulsarConfiguration {

    @Category
    private static final String CATEGORY_SERVER = "Server";
    @Category
    private static final String CATEGORY_PROTOCOLS = "Protocols";
    @Category
    private static final String CATEGORY_STORAGE_BK = "Storage (BookKeeper)";
    @Category
    private static final String CATEGORY_STORAGE_ML = "Storage (Managed Ledger)";
    @Category
    private static final String CATEGORY_STORAGE_OFFLOADING = "Storage (Ledger Offloading)";
    @Category
    private static final String CATEGORY_POLICIES = "Policies";
    @Category
    private static final String CATEGORY_WEBSOCKET = "WebSocket";
    @Category
    private static final String CATEGORY_SCHEMA = "Schema";
    @Category
    private static final String CATEGORY_METRICS = "Metrics";
    @Category
    private static final String CATEGORY_REPLICATION = "Replication";
    @Category
    private static final String CATEGORY_LOAD_BALANCER = "Load Balancer";
    @Category
    private static final String CATEGORY_FUNCTIONS = "Functions";
    @Category
    private static final String CATEGORY_TLS = "TLS";
    @Category
    private static final String CATEGORY_AUTHENTICATION = "Authentication";
    @Category
    private static final String CATEGORY_AUTHORIZATION = "Authorization";
    @Category
    private static final String CATEGORY_TOKEN_AUTH = "Token Authentication Provider";
    @Category
    private static final String CATEGORY_SASL_AUTH = "SASL Authentication Provider";
    @Category
    private static final String CATEGORY_HTTP = "HTTP";
    @Category
    private static final String CATEGORY_TRANSACTION = "Transaction";

    /***** --- pulsar configuration --- ****/
    @FieldContext(
        category = CATEGORY_SERVER,
        required = true,
        doc = "The Zookeeper quorum connection string (as a comma-separated list)"
    )
    private String zookeeperServers;
    @Deprecated
    @FieldContext(
        category = CATEGORY_SERVER,
        required = false,
        deprecated = true,
        doc = "Global Zookeeper quorum connection string (as a comma-separated list)."
            + " Deprecated in favor of using `configurationStoreServers`"
    )
    private String globalZookeeperServers;
    @FieldContext(
        category = CATEGORY_SERVER,
        required = false,
        doc = "Configuration store connection string (as a comma-separated list)"
    )
    private String configurationStoreServers;
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "The port for serving binary protobuf requests"
    )

    private Optional<Integer> brokerServicePort = Optional.of(6650);
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "The port for serving tls secured binary protobuf requests"
    )
    private Optional<Integer> brokerServicePortTls = Optional.empty();
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "The port for serving http requests"
    )
    private Optional<Integer> webServicePort = Optional.of(8080);
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "The port for serving https requests"
    )
    private Optional<Integer> webServicePortTls = Optional.empty();

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Hostname or IP address the service binds on"
    )
    private String bindAddress = "0.0.0.0";

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Hostname or IP address the service advertises to the outside world."
            + " If not set, the value of `InetAddress.getLocalHost().getHostname()` is used."
    )
    private String advertisedAddress;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Number of threads to use for Netty IO."
            + " Default is set to `2 * Runtime.getRuntime().availableProcessors()`"
    )
    private int numIOThreads = 2 * Runtime.getRuntime().availableProcessors();

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "Number of threads to use for HTTP requests processing"
                + " Default is set to `2 * Runtime.getRuntime().availableProcessors()`"
        )
    // Use at least 8 threads to avoid having Jetty go into threads starving and
    // having the possibility of getting into a deadlock where a Jetty thread is
    // waiting for another HTTP call to complete in same thread.
    private int numHttpServerThreads = Math.max(8, 2 * Runtime.getRuntime().availableProcessors());

    @FieldContext(category = CATEGORY_SERVER, doc = "Whether to enable the delayed delivery for messages.")
    private boolean delayedDeliveryEnabled = true;

    @FieldContext(category = CATEGORY_SERVER, doc = "Class name of the factory that implements the delayed deliver tracker")
    private String delayedDeliveryTrackerFactoryClassName = "org.apache.pulsar.broker.delayed.InMemoryDelayedDeliveryTrackerFactory";

    @FieldContext(category = CATEGORY_SERVER, doc = "Control the tick time for when retrying on delayed delivery, "
            + " affecting the accuracy of the delivery time compared to the scheduled time. Default is 1 second.")
    private long delayedDeliveryTickTimeMillis = 1000;

    @FieldContext(
        category = CATEGORY_WEBSOCKET,
        doc = "Enable the WebSocket API service in broker"
    )
    private boolean webSocketServiceEnabled = false;

    @FieldContext(
        category = CATEGORY_WEBSOCKET,
        doc = "Flag indicates whether to run broker in standalone mode"
    )
    private boolean isRunningStandalone = false;

    @FieldContext(
        category = CATEGORY_SERVER,
        required = true,
        doc = "Name of the cluster to which this broker belongs to"
    )
    private String clusterName;
    @FieldContext(
        category = CATEGORY_SERVER,
        dynamic = true,
        doc = "Enable cluster's failure-domain which can distribute brokers into logical region"
    )
    private boolean failureDomainsEnabled = false;
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "ZooKeeper session timeout in milliseconds"
    )
    private long zooKeeperSessionTimeoutMillis = 30000;
    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "ZooKeeper operation timeout in seconds"
        )
    private int zooKeeperOperationTimeoutSeconds = 30;
    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "ZooKeeper cache expiry time in seconds"
        )
    private int zooKeeperCacheExpirySeconds = 300;
    @FieldContext(
        category = CATEGORY_SERVER,
        dynamic = true,
        doc = "Time to wait for broker graceful shutdown. After this time elapses, the process will be killed"
    )
    private long brokerShutdownTimeoutMs = 60000;
    @FieldContext(
        category = CATEGORY_SERVER,
        dynamic = true,
        doc = "Flag to skip broker shutdown when broker handles Out of memory error"
    )
    private boolean skipBrokerShutdownOnOOM = false;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Enable backlog quota check. Enforces actions on topic when the quota is reached"
    )
    private boolean backlogQuotaCheckEnabled = true;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "How often to check for topics that have reached the quota."
            + " It only takes effects when `backlogQuotaCheckEnabled` is true"
    )
    private int backlogQuotaCheckIntervalInSeconds = 60;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Default per-topic backlog quota limit, less than 0 means no limitation. default is -1."
                + " Increase it if you want to allow larger msg backlog"
    )
    private long backlogQuotaDefaultLimitGB = -1;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Default backlog quota retention policy. Default is producer_request_hold\n\n"
            + "'producer_request_hold' Policy which holds producer's send request until the"
            + "resource becomes available (or holding times out)\n"
            + "'producer_exception' Policy which throws javax.jms.ResourceAllocationException to the producer\n"
            + "'consumer_backlog_eviction' Policy which evicts the oldest message from the slowest consumer's backlog"
    )
    private BacklogQuota.RetentionPolicy backlogQuotaDefaultRetentionPolicy = BacklogQuota.RetentionPolicy.producer_request_hold;
    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Default ttl for namespaces if ttl is not already configured at namespace policies. "
                    + "(disable default-ttl with value 0)"
        )
    private int ttlDurationDefaultInSeconds = 0;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Enable the deletion of inactive topics"
    )
    private boolean brokerDeleteInactiveTopicsEnabled = true;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "How often to check for inactive topics"
    )
    private int brokerDeleteInactiveTopicsFrequencySeconds = 60;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Set the inactive topic delete mode. Default is delete_when_no_subscriptions\n"
        + "'delete_when_no_subscriptions' mode only delete the topic which has no subscriptions and no active producers\n"
        + "'delete_when_subscriptions_caught_up' mode only delete the topic that all subscriptions has no backlogs(caught up)"
        + "and no active producers/consumers"
    )
    private InactiveTopicDeleteMode brokerDeleteInactiveTopicsMode = InactiveTopicDeleteMode.delete_when_no_subscriptions;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Max duration of topic inactivity in seconds, default is not present\n"
        + "If not present, 'brokerDeleteInactiveTopicsFrequencySeconds' will be used\n"
        + "Topics that are inactive for longer than this value will be deleted"
    )
    private Integer brokerDeleteInactiveTopicsMaxInactiveDurationSeconds = null;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Max pending publish requests per connection to avoid keeping large number of pending "
                + "requests in memory. Default: 1000"
    )
    private int maxPendingPublishdRequestsPerConnection = 1000;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "How frequently to proactively check and purge expired messages"
    )
    private int messageExpiryCheckIntervalInMinutes = 5;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "How long to delay rewinding cursor and dispatching messages when active consumer is changed"
    )
    private int activeConsumerFailoverDelayTimeMillis = 1000;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "How long to delete inactive subscriptions from last consuming."
            + " When it is 0, inactive subscriptions are not deleted automatically"
    )
    private long subscriptionExpirationTimeMinutes = 0;
    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Enable subscription message redelivery tracker to send redelivery "
                    + "count to consumer (default is enabled)"
        )
    private boolean subscriptionRedeliveryTrackerEnabled = true;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "How frequently to proactively check and purge expired subscription"
    )
    private long subscriptionExpiryCheckIntervalInMinutes = 5;

    @FieldContext(
        category = CATEGORY_POLICIES,
        dynamic = true,
        doc = "Enable Key_Shared subscription (default is enabled)"
    )
    private boolean subscriptionKeySharedEnable = true;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Set the default behavior for message deduplication in the broker.\n\n"
            + "This can be overridden per-namespace. If enabled, broker will reject"
            + " messages that were already stored in the topic"
    )
    private boolean brokerDeduplicationEnabled = false;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Maximum number of producer information that it's going to be persisted for deduplication purposes"
    )
    private int brokerDeduplicationMaxNumberOfProducers = 10000;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Number of entries after which a dedup info snapshot is taken.\n\n"
            + "A bigger interval will lead to less snapshots being taken though it would"
            + " increase the topic recovery time, when the entries published after the"
            + " snapshot need to be replayed"
    )
    private int brokerDeduplicationEntriesInterval = 1000;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Time of inactivity after which the broker will discard the deduplication information"
            + " relative to a disconnected producer. Default is 6 hours.")
    private int brokerDeduplicationProducerInactivityTimeoutMinutes = 360;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "When a namespace is created without specifying the number of bundle, this"
            + " value will be used as the default")
    private int defaultNumberOfNamespaceBundles = 4;

    @FieldContext(
        category = CATEGORY_SERVER,
        dynamic = true,
        doc = "Enable check for minimum allowed client library version"
    )
    private boolean clientLibraryVersionCheckEnabled = false;
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Path for the file used to determine the rotation status for the broker"
            + " when responding to service discovery health checks")
    private String statusFilePath;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Max number of unacknowledged messages allowed to receive messages by a consumer on"
            + " a shared subscription.\n\n Broker will stop sending messages to consumer once,"
            + " this limit reaches until consumer starts acknowledging messages back and unack count"
            + " reaches to `maxUnackedMessagesPerConsumer/2`. Using a value of 0, it is disabling "
            + " unackedMessage-limit check and consumer can receive messages without any restriction")
    private int maxUnackedMessagesPerConsumer = 50000;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Max number of unacknowledged messages allowed per shared subscription. \n\n"
            + " Broker will stop dispatching messages to all consumers of the subscription once this "
            + " limit reaches until consumer starts acknowledging messages back and unack count reaches"
            + " to `limit/2`. Using a value of 0, is disabling unackedMessage-limit check and dispatcher"
            + " can dispatch messages without any restriction")
    private int maxUnackedMessagesPerSubscription = 4 * 50000;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Max number of unacknowledged messages allowed per broker. \n\n"
            + " Once this limit reaches, broker will stop dispatching messages to all shared subscription "
            + " which has higher number of unack messages until subscriptions start acknowledging messages "
            + " back and unack count reaches to `limit/2`. Using a value of 0, is disabling unackedMessage-limit"
            + " check and broker doesn't block dispatchers")
    private int maxUnackedMessagesPerBroker = 0;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Once broker reaches maxUnackedMessagesPerBroker limit, it blocks subscriptions which has higher "
            + " unacked messages than this percentage limit and subscription will not receive any new messages "
            + " until that subscription acks back `limit/2` messages")
    private double maxUnackedMessagesPerSubscriptionOnBrokerBlocked = 0.16;
    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Tick time to schedule task that checks topic publish rate limiting across all topics  "
                    + "Reducing to lower value can give more accuracy while throttling publish but "
                    + "it uses more CPU to perform frequent check. (Disable publish throttling with value 0)"
        )
    private int topicPublisherThrottlingTickTimeMillis = 5;

    @FieldContext(
        category = CATEGORY_SERVER,
        dynamic = true,
        doc = "Tick time to schedule task that checks broker publish rate limiting across all topics  "
            + "Reducing to lower value can give more accuracy while throttling publish but "
            + "it uses more CPU to perform frequent check. (Disable publish throttling with value 0)"
    )
    private int brokerPublisherThrottlingTickTimeMillis = 50;
    @FieldContext(
        category = CATEGORY_SERVER,
        dynamic = true,
        doc = "Max Rate(in 1 seconds) of Message allowed to publish for a broker "
            + "when broker publish rate limiting enabled. (Disable message rate limit with value 0)"
    )
    private int brokerPublisherThrottlingMaxMessageRate = 0;
    @FieldContext(
        category = CATEGORY_SERVER,
        dynamic = true,
        doc = "Max Rate(in 1 seconds) of Byte allowed to publish for a broker "
            + "when broker publish rate limiting enabled. (Disable byte rate limit with value 0)"
    )
    private long brokerPublisherThrottlingMaxByteRate = 0;

    @FieldContext(
        category = CATEGORY_POLICIES,
        dynamic = true,
        doc = "Too many subscribe requests from a consumer can cause broker rewinding consumer cursors "
            + " and loading data from bookies, hence causing high network bandwidth usage When the positive"
            + " value is set, broker will throttle the subscribe requests for one consumer. Otherwise, the"
            + " throttling will be disabled. The default value of this setting is 0 - throttling is disabled.")
    private int subscribeThrottlingRatePerConsumer = 0;
    @FieldContext(
        minValue = 1,
        dynamic = true,
        category = CATEGORY_POLICIES,
        doc = "Rate period for {subscribeThrottlingRatePerConsumer}. Default is 30s."
    )
    private int subscribeRatePeriodPerConsumerInSecond = 30;

    @FieldContext(
        dynamic = true,
        category = CATEGORY_POLICIES,
        doc = "Default number of message dispatching throttling-limit for every topic. \n\n"
            + "Using a value of 0, is disabling default message dispatch-throttling")
    private int dispatchThrottlingRatePerTopicInMsg = 0;
    @FieldContext(
        dynamic = true,
        category = CATEGORY_POLICIES,
        doc = "Default number of message-bytes dispatching throttling-limit for every topic. \n\n"
            + "Using a value of 0, is disabling default message-byte dispatch-throttling")
    private long dispatchThrottlingRatePerTopicInByte = 0;

    @FieldContext(
        dynamic = true,
        category = CATEGORY_POLICIES,
        doc = "Default number of message dispatching throttling-limit for a subscription. \n\n"
            + "Using a value of 0, is disabling default message dispatch-throttling.")
    private int dispatchThrottlingRatePerSubscriptionInMsg = 0;
    @FieldContext(
        dynamic = true,
        category = CATEGORY_POLICIES,
        doc = "Default number of message-bytes dispatching throttling-limit for a subscription. \n\n"
            + "Using a value of 0, is disabling default message-byte dispatch-throttling.")
    private long dispatchThrottlingRatePerSubscriptionInByte = 0;

    @FieldContext(
        dynamic = true,
        category = CATEGORY_POLICIES,
        doc = "Default number of message dispatching throttling-limit for every replicator in replication. \n\n"
            + "Using a value of 0, is disabling replication message dispatch-throttling")
    private int dispatchThrottlingRatePerReplicatorInMsg = 0;
    @FieldContext(
        dynamic = true,
        category = CATEGORY_POLICIES,
        doc = "Default number of message-bytes dispatching throttling-limit for every replicator in replication. \n\n"
            + "Using a value of 0, is disabling replication message-byte dispatch-throttling")
    private long dispatchThrottlingRatePerReplicatorInByte = 0;
    @FieldContext(
            dynamic = true,
            category = CATEGORY_POLICIES,
            doc = "Dispatch rate-limiting relative to publish rate. (Enabling flag will make broker to dynamically "
                    + "update dispatch-rate relatively to publish-rate: "
                    + "throttle-dispatch-rate = (publish-rate + configured dispatch-rate) ")
    private boolean dispatchThrottlingRateRelativeToPublishRate = false;
    @FieldContext(
        dynamic = true,
        category = CATEGORY_POLICIES,
        doc = "Default dispatch-throttling is disabled for consumers which already caught-up with"
            + " published messages and don't have backlog. This enables dispatch-throttling for "
            + " non-backlog consumers as well.")
    private boolean dispatchThrottlingOnNonBacklogConsumerEnabled = false;

    // <-- dispatcher read settings -->
    @FieldContext(
        dynamic = true,
        category = CATEGORY_SERVER,
        doc = "Max number of entries to read from bookkeeper. By default it is 100 entries."
    )
    private int dispatcherMaxReadBatchSize = 100;

    @FieldContext(
        dynamic = true,
        category = CATEGORY_SERVER,
        doc = "Min number of entries to read from bookkeeper. By default it is 1 entries."
            + "When there is an error occurred on reading entries from bookkeeper, the broker"
            + " will backoff the batch size to this minimum number."
    )
    private int dispatcherMinReadBatchSize = 1;

    @FieldContext(
        dynamic = true,
        category = CATEGORY_SERVER,
        doc = "Max number of entries to dispatch for a shared subscription. By default it is 20 entries."
    )
    private int dispatcherMaxRoundRobinBatchSize = 20;

    @FieldContext(
        dynamic = true,
        category = CATEGORY_SERVER,
        doc = "Max number of concurrent lookup request broker allows to throttle heavy incoming lookup traffic")
    private int maxConcurrentLookupRequest = 50000;

    @FieldContext(
        dynamic = true,
        category = CATEGORY_SERVER,
        doc = "Max number of concurrent topic loading request broker allows to control number of zk-operations"
    )
    private int maxConcurrentTopicLoadRequest = 5000;
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Max concurrent non-persistent message can be processed per connection")
    private int maxConcurrentNonPersistentMessagePerConnection = 1000;
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Number of worker threads to serve non-persistent topic")
    private int numWorkerThreadsForNonPersistentTopic = Runtime.getRuntime().availableProcessors();;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Enable broker to load persistent topics"
    )
    private boolean enablePersistentTopics = true;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Enable broker to load non-persistent topics"
    )
    private boolean enableNonPersistentTopics = true;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Enable to run bookie along with broker"
    )
    private boolean enableRunBookieTogether = false;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Enable to run bookie autorecovery along with broker"
    )
    private boolean enableRunBookieAutoRecoveryTogether = false;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Max number of producers allowed to connect to topic. \n\nOnce this limit reaches,"
            + " Broker will reject new producers until the number of connected producers decrease."
            + " Using a value of 0, is disabling maxProducersPerTopic-limit check.")
    private int maxProducersPerTopic = 0;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Max number of consumers allowed to connect to topic. \n\nOnce this limit reaches,"
            + " Broker will reject new consumers until the number of connected consumers decrease."
            + " Using a value of 0, is disabling maxConsumersPerTopic-limit check.")
    private int maxConsumersPerTopic = 0;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Max number of consumers allowed to connect to subscription. \n\nOnce this limit reaches,"
            + " Broker will reject new consumers until the number of connected consumers decrease."
            + " Using a value of 0, is disabling maxConsumersPerSubscription-limit check.")
    private int maxConsumersPerSubscription = 0;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Max size of messages.",
        maxValue = Integer.MAX_VALUE - Commands.MESSAGE_SIZE_FRAME_PADDING)
    private int maxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE;

    @FieldContext(
            category = CATEGORY_SERVER,
        doc = "Enable tracking of replicated subscriptions state across clusters.")
    private boolean enableReplicatedSubscriptions = true;

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "Frequency of snapshots for replicated subscriptions tracking.")
    private int replicatedSubscriptionsSnapshotFrequencyMillis = 1_000;

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "Timeout for building a consistent snapshot for tracking replicated subscriptions state. ")
    private int replicatedSubscriptionsSnapshotTimeoutSeconds = 30;

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "Max number of snapshot to be cached per subscription.")
    private int replicatedSubscriptionsSnapshotMaxCachedPerSubscription = 10;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Max memory size for broker handling messages sending from producers.\n\n"
            + " If the processing message size exceed this value, broker will stop read data"
            + " from the connection. The processing messages means messages are sends to broker"
            + " but broker have not send response to client, usually waiting to write to bookies.\n\n"
            + " It's shared across all the topics running in the same broker.\n\n"
            + " Use -1 to disable the memory limitation. Default is 1/2 of direct memory.\n\n")
    private int maxMessagePublishBufferSizeInMB = Math.max(64,
        (int) (PlatformDependent.maxDirectMemory() / 2 / (1024 * 1024)));

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Interval between checks to see if message publish buffer size is exceed the max message publish buffer size"
    )
    private int messagePublishBufferCheckIntervalInMillis = 100;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Check between intervals to see if consumed ledgers need to be trimmed"
    )
    private int retentionCheckIntervalInSeconds = 120;

    /**** --- Messaging Protocols --- ****/

    @FieldContext(
        category = CATEGORY_PROTOCOLS,
        doc = "The directory to locate messaging protocol handlers"
    )
    private String protocolHandlerDirectory = "./protocols";

    @FieldContext(
        category = CATEGORY_PROTOCOLS,
        doc = "List of messaging protocols to load, which is a list of protocol names"
    )
    private Set<String> messagingProtocols = Sets.newTreeSet();

    /***** --- TLS --- ****/
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Enable TLS"
    )
    @Deprecated
    private boolean tlsEnabled = false;
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Tls cert refresh duration in seconds (set 0 to check on every new connection)"
    )
    private long tlsCertRefreshCheckDurationSec = 300;
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Path for the TLS certificate file"
    )
    private String tlsCertificateFilePath;
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Path for the TLS private key file"
    )
    private String tlsKeyFilePath;
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Path for the trusted TLS certificate file"
    )
    private String tlsTrustCertsFilePath = "";
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Accept untrusted TLS certificate from client"
    )
    private boolean tlsAllowInsecureConnection = false;
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Specify the tls protocols the broker will use to negotiate during TLS Handshake.\n\n"
            + "Example:- [TLSv1.2, TLSv1.1, TLSv1]"
    )
    private Set<String> tlsProtocols = Sets.newTreeSet();
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Specify the tls cipher the broker will use to negotiate during TLS Handshake.\n\n"
            + "Example:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]"
    )
    private Set<String> tlsCiphers = Sets.newTreeSet();
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Specify whether Client certificates are required for TLS Reject.\n"
            + "the Connection if the Client Certificate is not trusted")
    private boolean tlsRequireTrustedClientCertOnConnect = false;

    /***** --- Authentication --- ****/
    @FieldContext(
        category = CATEGORY_AUTHENTICATION,
        doc = "Enable authentication"
    )
    private boolean authenticationEnabled = false;
    @FieldContext(
        category = CATEGORY_AUTHENTICATION,
        doc = "Autentication provider name list, which is a list of class names"
    )
    private Set<String> authenticationProviders = Sets.newTreeSet();

    @FieldContext(
        category = CATEGORY_AUTHENTICATION,
        doc = "Interval of time for checking for expired authentication credentials"
    )
    private int authenticationRefreshCheckSeconds = 60;

    @FieldContext(
        category = CATEGORY_AUTHORIZATION,
        doc = "Enforce authorization"
    )
    private boolean authorizationEnabled = false;
    @FieldContext(
        category = CATEGORY_AUTHORIZATION,
        doc = "Authorization provider fully qualified class-name"
    )
    private String authorizationProvider = PulsarAuthorizationProvider.class.getName();

    @FieldContext(
        category = CATEGORY_AUTHORIZATION,
        dynamic = true,
        doc = "Role names that are treated as `super-user`, meaning they will be able to"
            + " do all admin operations and publish/consume from all topics"
    )
    private Set<String> superUserRoles = Sets.newTreeSet();

    @FieldContext(
        category = CATEGORY_AUTHORIZATION,
        doc = "Role names that are treated as `proxy roles`. \n\nIf the broker sees"
            + " a request with role as proxyRoles - it will demand to see the original"
            + " client role or certificate.")
    private Set<String> proxyRoles = Sets.newTreeSet();

    @FieldContext(
        category = CATEGORY_AUTHORIZATION,
        doc = "If this flag is set then the broker authenticates the original Auth data"
            + " else it just accepts the originalPrincipal and authorizes it (if required)")
    private boolean authenticateOriginalAuthData = false;

    @FieldContext(
        category = CATEGORY_AUTHORIZATION,
        doc = "Allow wildcard matching in authorization\n\n"
            + "(wildcard matching only applicable if wildcard-char: * presents at first"
            + " or last position eg: *.pulsar.service, pulsar.service.*)")
    private boolean authorizationAllowWildcardsMatching = false;

    @FieldContext(
        category = CATEGORY_AUTHENTICATION,
        dynamic = true,
        doc = "Authentication settings of the broker itself. \n\nUsed when the broker connects"
            + " to other brokers, either in same or other clusters. Default uses plugin which disables authentication"
    )
    private String brokerClientAuthenticationPlugin = "org.apache.pulsar.client.impl.auth.AuthenticationDisabled";
    @FieldContext(
        category = CATEGORY_AUTHENTICATION,
        dynamic = true,
        doc = "Authentication parameters of the authentication plugin the broker is using to connect to other brokers"
    )
    private String brokerClientAuthenticationParameters = "";
    @FieldContext(
        category = CATEGORY_AUTHENTICATION,
        doc = "Path for the trusted TLS certificate file for outgoing connection to a server (broker)")
    private String brokerClientTrustCertsFilePath = "";

    @FieldContext(
        category = CATEGORY_AUTHORIZATION,
        doc = "When this parameter is not empty, unauthenticated users perform as anonymousUserRole"
    )
    private String anonymousUserRole = null;

    @FieldContext(
        category =  CATEGORY_HTTP,
        doc = "If >0, it will reject all HTTP requests with bodies larged than the configured limit"
    )
    private long httpMaxRequestSize = -1;

    @FieldContext(
        category = CATEGORY_SASL_AUTH,
        doc = "This is a regexp, which limits the range of possible ids which can connect to the Broker using SASL.\n"
            + " Default value is: \".*pulsar.*\", so only clients whose id contains 'pulsar' are allowed to connect."
    )
    private String saslJaasClientAllowedIds = SaslConstants.JAAS_CLIENT_ALLOWED_IDS_DEFAULT;

    @FieldContext(
        category = CATEGORY_SASL_AUTH,
        doc = "Service Principal, for login context name. Default value is \"PulsarBroker\"."
    )
    private String saslJaasServerSectionName = SaslConstants.JAAS_DEFAULT_BROKER_SECTION_NAME;

    @FieldContext(
        category = CATEGORY_SASL_AUTH,
        doc = "kerberos kinit command."
    )
    private String kinitCommand = "/usr/bin/kinit";

    /**** --- BookKeeper Client --- ****/
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Authentication plugin to use when connecting to bookies"
    )
    private String bookkeeperClientAuthenticationPlugin;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "BookKeeper auth plugin implementatation specifics parameters name and values"
    )
    private String bookkeeperClientAuthenticationParametersName;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Parameters for bookkeeper auth plugin"
    )
    private String bookkeeperClientAuthenticationParameters;

    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Timeout for BK add / read operations"
    )
    private long bookkeeperClientTimeoutInSeconds = 30;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Speculative reads are initiated if a read request doesn't complete within"
            + " a certain time Using a value of 0, is disabling the speculative reads")
    private int bookkeeperClientSpeculativeReadTimeoutInMillis = 0;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Use older Bookkeeper wire protocol with bookie"
    )
    private boolean bookkeeperUseV2WireProtocol = true;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Enable bookies health check. \n\n Bookies that have more than the configured"
            + " number of failure within the interval will be quarantined for some time."
            + " During this period, new ledgers won't be created on these bookies")
    private boolean bookkeeperClientHealthCheckEnabled = true;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Bookies health check interval in seconds"
    )
    private long bookkeeperClientHealthCheckIntervalSeconds = 60;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Bookies health check error threshold per check interval"
    )
    private long bookkeeperClientHealthCheckErrorThresholdPerInterval = 5;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Bookie health check quarantined time in seconds"
    )
    private long bookkeeperClientHealthCheckQuarantineTimeInSeconds = 1800;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Enable rack-aware bookie selection policy. \n\nBK will chose bookies from"
            + " different racks when forming a new bookie ensemble")
    private boolean bookkeeperClientRackawarePolicyEnabled = true;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Enable region-aware bookie selection policy. \n\nBK will chose bookies from"
            + " different regions and racks when forming a new bookie ensemble")
    private boolean bookkeeperClientRegionawarePolicyEnabled = false;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        doc = "Enable/disable reordering read sequence on reading entries")
    private boolean bookkeeperClientReorderReadSequenceEnabled = false;
    @FieldContext(
        category = CATEGORY_STORAGE_BK,
        required = false,
        doc = "Enable bookie isolation by specifying a list of bookie groups to choose from. \n\n"
            + "Any bookie outside the specified groups will not be used by the broker")
    private String bookkeeperClientIsolationGroups;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            required = false,
            doc = "Enable bookie secondary-isolation group if bookkeeperClientIsolationGroups doesn't have enough bookie available."
                )
    private String bookkeeperClientSecondaryIsolationGroups;
    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Enable/disable having read operations for a ledger to be sticky to "
            + "a single bookie.\n" +
            "If this flag is enabled, the client will use one single bookie (by " +
            "preference) to read all entries for a ledger.")
    private boolean bookkeeperEnableStickyReads = false;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Set the client security provider factory class name. "
            + "Default: org.apache.bookkeeper.tls.TLSContextFactory")
    private String bookkeeperTLSProviderFactoryClass = "org.apache.bookkeeper.tls.TLSContextFactory";

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Enable tls authentication with bookie")
    private boolean bookkeeperTLSClientAuthentication = false;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Supported type: PEM, JKS, PKCS12. Default value: PEM")
    private String bookkeeperTLSKeyFileType = "PEM";

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Supported type: PEM, JKS, PKCS12. Default value: PEM")
    private String bookkeeperTLSTrustCertTypes = "PEM";

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Path to file containing keystore password, "
            + "if the client keystore is password protected.")
    private String bookkeeperTLSKeyStorePasswordPath;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Path to file containing truststore password, "
            + "if the client truststore is password protected.")
    private String bookkeeperTLSTrustStorePasswordPath;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Path for the TLS private key file")
    private String bookkeeperTLSKeyFilePath;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Path for the TLS certificate file")
    private String bookkeeperTLSCertificateFilePath;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Path for the trusted TLS certificate file")
    private String bookkeeperTLSTrustCertsFilePath;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Enable/disable disk weight based placement. Default is false")
    private boolean bookkeeperDiskWeightBasedPlacementEnabled = false;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Set the interval to check the need for sending an explicit LAC")
    private int bookkeeperExplicitLacIntervalInMills = 0;

    /**** --- Managed Ledger --- ****/
    @FieldContext(
        minValue = 1,
        category = CATEGORY_STORAGE_ML,
        doc = "Number of bookies to use when creating a ledger"
    )
    private int managedLedgerDefaultEnsembleSize = 2;
    @FieldContext(
        minValue = 1,
        category = CATEGORY_STORAGE_ML,
        doc = "Number of copies to store for each message"
    )
    private int managedLedgerDefaultWriteQuorum = 2;
    @FieldContext(
        minValue = 1,
        category = CATEGORY_STORAGE_ML,
        doc = "Number of guaranteed copies (acks to wait before write is complete)"
    )
    private int managedLedgerDefaultAckQuorum = 2;

    //
    //
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Default type of checksum to use when writing to BookKeeper. \n\nDefault is `CRC32C`."
            + " Other possible options are `CRC32`, `MAC` or `DUMMY` (no checksum)."
    )
    private DigestType managedLedgerDigestType = DigestType.CRC32C;

    @FieldContext(
        minValue = 1,
        category = CATEGORY_STORAGE_ML,
        doc = "Max number of bookies to use when creating a ledger"
    )
    private int managedLedgerMaxEnsembleSize = 5;
    @FieldContext(
        minValue = 1,
        category = CATEGORY_STORAGE_ML,
        doc = "Max number of copies to store for each message"
    )
    private int managedLedgerMaxWriteQuorum = 5;
    @FieldContext(
        minValue = 1,
        category = CATEGORY_STORAGE_ML,
        doc = "Max number of guaranteed copies (acks to wait before write is complete)"
    )
    private int managedLedgerMaxAckQuorum = 5;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Amount of memory to use for caching data payload in managed ledger. \n\nThis"
            + " memory is allocated from JVM direct memory and it's shared across all the topics"
            + " running in the same broker. By default, uses 1/5th of available direct memory")
    private int managedLedgerCacheSizeMB = Math.max(64,
            (int) (PlatformDependent.maxDirectMemory() / 5 / (1024 * 1024)));
    @FieldContext(category = CATEGORY_STORAGE_ML, doc = "Whether we should make a copy of the entry payloads when inserting in cache")
    private boolean managedLedgerCacheCopyEntries = false;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Threshold to which bring down the cache level when eviction is triggered"
    )
    private double managedLedgerCacheEvictionWatermark = 0.9f;
    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "Configure the cache eviction frequency for the managed ledger cache. Default is 100/s")
    private double managedLedgerCacheEvictionFrequency = 100.0;
    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "All entries that have stayed in cache for more than the configured time, will be evicted")
    private long managedLedgerCacheEvictionTimeThresholdMillis = 1000;
    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "Configure the threshold (in number of entries) from where a cursor should be considered 'backlogged'"
                    + " and thus should be set as inactive.")
    private long managedLedgerCursorBackloggedThreshold = 1000;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Rate limit the amount of writes per second generated by consumer acking the messages"
    )
    private double managedLedgerDefaultMarkDeleteRateLimit = 1.0;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
    	doc = "Allow automated creation of topics if set to true (default value)."
    )
    private boolean allowAutoTopicCreation = true;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "The type of topic that is allowed to be automatically created.(partitioned/non-partitioned)"
    )
    private String allowAutoTopicCreationType = "non-partitioned";
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Allow automated creation of subscriptions if set to true (default value)."
    )
    private boolean allowAutoSubscriptionCreation = true;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "The number of partitioned topics that is allowed to be automatically created"
                    + "if allowAutoTopicCreationType is partitioned."
    )
    private int defaultNumPartitions = 1;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Number of threads to be used for managed ledger tasks dispatching"
    )
    private int managedLedgerNumWorkerThreads = Runtime.getRuntime().availableProcessors();
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Number of threads to be used for managed ledger scheduled tasks"
    )
    private int managedLedgerNumSchedulerThreads = Runtime.getRuntime().availableProcessors();

    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Max number of entries to append to a ledger before triggering a rollover.\n\n"
            + "A ledger rollover is triggered on these conditions Either the max"
            + " rollover time has been reached or max entries have been written to the"
            + " ledged and at least min-time has passed")
    private int managedLedgerMaxEntriesPerLedger = 50000;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Minimum time between ledger rollover for a topic"
    )
    private int managedLedgerMinLedgerRolloverTimeMinutes = 10;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Maximum time before forcing a ledger rollover for a topic"
    )
    private int managedLedgerMaxLedgerRolloverTimeMinutes = 240;
    @FieldContext(
        category = CATEGORY_STORAGE_OFFLOADING,
        doc = "Delay between a ledger being successfully offloaded to long term storage,"
            + " and the ledger being deleted from bookkeeper"
    )
    private long managedLedgerOffloadDeletionLagMs = TimeUnit.HOURS.toMillis(4);
    @FieldContext(
        category = CATEGORY_STORAGE_OFFLOADING,
        doc = "The number of bytes before triggering automatic offload to long term storage"
    )
    private long managedLedgerOffloadAutoTriggerSizeThresholdBytes = -1L;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Max number of entries to append to a cursor ledger"
    )
    private int managedLedgerCursorMaxEntriesPerLedger = 50000;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Max time before triggering a rollover on a cursor ledger"
    )
    private int managedLedgerCursorRolloverTimeInSeconds = 14400;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Max number of `acknowledgment holes` that are going to be persistently stored.\n\n"
            + "When acknowledging out of order, a consumer will leave holes that are supposed"
            + " to be quickly filled by acking all the messages. The information of which"
            + " messages are acknowledged is persisted by compressing in `ranges` of messages"
            + " that were acknowledged. After the max number of ranges is reached, the information"
            + " will only be tracked in memory and messages will be redelivered in case of"
            + " crashes.")
    private int managedLedgerMaxUnackedRangesToPersist = 10000;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "Max number of `acknowledgment holes` that can be stored in Zookeeper.\n\n"
            + "If number of unack message range is higher than this limit then broker will persist"
            + " unacked ranges into bookkeeper to avoid additional data overhead into zookeeper.")
    private int managedLedgerMaxUnackedRangesToPersistInZooKeeper = 1000;
    @FieldContext(
            category = CATEGORY_STORAGE_OFFLOADING,
            doc = "Use Open Range-Set to cache unacked messages (it is memory efficient but it can take more cpu)"
        )
    private boolean managedLedgerUnackedRangesOpenCacheSetEnabled = true;
    @FieldContext(
        dynamic = true,
        category = CATEGORY_STORAGE_ML,
        doc = "Skip reading non-recoverable/unreadable data-ledger under managed-ledger's list.\n\n"
            + " It helps when data-ledgers gets corrupted at bookkeeper and managed-cursor is stuck at that ledger."
    )
    private boolean autoSkipNonRecoverableData = false;
    @FieldContext(
        category = CATEGORY_STORAGE_ML,
        doc = "operation timeout while updating managed-ledger metadata."
    )
    private long managedLedgerMetadataOperationsTimeoutSeconds = 60;

    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Read entries timeout when broker tries to read messages from bookkeeper "
                    + "(0 to disable it)"
        )
    private long managedLedgerReadEntryTimeoutSeconds = 0;

    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "Add entry timeout when broker tries to publish message to bookkeeper.(0 to disable it)")
    private long managedLedgerAddEntryTimeoutSeconds = 0;

    /*** --- Load balancer --- ****/
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        doc = "Enable load balancer"
    )
    private boolean loadBalancerEnabled = true;
    @Deprecated
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        deprecated = true,
        doc = "load placement strategy[weightedRandomSelection/leastLoadedServer] (only used by SimpleLoadManagerImpl)"
    )
    private String loadBalancerPlacementStrategy = "leastLoadedServer"; // weighted random selection
    @FieldContext(
        dynamic = true,
        category = CATEGORY_LOAD_BALANCER,
        doc = "Percentage of change to trigger load report update"
    )
    private int loadBalancerReportUpdateThresholdPercentage = 10;
    @FieldContext(
        dynamic = true,
        category = CATEGORY_LOAD_BALANCER,
        doc = "maximum interval to update load report"
    )
    private int loadBalancerReportUpdateMaxIntervalMinutes = 15;
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        doc = "Frequency of report to collect, in minutes"
    )
    private int loadBalancerHostUsageCheckIntervalMinutes = 1;
    @FieldContext(
        dynamic = true,
        category = CATEGORY_LOAD_BALANCER,
        doc = "Enable/disable automatic bundle unloading for load-shedding"
    )
    private boolean loadBalancerSheddingEnabled = true;
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        doc = "Load shedding interval. \n\nBroker periodically checks whether some traffic"
            + " should be offload from some over-loaded broker to other under-loaded brokers"
    )
    private int loadBalancerSheddingIntervalMinutes = 1;
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        doc = "Prevent the same topics to be shed and moved to other broker more that"
            + " once within this timeframe"
    )
    private long loadBalancerSheddingGracePeriodMinutes = 30;
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        deprecated = true,
        doc = "Usage threshold to determine a broker as under-loaded (only used by SimpleLoadManagerImpl)"
    )
    @Deprecated
    private int loadBalancerBrokerUnderloadedThresholdPercentage = 50;
    @FieldContext(
        dynamic = true,
        category = CATEGORY_LOAD_BALANCER,
        doc = "Usage threshold to allocate max number of topics to broker"
    )
    private int loadBalancerBrokerMaxTopics = 50000;

    @FieldContext(
        dynamic = true,
        category = CATEGORY_LOAD_BALANCER,
        doc = "Usage threshold to determine a broker as over-loaded"
    )
    private int loadBalancerBrokerOverloadedThresholdPercentage = 85;
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        doc = "Interval to flush dynamic resource quota to ZooKeeper"
    )
    private int loadBalancerResourceQuotaUpdateIntervalMinutes = 15;
    @Deprecated
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        deprecated = true,
        doc = "Usage threshold to determine a broker is having just right level of load"
            + " (only used by SimpleLoadManagerImpl)"
    )
    private int loadBalancerBrokerComfortLoadLevelPercentage = 65;
    @FieldContext(
        dynamic = true,
        category = CATEGORY_LOAD_BALANCER,
        doc = "enable/disable automatic namespace bundle split"
    )
    private boolean loadBalancerAutoBundleSplitEnabled = true;
    @FieldContext(
        dynamic = true,
        category = CATEGORY_LOAD_BALANCER,
        doc = "enable/disable automatic unloading of split bundles"
    )
    private boolean loadBalancerAutoUnloadSplitBundlesEnabled = true;
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        doc = "maximum topics in a bundle, otherwise bundle split will be triggered"
    )
    private int loadBalancerNamespaceBundleMaxTopics = 1000;
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        doc = "maximum sessions (producers + consumers) in a bundle, otherwise bundle split will be triggered"
    )
    private int loadBalancerNamespaceBundleMaxSessions = 1000;
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        doc = "maximum msgRate (in + out) in a bundle, otherwise bundle split will be triggered"
    )
    private int loadBalancerNamespaceBundleMaxMsgRate = 30000;
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        doc = "maximum bandwidth (in + out) in a bundle, otherwise bundle split will be triggered"
    )
    private int loadBalancerNamespaceBundleMaxBandwidthMbytes = 100;
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        doc = "maximum number of bundles in a namespace"
    )
    private int loadBalancerNamespaceMaximumBundles = 128;
    @FieldContext(
        dynamic = true,
        category = CATEGORY_LOAD_BALANCER,
        doc = "Name of load manager to use"
    )
    private String loadManagerClassName = "org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl";
    @FieldContext(
        dynamic = true,
        category = CATEGORY_LOAD_BALANCER,
        doc = "Supported algorithms name for namespace bundle split"
    )
    private List<String> supportedNamespaceBundleSplitAlgorithms = Lists.newArrayList("range_equally_divide", "topic_count_equally_divide");
    @FieldContext(
        dynamic = true,
        category = CATEGORY_LOAD_BALANCER,
        doc = "Default algorithm name for namespace bundle split"
    )
    private String defaultNamespaceBundleSplitAlgorithm = "range_equally_divide";
    @FieldContext(
        category = CATEGORY_LOAD_BALANCER,
        doc = "Option to override the auto-detected network interfaces max speed"
    )
    private Double loadBalancerOverrideBrokerNicSpeedGbps;

    /**** --- Replication --- ****/
    @FieldContext(
        category = CATEGORY_REPLICATION,
        doc = "Enable replication metrics"
    )
    private boolean replicationMetricsEnabled = false;
    @FieldContext(
        category = CATEGORY_REPLICATION,
        doc = "Max number of connections to open for each broker in a remote cluster.\n\n"
            + "More connections host-to-host lead to better throughput over high-latency links"
    )
    private int replicationConnectionsPerBroker = 16;
    @FieldContext(
        required = false,
        category = CATEGORY_REPLICATION,
        doc = "replicator prefix used for replicator producer name and cursor name"
    )
    private String replicatorPrefix = "pulsar.repl";
    @FieldContext(
        category = CATEGORY_REPLICATION,
        doc = "Replicator producer queue size"
    )
    private int replicationProducerQueueSize = 1000;
    @FieldContext(
            category = CATEGORY_REPLICATION,
            doc = "Duration to check replication policy to avoid replicator "
                    + "inconsistency due to missing ZooKeeper watch (disable with value 0)"
        )
    private int replicatioPolicyCheckDurationSeconds = 600;
    @Deprecated
    @FieldContext(
        category = CATEGORY_REPLICATION,
        deprecated = true,
        doc = "@deprecated - Use brokerClientTlsEnabled instead."
    )
    private boolean replicationTlsEnabled = false;
    @FieldContext(
        category = CATEGORY_REPLICATION,
        dynamic = true,
        doc = "Enable TLS when talking with other brokers in the same cluster (admin operation)"
            + " or different clusters (replication)"
    )
    private boolean brokerClientTlsEnabled = false;

    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Default message retention time"
    )
    private int defaultRetentionTimeInMinutes = 0;
    @FieldContext(
        category = CATEGORY_POLICIES,
        doc = "Default retention size"
    )
    private int defaultRetentionSizeInMB = 0;
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "How often to check pulsar connection is still alive"
    )
    private int keepAliveIntervalSeconds = 30;
    @Deprecated
    @FieldContext(
        category = CATEGORY_POLICIES,
        deprecated = true,
        doc = "How often broker checks for inactive topics to be deleted (topics with no subscriptions and no one connected)"
            + "Deprecated in favor of using `brokerDeleteInactiveTopicsFrequencySeconds`"
    )
    private int brokerServicePurgeInactiveFrequencyInSeconds = 60;
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "A comma-separated list of namespaces to bootstrap"
    )
    private List<String> bootstrapNamespaces = new ArrayList<String>();
    private Properties properties = new Properties();
    @FieldContext(
        dynamic = true,
        category = CATEGORY_SERVER,
        doc = "If true, (and ModularLoadManagerImpl is being used), the load manager will attempt to "
            + "use only brokers running the latest software version (to minimize impact to bundles)"
    )
    private boolean preferLaterVersions = false;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Interval between checks to see if topics with compaction policies need to be compacted"
    )
    private int brokerServiceCompactionMonitorIntervalInSeconds = 60;

    @FieldContext(
        category = CATEGORY_SCHEMA,
        doc = "Enforce schema validation on following cases:\n\n"
            + " - if a producer without a schema attempts to produce to a topic with schema, the producer will be\n"
            + "   failed to connect. PLEASE be carefully on using this, since non-java clients don't support schema.\n"
            + "   if you enable this setting, it will cause non-java clients failed to produce."
    )
    private boolean isSchemaValidationEnforced = false;
    @FieldContext(
        category = CATEGORY_SCHEMA,
        doc = "The schema storage implementation used by this broker"
    )
    private String schemaRegistryStorageClassName = "org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorageFactory";
    @FieldContext(
        category = CATEGORY_SCHEMA,
        doc = "The list compatibility checkers to be used in schema registry"
    )
    private Set<String> schemaRegistryCompatibilityCheckers = Sets.newHashSet(
            "org.apache.pulsar.broker.service.schema.JsonSchemaCompatibilityCheck",
            "org.apache.pulsar.broker.service.schema.AvroSchemaCompatibilityCheck"
    );

    /**** --- WebSocket --- ****/
    @FieldContext(
        category = CATEGORY_WEBSOCKET,
        doc = "Number of IO threads in Pulsar Client used in WebSocket proxy"
    )
    private int webSocketNumIoThreads = Runtime.getRuntime().availableProcessors();
    @FieldContext(
        category = CATEGORY_WEBSOCKET,
        doc = "Number of connections per Broker in Pulsar Client used in WebSocket proxy"
    )
    private int webSocketConnectionsPerBroker = Runtime.getRuntime().availableProcessors();
    @FieldContext(
        category = CATEGORY_WEBSOCKET,
        doc = "Time in milliseconds that idle WebSocket session times out"
    )
    private int webSocketSessionIdleTimeoutMillis = 300000;

    /**** --- Metrics --- ****/
    @FieldContext(
        category = CATEGORY_METRICS,
        doc = "If true, export topic level metrics otherwise namespace level"
    )
    private boolean exposeTopicLevelMetricsInPrometheus = true;
    @FieldContext(
        category = CATEGORY_METRICS,
        doc = "If true, export consumer level metrics otherwise namespace level"
    )
    private boolean exposeConsumerLevelMetricsInPrometheus = false;
    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "Classname of Pluggable JVM GC metrics logger that can log GC specific metrics")
    private String jvmGCMetricsLoggerClassName;

    @FieldContext(
        category = CATEGORY_METRICS,
        doc = "Enable expose the precise backlog stats.\n" +
            " Set false to use published counter and consumed counter to calculate,\n" +
            " this would be more efficient but may be inaccurate. Default is false."
    )
    private boolean exposePreciseBacklogInPrometheus = false;

    /**** --- Functions --- ****/
    @FieldContext(
        category = CATEGORY_FUNCTIONS,
        doc = "Flag indicates enabling or disabling function worker on brokers"
    )
    private boolean functionsWorkerEnabled = false;

    /**** --- Broker Web Stats --- ****/
    @FieldContext(
        category = CATEGORY_METRICS,
        doc = "If true, export publisher stats when returning topics stats from the admin rest api"
    )
    private boolean exposePublisherStats = true;
    @FieldContext(
        category = CATEGORY_METRICS,
        doc = "Stats update frequency in seconds"
    )
    private int statsUpdateFrequencyInSecs = 60;
    @FieldContext(
        category = CATEGORY_METRICS,
        doc = "Stats update initial delay in seconds"
    )
    private int statsUpdateInitialDelayInSecs = 60;

    /**** --- Ledger Offloading --- ****/
    /****
     * NOTES: all implementation related settings should be put in implementation package.
     *        only common settings like driver name, io threads can be added here.
     ****/
    @FieldContext(
        category = CATEGORY_STORAGE_OFFLOADING,
        doc = "The directory to locate offloaders"
    )
    private String offloadersDirectory = "./offloaders";

    @FieldContext(
        category = CATEGORY_STORAGE_OFFLOADING,
        doc = "Driver to use to offload old data to long term storage"
    )
    private String managedLedgerOffloadDriver = null;

    @FieldContext(
        category = CATEGORY_STORAGE_OFFLOADING,
        doc = "Maximum number of thread pool threads for ledger offloading"
    )
    private int managedLedgerOffloadMaxThreads = 2;

    /**** --- Transaction config variables --- ****/
    @FieldContext(
            category = CATEGORY_TRANSACTION,
            doc = "Enable transaction coordinator in broker"
    )
    private boolean transactionCoordinatorEnabled = true;

    @FieldContext(
        category = CATEGORY_TRANSACTION,
            doc = "Class name for transaction metadata store provider"
    )
    private String transactionMetadataStoreProviderClassName =
            "org.apache.pulsar.transaction.coordinator.impl.InMemTransactionMetadataStoreProvider";

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
        return brokerServicePort;
    }

    public Optional<Integer> getBrokerServicePortTls() {
        return brokerServicePortTls;
    }

    public Optional<Integer> getWebServicePort() {
        return webServicePort;
    }

    public Optional<Integer> getWebServicePortTls() {
        return webServicePortTls;
    }

    public boolean isDefaultTopicTypePartitioned() {
        return TopicType.PARTITIONED.toString().equals(allowAutoTopicCreationType);
    }

    public int getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds() {
        if (brokerDeleteInactiveTopicsMaxInactiveDurationSeconds == null) {
            return brokerDeleteInactiveTopicsFrequencySeconds;
        } else {
            return brokerDeleteInactiveTopicsMaxInactiveDurationSeconds;
        }
    }
}
