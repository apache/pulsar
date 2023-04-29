/*
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
package org.apache.pulsar.broker.configuration;

import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;

import java.util.Properties;
import java.util.Set;

import static org.apache.pulsar.broker.ServiceConfiguration.CATEGORY_POLICIES;

@Getter
@Setter
@ToString
public class PoliciesConfiguration implements PulsarConfiguration {
    private ServiceConfiguration serviceConfiguration;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Enable backlog quota check. Enforces actions on topic when the quota is reached"
    )
    private boolean backlogQuotaCheckEnabled = true;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Whether to enable precise time based backlog quota check. "
                    + "Enabling precise time based backlog quota check will cause broker to read first entry in backlog "
                    + "of the slowest cursor on a ledger which will mostly result in reading entry from BookKeeper's "
                    + "disk which can have negative impact on overall performance. "
                    + "Disabling precise time based backlog quota check will just use the timestamp indicating when a "
                    + "ledger was closed, which is of coarser granularity."
    )
    private boolean preciseTimeBasedBacklogQuotaCheck = false;

    @FieldContext(
            category = CATEGORY_POLICIES,
            minValue = 1,
            doc = "How often to check for topics that have reached the quota."
                    + " It only takes effects when `backlogQuotaCheckEnabled` is true"
    )
    private int backlogQuotaCheckIntervalInSeconds = 60;

    @Deprecated
    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "@deprecated - Use backlogQuotaDefaultLimitByte instead."
    )
    private double backlogQuotaDefaultLimitGB = -1;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Default per-topic backlog quota limit by size, less than 0 means no limitation. default is -1."
                    + " Increase it if you want to allow larger msg backlog"
    )
    private long backlogQuotaDefaultLimitBytes = -1;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Default per-topic backlog quota limit by time in second, less than 0 means no limitation. "
                    + "default is -1. Increase it if you want to allow larger msg backlog"
    )
    private int backlogQuotaDefaultLimitSecond = -1;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Default backlog quota retention policy. Default is producer_request_hold\n\n"
                    + "'producer_request_hold' Policy which holds producer's send request until the"
                    + "resource becomes available (or holding times out)\n"
                    + "'producer_exception' Policy which throws javax.jms.ResourceAllocationException to the producer\n"
                    + "'consumer_backlog_eviction' Policy which evicts the oldest message from the slowest consumer's backlog"
    )
    private BacklogQuota.RetentionPolicy backlogQuotaDefaultRetentionPolicy = BacklogQuota.RetentionPolicy
            .producer_request_hold;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Default ttl for namespaces if ttl is not already configured at namespace policies. "
                    + "(disable default-ttl with value 0)"
    )
    private int ttlDurationDefaultInSeconds = 0;

    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Enable the deletion of inactive topics.\n"
                    + "If only enable this option, will not clean the metadata of partitioned topic."
    )
    private boolean brokerDeleteInactiveTopicsEnabled = true;
    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Metadata of inactive partitioned topic will not be automatically cleaned up by default.\n"
                    + "Note: If `allowAutoTopicCreation` and this option are enabled at the same time,\n"
                    + "it may appear that a partitioned topic has just been deleted but is automatically created as a "
                    + "non-partitioned topic."
    )
    private boolean brokerDeleteInactivePartitionedTopicMetadataEnabled = false;
    @FieldContext(
            category = CATEGORY_POLICIES,
            minValue = 1,
            dynamic = true,
            doc = "How often to check for inactive topics"
    )
    private int brokerDeleteInactiveTopicsFrequencySeconds = 60;

    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Set the inactive topic delete mode. Default is delete_when_no_subscriptions\n"
                    + "'delete_when_no_subscriptions' mode only delete the topic which has no subscriptions and no active "
                    + "producers\n"
                    + "'delete_when_subscriptions_caught_up' mode only delete the topic that all subscriptions has no "
                    + "backlogs(caught up) and no active producers/consumers"
    )
    private InactiveTopicDeleteMode brokerDeleteInactiveTopicsMode = InactiveTopicDeleteMode.
            delete_when_no_subscriptions;

    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Max duration of topic inactivity in seconds, default is not present\n"
                    + "If not present, 'brokerDeleteInactiveTopicsFrequencySeconds' will be used\n"
                    + "Topics that are inactive for longer than this value will be deleted"
    )
    private Integer brokerDeleteInactiveTopicsMaxInactiveDurationSeconds = null;

    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Allow forced deletion of tenants. Default is false."
    )
    private boolean forceDeleteTenantAllowed = false;

    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Allow forced deletion of namespaces. Default is false."
    )
    private boolean forceDeleteNamespaceAllowed = false;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Max pending publish requests per connection to avoid keeping large number of pending "
                    + "requests in memory. Default: 1000"
    )
    private int maxPendingPublishRequestsPerConnection = 1000;

    @FieldContext(
            category = CATEGORY_POLICIES,
            minValue = 1,
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
            doc = "Maximum time to spend while scanning a subscription to calculate the accurate backlog"
    )
    private long subscriptionBacklogScanMaxTimeMs = 1000 * 60 * 2L;
    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Maximum number of entries to process while scanning a subscription to calculate the accurate backlog"
    )
    private long subscriptionBacklogScanMaxEntries = 10_000;
    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "How long to delete inactive subscriptions from last consuming."
                    + " When it is 0, inactive subscriptions are not deleted automatically"
    )
    private int subscriptionExpirationTimeMinutes = 0;
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
    private int subscriptionExpiryCheckIntervalInMinutes = 5;

    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Enable subscription types (default is all type enabled)"
    )
    private Set<String> subscriptionTypesEnabled =
            Sets.newHashSet("Exclusive", "Shared", "Failover", "Key_Shared");

    @Deprecated
    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Enable Key_Shared subscription (default is enabled).\n"
                    + "@deprecated - use subscriptionTypesEnabled instead."
    )
    private boolean subscriptionKeySharedEnable = true;

    @FieldContext(category = CATEGORY_POLICIES,
            doc = "On KeyShared subscriptions, with default AUTO_SPLIT mode, use splitting ranges or "
                    + "consistent hashing to reassign keys to new consumers (default is consistent hashing)")
    private boolean subscriptionKeySharedUseConsistentHashing = true;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "On KeyShared subscriptions, number of points in the consistent-hashing ring. "
                    + "The higher the number, the more equal the assignment of keys to consumers")
    private int subscriptionKeySharedConsistentHashingReplicaPoints = 100;

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
            doc = "How often is the thread pool scheduled to check whether a snapshot needs to be taken."
                    + "(disable with value 0)"
    )
    private int brokerDeduplicationSnapshotFrequencyInSeconds = 120;
    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "If this time interval is exceeded, a snapshot will be taken."
                    + "It will run simultaneously with `brokerDeduplicationEntriesInterval`"
    )
    private Integer brokerDeduplicationSnapshotIntervalSeconds = 120;

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
            minValue = 1,
            doc = "Time of inactivity after which the broker will discard the deduplication information"
                    + " relative to a disconnected producer. Default is 6 hours.")
    private int brokerDeduplicationProducerInactivityTimeoutMinutes = 360;

    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "When a namespace is created without specifying the number of bundle, this"
                    + " value will be used as the default")
    private int defaultNumberOfNamespaceBundles = 4;

    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "The maximum number of namespaces that each tenant can create."
                    + "This configuration is not precise control, in a concurrent scenario, the threshold will be exceeded")
    private int maxNamespacesPerTenant = 0;

    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Max number of topics allowed to be created in the namespace. "
                    + "When the topics reach the max topics of the namespace, the broker should reject "
                    + "the new topic request(include topic auto-created by the producer or consumer) until "
                    + "the number of connected consumers decrease. "
                    + " Using a value of 0, is disabling maxTopicsPerNamespace-limit check."
    )
    private int maxTopicsPerNamespace = 0;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "The maximum number of connections in the broker. If it exceeds, new connections are rejected."
    )
    private int brokerMaxConnections = 0;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "The maximum number of connections per IP. If it exceeds, new connections are rejected."
    )
    private int brokerMaxConnectionsPerIp = 0;

    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Allow schema to be auto updated at broker level. User can override this by 'is_allow_auto_update_schema'"
                    + " of namespace policy. This is enabled by default."
    )
    private boolean isAllowAutoUpdateSchemaEnabled = true;

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
            doc = "Maximum size of Consumer metadata")
    private int maxConsumerMetadataSize = 1024;
    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Broker periodically checks if subscription is stuck and unblock if flag is enabled. "
                    + "(Default is disabled)"
    )
    private boolean unblockStuckSubscriptionEnabled = false;
    @FieldContext(
            category = CATEGORY_POLICIES,
            dynamic = true,
            doc = "Tick time to schedule task that checks topic publish rate limiting across all topics  "
                    + "Reducing to lower value can give more accuracy while throttling publish but "
                    + "it uses more CPU to perform frequent check. (Disable publish throttling with value 0)"
    )
    private int topicPublisherThrottlingTickTimeMillis = 10;


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
            doc = "Apply dispatch rate limiting on batch message instead individual "
                    + "messages with in batch message. (Default is disabled)")
    private boolean dispatchThrottlingOnBatchMessageEnabled = false;

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
    private boolean dispatchThrottlingOnNonBacklogConsumerEnabled = true;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Default policy for publishing usage reports to system topic is disabled."
                    + "This enables publishing of usage reports"
    )
    private String resourceUsageTransportClassName = "";

    @FieldContext(
            dynamic = true,
            category = CATEGORY_POLICIES,
            doc = "Default interval to publish usage reports if resourceUsagePublishToTopic is enabled."
    )
    private int resourceUsageTransportPublishIntervalInSecs = 60;

    @FieldContext(
            dynamic = false,
            category = CATEGORY_POLICIES,
            doc = "Enables evaluating subscription pattern on broker side."
    )
    private boolean enableBrokerSideSubscriptionPatternEvaluation = true;

    @FieldContext(
            dynamic = false,
            category = CATEGORY_POLICIES,
            doc = "Max length of subscription pattern"
    )
    private int subscriptionPatternMaxLength = 50;

    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Default message retention time."
                    + " 0 means retention is disabled. -1 means data is not removed by time quota"
    )
    private int defaultRetentionTimeInMinutes = 0;
    @FieldContext(
            category = CATEGORY_POLICIES,
            doc = "Default retention size."
                    + " 0 means retention is disabled. -1 means data is not removed by size quota"
    )
    private int defaultRetentionSizeInMB = 0;


    @ToString.Exclude
    @com.fasterxml.jackson.annotation.JsonIgnore
    private Properties properties = new Properties();

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

    /**
     * @deprecated Use {@link #getSubscriptionTypesEnabled()} instead
     */
    @Deprecated
    public boolean isSubscriptionKeySharedEnable() {
        return subscriptionKeySharedEnable && subscriptionTypesEnabled.contains("Key_Shared");
    }

    public int getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds() {
        if (brokerDeleteInactiveTopicsMaxInactiveDurationSeconds == null) {
            return brokerDeleteInactiveTopicsFrequencySeconds;
        } else {
            return brokerDeleteInactiveTopicsMaxInactiveDurationSeconds;
        }
    }
}
