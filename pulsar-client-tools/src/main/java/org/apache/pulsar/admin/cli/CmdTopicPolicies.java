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
package org.apache.pulsar.admin.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.util.RelativeTimeUtil;

@Parameters(commandDescription = "Operations on persistent topics")
public class CmdTopicPolicies extends CmdBase {

    public CmdTopicPolicies(Supplier<PulsarAdmin> admin) {
        super("topicPolicies", admin);

        jcommander.addCommand("get-message-ttl", new GetMessageTTL());
        jcommander.addCommand("set-message-ttl", new SetMessageTTL());
        jcommander.addCommand("remove-message-ttl", new RemoveMessageTTL());

        jcommander.addCommand("get-max-unacked-messages-per-consumer", new GetMaxUnackedMessagesPerConsumer());
        jcommander.addCommand("set-max-unacked-messages-per-consumer", new SetMaxUnackedMessagesPerConsumer());
        jcommander.addCommand("remove-max-unacked-messages-per-consumer", new RemoveMaxUnackedMessagesPerConsumer());

        jcommander.addCommand("get-max-consumers-per-subscription", new GetMaxConsumersPerSubscription());
        jcommander.addCommand("set-max-consumers-per-subscription", new SetMaxConsumersPerSubscription());
        jcommander.addCommand("remove-max-consumers-per-subscription", new RemoveMaxConsumersPerSubscription());
        jcommander.addCommand("set-subscription-types-enabled", new SetSubscriptionTypesEnabled());
        jcommander.addCommand("get-subscription-types-enabled", new GetSubscriptionTypesEnabled());
        jcommander.addCommand("remove-subscription-types-enabled", new RemoveSubscriptionTypesEnabled());
        jcommander.addCommand("get-retention", new GetRetention());
        jcommander.addCommand("set-retention", new SetRetention());
        jcommander.addCommand("remove-retention", new RemoveRetention());
        jcommander.addCommand("get-backlog-quota", new GetBacklogQuotaMap());
        jcommander.addCommand("set-backlog-quota", new SetBacklogQuota());
        jcommander.addCommand("remove-backlog-quota", new RemoveBacklogQuota());

        jcommander.addCommand("get-max-producers", new GetMaxProducers());
        jcommander.addCommand("set-max-producers", new SetMaxProducers());
        jcommander.addCommand("remove-max-producers", new RemoveMaxProducers());

        jcommander.addCommand("get-max-message-size", new GetMaxMessageSize());
        jcommander.addCommand("set-max-message-size", new SetMaxMessageSize());
        jcommander.addCommand("remove-max-message-size", new RemoveMaxMessageSize());

        jcommander.addCommand("set-deduplication", new SetDeduplicationStatus());
        jcommander.addCommand("get-deduplication", new GetDeduplicationStatus());
        jcommander.addCommand("remove-deduplication", new RemoveDeduplicationStatus());

        jcommander.addCommand("get-deduplication-snapshot-interval", new GetDeduplicationSnapshotInterval());
        jcommander.addCommand("set-deduplication-snapshot-interval", new SetDeduplicationSnapshotInterval());
        jcommander.addCommand("remove-deduplication-snapshot-interval", new RemoveDeduplicationSnapshotInterval());

        jcommander.addCommand("get-persistence", new GetPersistence());
        jcommander.addCommand("set-persistence", new SetPersistence());
        jcommander.addCommand("remove-persistence", new RemovePersistence());

        jcommander.addCommand("get-subscription-dispatch-rate", new GetSubscriptionDispatchRate());
        jcommander.addCommand("set-subscription-dispatch-rate", new SetSubscriptionDispatchRate());
        jcommander.addCommand("remove-subscription-dispatch-rate", new RemoveSubscriptionDispatchRate());

        jcommander.addCommand("get-replicator-dispatch-rate", new GetReplicatorDispatchRate());
        jcommander.addCommand("set-replicator-dispatch-rate", new SetReplicatorDispatchRate());
        jcommander.addCommand("remove-replicator-dispatch-rate", new RemoveReplicatorDispatchRate());

        jcommander.addCommand("get-publish-rate", new GetPublishRate());
        jcommander.addCommand("set-publish-rate", new SetPublishRate());
        jcommander.addCommand("remove-publish-rate", new RemovePublishRate());

        jcommander.addCommand("get-compaction-threshold", new GetCompactionThreshold());
        jcommander.addCommand("set-compaction-threshold", new SetCompactionThreshold());
        jcommander.addCommand("remove-compaction-threshold", new RemoveCompactionThreshold());

        jcommander.addCommand("get-subscribe-rate", new GetSubscribeRate());
        jcommander.addCommand("set-subscribe-rate", new SetSubscribeRate());
        jcommander.addCommand("remove-subscribe-rate", new RemoveSubscribeRate());

        jcommander.addCommand("get-max-consumers", new GetMaxConsumers());
        jcommander.addCommand("set-max-consumers", new SetMaxConsumers());
        jcommander.addCommand("remove-max-consumers", new RemoveMaxConsumers());

        jcommander.addCommand("get-delayed-delivery", new GetDelayedDelivery());
        jcommander.addCommand("set-delayed-delivery", new SetDelayedDelivery());
        jcommander.addCommand("remove-delayed-delivery", new RemoveDelayedDelivery());

        jcommander.addCommand("get-dispatch-rate", new GetDispatchRate());
        jcommander.addCommand("set-dispatch-rate", new SetDispatchRate());
        jcommander.addCommand("remove-dispatch-rate", new RemoveDispatchRate());

        jcommander.addCommand("get-offload-policies", new GetOffloadPolicies());
        jcommander.addCommand("set-offload-policies", new SetOffloadPolicies());
        jcommander.addCommand("remove-offload-policies", new RemoveOffloadPolicies());

        jcommander.addCommand("get-max-unacked-messages-per-subscription", new GetMaxUnackedMessagesPerSubscription());
        jcommander.addCommand("set-max-unacked-messages-per-subscription", new SetMaxUnackedMessagesPerSubscription());
        jcommander.addCommand("remove-max-unacked-messages-per-subscription",
                new RemoveMaxUnackedMessagesPerSubscription());

        jcommander.addCommand("get-inactive-topic-policies", new GetInactiveTopicPolicies());
        jcommander.addCommand("set-inactive-topic-policies", new SetInactiveTopicPolicies());
        jcommander.addCommand("remove-inactive-topic-policies", new RemoveInactiveTopicPolicies());

        jcommander.addCommand("get-max-subscriptions-per-topic", new GetMaxSubscriptionsPerTopic());
        jcommander.addCommand("set-max-subscriptions-per-topic", new SetMaxSubscriptionsPerTopic());
        jcommander.addCommand("remove-max-subscriptions-per-topic", new RemoveMaxSubscriptionsPerTopic());

        jcommander.addCommand("remove-schema-compatibility-strategy", new RemoveSchemaCompatibilityStrategy());
        jcommander.addCommand("set-schema-compatibility-strategy", new SetSchemaCompatibilityStrategy());
        jcommander.addCommand("get-schema-compatibility-strategy", new GetSchemaCompatibilityStrategy());
    }

    @Parameters(commandDescription = "Get max consumers per subscription for a topic")
    private class GetMaxConsumersPerSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getMaxConsumersPerSubscription(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Set max consumers per subscription for a topic")
    private class SetMaxConsumersPerSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--max-consumers-per-subscription", "-c" },
                description = "maxConsumersPerSubscription for a namespace", required = true)
        private int maxConsumersPerSubscription;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setMaxConsumersPerSubscription(persistentTopic, maxConsumersPerSubscription);
        }
    }

    @Parameters(commandDescription = "Remove max consumers per subscription for a topic")
    private class RemoveMaxConsumersPerSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeMaxConsumersPerSubscription(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get max unacked messages policy per consumer for a topic")
    private class GetMaxUnackedMessagesPerConsumer extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getMaxUnackedMessagesOnConsumer(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Remove max unacked messages policy per consumer for a topic")
    private class RemoveMaxUnackedMessagesPerConsumer extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeMaxUnackedMessagesOnConsumer(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Set max unacked messages policy per consumer for a topic")
    private class SetMaxUnackedMessagesPerConsumer extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-m", "--maxNum"}, description = "max unacked messages num on consumer", required = true)
        private int maxNum;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setMaxUnackedMessagesOnConsumer(persistentTopic, maxNum);
        }
    }

    @Parameters(commandDescription = "Get the message TTL for a topic")
    private class GetMessageTTL extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getMessageTTL(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set message TTL for a topic")
    private class SetMessageTTL extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-t", "--ttl" }, description = "Message TTL for topic in second, "
                + "allowed range from 1 to Integer.MAX_VALUE", required = true)
        private int messageTTLInSecond;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            if (messageTTLInSecond < 0) {
                throw new ParameterException(String.format("Invalid retention policy type '%d'. ", messageTTLInSecond));
            }

            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setMessageTTL(persistentTopic, messageTTLInSecond);
        }
    }

    @Parameters(commandDescription = "Remove message TTL for a topic")
    private class RemoveMessageTTL extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeMessageTTL(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Set subscription types enabled for a topic")
    private class SetSubscriptionTypesEnabled extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--types", "-t"}, description = "Subscription types enabled list (comma separated values)."
                + " Possible values: (Exclusive, Shared, Failover, Key_Shared).", required = true)
        private List<String> subTypes;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            Set<SubscriptionType> types = new HashSet<>();
            subTypes.forEach(s -> {
                SubscriptionType subType;
                try {
                    subType = SubscriptionType.valueOf(s);
                } catch (IllegalArgumentException exception) {
                    throw new ParameterException(String.format("Illegal subscription type %s. Possible values: %s.", s,
                            Arrays.toString(SubscriptionType.values())));
                }
                types.add(subType);
            });
            getTopicPolicies(isGlobal).setSubscriptionTypesEnabled(persistentTopic, types);
        }
    }

    @Parameters(commandDescription = "Get subscription types enabled for a topic")
    private class GetSubscriptionTypesEnabled extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getSubscriptionTypesEnabled(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Remove subscription types enabled for a topic")
    private class RemoveSubscriptionTypesEnabled extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeSubscriptionTypesEnabled(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get max number of consumers for a topic")
    private class GetMaxConsumers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getMaxConsumers(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set max number of consumers for a topic")
    private class SetMaxConsumers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--max-consumers", "-c" }, description = "Max consumers for a topic", required = true)
        private int maxConsumers;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setMaxConsumers(persistentTopic, maxConsumers);
        }
    }

    @Parameters(commandDescription = "Remove max number of consumers for a topic")
    private class RemoveMaxConsumers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeMaxConsumers(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get the retention policy for a topic")
    private class GetRetention extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getRetention(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set the retention policy for a topic")
    private class SetRetention extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private List<String> params;

        @Parameter(names = { "--time",
                "-t" }, description = "Retention time in minutes (or minutes, hours,days,weeks eg: 100m, 3h, 2d, 5w). "
                + "0 means no retention and -1 means infinite time retention", required = true)
        private String retentionTimeStr;

        @Parameter(names = { "--size", "-s" }, description = "Retention size limit (eg: 10M, 16G, 3T). "
                + "0 or less than 1MB means no retention and -1 means infinite size retention", required = true)
        private String limitStr;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            long sizeLimit = validateSizeString(limitStr);
            long retentionTimeInSec = RelativeTimeUtil.parseRelativeTimeInSeconds(retentionTimeStr);

            final int retentionTimeInMin;
            if (retentionTimeInSec != -1) {
                retentionTimeInMin = (int) TimeUnit.SECONDS.toMinutes(retentionTimeInSec);
            } else {
                retentionTimeInMin = -1;
            }

            final int retentionSizeInMB;
            if (sizeLimit != -1) {
                retentionSizeInMB = (int) (sizeLimit / (1024 * 1024));
            } else {
                retentionSizeInMB = -1;
            }
            getTopicPolicies(isGlobal).setRetention(persistentTopic,
                    new RetentionPolicies(retentionTimeInMin, retentionSizeInMB));
        }
    }

    @Parameters(commandDescription = "Remove the retention policy for a topic")
    private class RemoveRetention extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeRetention(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get max unacked messages policy per subscription for a topic")
    private class GetMaxUnackedMessagesPerSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getMaxUnackedMessagesOnSubscription(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Remove max unacked messages policy per subscription for a topic")
    private class RemoveMaxUnackedMessagesPerSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeMaxUnackedMessagesOnSubscription(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Set max unacked messages policy on subscription for a topic")
    private class SetMaxUnackedMessagesPerSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-m", "--maxNum"},
                description = "max unacked messages num on subscription", required = true)
        private int maxNum;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setMaxUnackedMessagesOnSubscription(persistentTopic, maxNum);
        }
    }

    @Parameters(commandDescription = "Get max number of producers for a topic")
    private class GetMaxProducers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getMaxProducers(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set max number of producers for a topic")
    private class SetMaxProducers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--max-producers", "-p"}, description = "Max producers for a topic", required = true)
        private int maxProducers;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setMaxProducers(persistentTopic, maxProducers);
        }
    }

    @Parameters(commandDescription = "Get the delayed delivery policy for a topic")
    private class GetDelayedDelivery extends CliCommand {
        @Parameter(description = "tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal;

        @Override
        void run() throws PulsarAdminException {
            String topicName = validateTopicName(params);
            print(getTopicPolicies(isGlobal).getDelayedDeliveryPolicy(topicName, applied));
        }
    }

    @Parameters(commandDescription = "Set the delayed delivery policy on a topic")
    private class SetDelayedDelivery extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable", "-e" }, description = "Enable delayed delivery messages")
        private boolean enable = false;

        @Parameter(names = { "--disable", "-d" }, description = "Disable delayed delivery messages")
        private boolean disable = false;

        @Parameter(names = { "--time", "-t" }, description = "The tick time for when retrying on "
                + "delayed delivery messages, affecting the accuracy of the delivery time compared to "
                + "the scheduled time. (eg: 1s, 10s, 1m, 5h, 3d)")
        private String delayedDeliveryTimeStr = "1s";

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal;

        @Override
        void run() throws PulsarAdminException {
            String topicName = validateTopicName(params);
            long delayedDeliveryTimeInMills;
            try {
                delayedDeliveryTimeInMills = TimeUnit.SECONDS.toMillis(
                        RelativeTimeUtil.parseRelativeTimeInSeconds(delayedDeliveryTimeStr));
            } catch (IllegalArgumentException exception) {
                throw new ParameterException(exception.getMessage());
            }

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }

            getTopicPolicies(isGlobal).setDelayedDeliveryPolicy(topicName, DelayedDeliveryPolicies.builder()
                    .tickTime(delayedDeliveryTimeInMills)
                    .active(enable)
                    .build());
        }
    }

    @Parameters(commandDescription = "Remove the delayed delivery policy on a topic")
    private class RemoveDelayedDelivery extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal;

        @Override
        void run() throws PulsarAdminException {
            String topicName = validateTopicName(params);
            getTopicPolicies(isGlobal).removeDelayedDeliveryPolicy(topicName);
        }
    }

    @Parameters(commandDescription = "Remove max number of producers for a topic")
    private class RemoveMaxProducers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeMaxProducers(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get max message size for a topic")
    private class GetMaxMessageSize extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returns global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getMaxMessageSize(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Set max message size for a topic")
    private class SetMaxMessageSize extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--max-message-size", "-m"}, description = "Max message size for a topic", required = true)
        private int maxMessageSize;

        @Parameter(names = {"--global", "-g"}, description = "Whether to set this policy globally.")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setMaxMessageSize(persistentTopic, maxMessageSize);
        }
    }

    @Parameters(commandDescription = "Remove max message size for a topic")
    private class RemoveMaxMessageSize extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--global", "-g"}, description = "Whether to remove this policy globally. ")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeMaxMessageSize(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Enable or disable status for a topic")
    private class SetDeduplicationStatus extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable", "-e" }, description = "Enable deduplication")
        private boolean enable = false;

        @Parameter(names = { "--disable", "-d" }, description = "Disable deduplication")
        private boolean disable = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            getTopicPolicies(isGlobal).setDeduplicationStatus(persistentTopic, enable);
        }
    }

    @Parameters(commandDescription = "Get the deduplication status for a topic")
    private class GetDeduplicationStatus extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. ")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getDeduplicationStatus(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Remove the deduplication status for a topic")
    private class RemoveDeduplicationStatus extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeDeduplicationStatus(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get deduplication snapshot interval for a topic")
    private class GetDeduplicationSnapshotInterval extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returns global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getDeduplicationSnapshotInterval(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Set deduplication snapshot interval for a topic")
    private class SetDeduplicationSnapshotInterval extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-i", "--interval"}, description =
                "Deduplication snapshot interval for topic in second, allowed range from 0 to Integer.MAX_VALUE",
                required = true)
        private int interval;

        @Parameter(names = {"--global", "-g"}, description = "Whether to set this policy globally.")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            if (interval < 0) {
                throw new ParameterException(String.format("Invalid interval '%d'. ", interval));
            }

            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setDeduplicationSnapshotInterval(persistentTopic, interval);
        }
    }

    @Parameters(commandDescription = "Remove deduplication snapshot interval for a topic")
    private class RemoveDeduplicationSnapshotInterval extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--global", "-g"}, description = "Whether to remove this policy globally. ")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeDeduplicationSnapshotInterval(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get the backlog quota policies for a topic")
    private class GetBacklogQuotaMap extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-ap", "--applied"}, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getBacklogQuotaMap(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set a backlog quota policy for a topic")
    private class SetBacklogQuota extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-l", "--limit" }, description = "Size limit (eg: 10M, 16G)")
        private String limitStr = "-1";

        @Parameter(names = { "-lt", "--limitTime" },
                description = "Time limit in second, non-positive number for disabling time limit.")
        private int limitTime = -1;

        @Parameter(names = { "-p", "--policy" }, description = "Retention policy to enforce when the limit is reached. "
                + "Valid options are: [producer_request_hold, producer_exception, consumer_backlog_eviction]",
                required = true)
        private String policyStr;

        @Parameter(names = {"-t", "--type"}, description = "Backlog quota type to set. Valid options are: "
                + "destination_storage and message_age. "
                + "destination_storage limits backlog by size (in bytes). "
                + "message_age limits backlog by time, that is, message timestamp (broker or publish timestamp). "
                + "You can set size or time to control the backlog, or combine them together to control the backlog. ")
        private String backlogQuotaTypeStr = BacklogQuota.BacklogQuotaType.destination_storage.name();

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            BacklogQuota.RetentionPolicy policy;
            long limit;
            BacklogQuota.BacklogQuotaType backlogQuotaType;

            try {
                policy = BacklogQuota.RetentionPolicy.valueOf(policyStr);
            } catch (IllegalArgumentException e) {
                throw new ParameterException(String.format("Invalid retention policy type '%s'. Valid options are: %s",
                        policyStr, Arrays.toString(BacklogQuota.RetentionPolicy.values())));
            }

            limit = validateSizeString(limitStr);

            try {
                backlogQuotaType = BacklogQuota.BacklogQuotaType.valueOf(backlogQuotaTypeStr);
            } catch (IllegalArgumentException e) {
                throw new ParameterException(String.format("Invalid backlog quota type '%s'. Valid options are: %s",
                        backlogQuotaTypeStr, Arrays.toString(BacklogQuota.BacklogQuotaType.values())));
            }

            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setBacklogQuota(persistentTopic,
                    BacklogQuota.builder().limitSize(limit)
                            .limitTime(limitTime)
                            .retentionPolicy(policy)
                            .build(),
                    backlogQuotaType);
        }
    }

    @Parameters(commandDescription = "Remove a backlog quota policy from a topic")
    private class RemoveBacklogQuota extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-t", "--type"}, description = "Backlog quota type to remove")
        private String backlogQuotaType = BacklogQuota.BacklogQuotaType.destination_storage.name();

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal)
                    .removeBacklogQuota(persistentTopic, BacklogQuota.BacklogQuotaType.valueOf(backlogQuotaType));
        }
    }

    @Parameters(commandDescription = "Get publish rate for a topic")
    private class GetPublishRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returns global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getPublishRate(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Set publish rate for a topic")
    private class SetPublishRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--msg-publish-rate", "-m"}, description = "message-publish-rate (default -1 will be "
                + "overwrite if not passed)", required = false)
        private int msgPublishRate = -1;

        @Parameter(names = {"--byte-publish-rate", "-b"}, description = "byte-publish-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private long bytePublishRate = -1;

        @Parameter(names = {"--global", "-g"}, description = "Whether to set this policy globally.")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setPublishRate(persistentTopic,
                    new PublishRate(msgPublishRate, bytePublishRate));
        }
    }

    @Parameters(commandDescription = "Remove publish rate for a topic")
    private class RemovePublishRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--global", "-g"}, description = "Whether to remove this policy globally. ")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removePublishRate(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get consumer subscribe rate for a topic")
    private class GetSubscribeRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returns global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getSubscribeRate(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set consumer subscribe rate for a topic")
    private class SetSubscribeRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--subscribe-rate",
                "-sr" }, description = "subscribe-rate (default -1 will be overwrite if not passed)", required = false)
        private int subscribeRate = -1;

        @Parameter(names = { "--subscribe-rate-period",
                "-st" }, description = "subscribe-rate-period in second type "
                + "(default 30 second will be overwrite if not passed)", required = false)
        private int subscribeRatePeriodSec = 30;

        @Parameter(names = {"--global", "-g"}, description = "Whether to set this policy globally.")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setSubscribeRate(persistentTopic,
                    new SubscribeRate(subscribeRate, subscribeRatePeriodSec));
        }
    }

    @Parameters(commandDescription = "Remove consumer subscribe rate for a topic")
    private class RemoveSubscribeRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--global", "-g"}, description = "Whether to remove this policy globally. ")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeSubscribeRate(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get the persistence policies for a topic")
    private class GetPersistence extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies", arity = 0)
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getPersistence(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Set the persistence policies for a topic")
    private class SetPersistence extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-e",
                "--bookkeeper-ensemble" }, description = "Number of bookies to use for a topic")
        private int bookkeeperEnsemble = 2;

        @Parameter(names = { "-w",
                "--bookkeeper-write-quorum" }, description = "How many writes to make of each entry")
        private int bookkeeperWriteQuorum = 2;

        @Parameter(names = { "-a", "--bookkeeper-ack-quorum" },
                description = "Number of acks (guaranteed copies) to wait for each entry")
        private int bookkeeperAckQuorum = 2;

        @Parameter(names = { "-r", "--ml-mark-delete-max-rate" },
                description = "Throttling rate of mark-delete operation (0 means no throttle)")
        private double managedLedgerMaxMarkDeleteRate = 0;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously", arity = 0)
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            if (bookkeeperEnsemble <= 0 || bookkeeperWriteQuorum <= 0 || bookkeeperAckQuorum <= 0) {
                throw new ParameterException("[--bookkeeper-ensemble], [--bookkeeper-write-quorum] "
                        + "and [--bookkeeper-ack-quorum] must greater than 0.");
            }
            if (managedLedgerMaxMarkDeleteRate < 0) {
                throw new ParameterException("[--ml-mark-delete-max-rate] cannot less than 0.");
            }
            getTopicPolicies(isGlobal).setPersistence(persistentTopic, new PersistencePolicies(bookkeeperEnsemble,
                    bookkeeperWriteQuorum, bookkeeperAckQuorum, managedLedgerMaxMarkDeleteRate));
        }
    }

    @Parameters(commandDescription = "Remove the persistence policy for a topic")
    private class RemovePersistence extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously"
                , arity = 0)
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removePersistence(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get compaction threshold for a topic")
    private class GetCompactionThreshold extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;
        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getCompactionThreshold(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set compaction threshold for a topic")
    private class SetCompactionThreshold extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--threshold", "-t" },
                description = "Maximum number of bytes in a topic backlog before compaction is triggered "
                        + "(eg: 10M, 16G, 3T). 0 disables automatic compaction",
                required = true)
        private String thresholdStr = "0";
        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            long threshold = validateSizeString(thresholdStr);
            getTopicPolicies(isGlobal).setCompactionThreshold(persistentTopic, threshold);
        }
    }

    @Parameters(commandDescription = "Remove compaction threshold for a topic")
    private class RemoveCompactionThreshold extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;
        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;
        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeCompactionThreshold(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get message dispatch rate for a topic")
    private class GetDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getDispatchRate(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set message dispatch rate for a topic")
    private class SetDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private int msgDispatchRate = -1;

        @Parameter(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private long byteDispatchRate = -1;

        @Parameter(names = { "--dispatch-rate-period",
                "-dt" }, description = "dispatch-rate-period in second type "
                + "(default 1 second will be overwrite if not passed)", required = false)
        private int dispatchRatePeriodSec = 1;

        @Parameter(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled "
                + "then broker will apply throttling value to (publish-rate + dispatch rate))", required = false)
        private boolean relativeToPublishRate = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setDispatchRate(persistentTopic,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Parameters(commandDescription = "Remove message dispatch rate for a topic")
    private class RemoveDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeDispatchRate(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get the inactive topic policies on a topic")
    private class GetInactiveTopicPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getInactiveTopicPolicies(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set the inactive topic policies on a topic")
    private class SetInactiveTopicPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable-delete-while-inactive", "-e" }, description = "Enable delete while inactive")
        private boolean enableDeleteWhileInactive = false;

        @Parameter(names = { "--disable-delete-while-inactive", "-d" }, description = "Disable delete while inactive")
        private boolean disableDeleteWhileInactive = false;

        @Parameter(names = {"--max-inactive-duration", "-t"},
                description = "Max duration of topic inactivity in seconds, topics that are inactive for longer than "
                        + "this value will be deleted (eg: 1s, 10s, 1m, 5h, 3d)", required = true)
        private String deleteInactiveTopicsMaxInactiveDuration;

        @Parameter(names = { "--delete-mode", "-m" }, description = "Mode of delete inactive topic, Valid options are: "
                + "[delete_when_no_subscriptions, delete_when_subscriptions_caught_up]", required = true)
        private String inactiveTopicDeleteMode;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            long maxInactiveDurationInSeconds;
            try {
                maxInactiveDurationInSeconds = TimeUnit.SECONDS.toSeconds(
                        RelativeTimeUtil.parseRelativeTimeInSeconds(deleteInactiveTopicsMaxInactiveDuration));
            } catch (IllegalArgumentException exception) {
                throw new ParameterException(exception.getMessage());
            }

            if (enableDeleteWhileInactive == disableDeleteWhileInactive) {
                throw new ParameterException("Need to specify either enable-delete-while-inactive or "
                        + "disable-delete-while-inactive");
            }
            InactiveTopicDeleteMode deleteMode;
            try {
                deleteMode = InactiveTopicDeleteMode.valueOf(inactiveTopicDeleteMode);
            } catch (IllegalArgumentException e) {
                throw new ParameterException("delete mode can only be set to delete_when_no_subscriptions or "
                        + "delete_when_subscriptions_caught_up");
            }
            getTopicPolicies(isGlobal).setInactiveTopicPolicies(persistentTopic, new InactiveTopicPolicies(deleteMode,
                    (int) maxInactiveDurationInSeconds, enableDeleteWhileInactive));
        }
    }

    @Parameters(commandDescription = "Remove inactive topic policies from a topic")
    private class RemoveInactiveTopicPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;
        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;
        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeInactiveTopicPolicies(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get replicator message-dispatch-rate for a topic")
    private class GetReplicatorDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getReplicatorDispatchRate(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set replicator message-dispatch-rate for a topic")
    private class SetReplicatorDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)")
        private int msgDispatchRate = -1;

        @Parameter(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)")
        private long byteDispatchRate = -1;

        @Parameter(names = {"--dispatch-rate-period",
                "-dt"}, description = "dispatch-rate-period in second type (default 1 second will be overwrite if not"
                + " passed)")
        private int dispatchRatePeriodSec = 1;

        @Parameter(names = {"--relative-to-publish-rate",
                "-rp"}, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled "
                + "then broker will apply throttling value to (publish-rate + dispatch rate))")
        private boolean relativeToPublishRate = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setReplicatorDispatchRate(persistentTopic,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Parameters(commandDescription = "Remove replicator message-dispatch-rate for a topic")
    private class RemoveReplicatorDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeReplicatorDispatchRate(persistentTopic);
        }

    }

    @Parameters(commandDescription = "Get subscription message-dispatch-rate for a topic")
    private class GetSubscriptionDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getSubscriptionDispatchRate(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set subscription message-dispatch-rate for a topic")
    private class SetSubscriptionDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)")
        private int msgDispatchRate = -1;

        @Parameter(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)")
        private long byteDispatchRate = -1;

        @Parameter(names = { "--dispatch-rate-period",
                "-dt" }, description = "dispatch-rate-period in second type "
                + "(default 1 second will be overwrite if not passed)")
        private int dispatchRatePeriodSec = 1;

        @Parameter(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled "
                + "then broker will apply throttling value to (publish-rate + dispatch rate))")
        private boolean relativeToPublishRate = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setSubscriptionDispatchRate(persistentTopic,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Parameters(commandDescription = "Remove subscription message-dispatch-rate for a topic")
    private class RemoveSubscriptionDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeSubscriptionDispatchRate(persistentTopic);
        }

    }

    @Parameters(commandDescription = "Get max subscriptions for a topic")
    private class GetMaxSubscriptionsPerTopic extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getMaxSubscriptionsPerTopic(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Set max subscriptions for a topic")
    private class SetMaxSubscriptionsPerTopic extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--max-subscriptions-per-topic",
                "-s"}, description = "max subscriptions for a topic (default -1 will be overwrite if not passed)",
                required = true)
        private int maxSubscriptionPerTopic;

        @Parameter(names = {"--global", "-g"}, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).setMaxSubscriptionsPerTopic(persistentTopic, maxSubscriptionPerTopic);
        }
    }

    @Parameters(commandDescription = "Remove max subscriptions for a topic")
    private class RemoveMaxSubscriptionsPerTopic extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--global", "-g"}, description = "Whether to remove this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeMaxSubscriptionsPerTopic(persistentTopic);
        }
    }


    @Parameters(commandDescription = "Get the offload policies for a topic")
    private class GetOffloadPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getOffloadPolicies(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Remove the offload policies for a topic")
    private class RemoveOffloadPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeOffloadPolicies(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Set the offload policies for a topic")
    private class SetOffloadPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-d", "--driver"}, description = "ManagedLedger offload driver", required = true)
        private String driver;

        @Parameter(names = {"-r", "--region"}
                , description = "ManagedLedger offload region, s3 and google-cloud-storage requires this parameter")
        private String region;

        @Parameter(names = {"-b", "--bucket"}
                , description = "ManagedLedger offload bucket, s3 and google-cloud-storage requires this parameter")
        private String bucket;

        @Parameter(names = {"-e", "--endpoint"}
                , description = "ManagedLedger offload service endpoint, only s3 requires this parameter")
        private String endpoint;

        @Parameter(names = {"-i", "--aws-id"}
                , description = "AWS Credential Id to use when using driver S3 or aws-s3")
        private String awsId;

        @Parameter(names = {"-s", "--aws-secret"}
                , description = "AWS Credential Secret to use when using driver S3 or aws-s3")
        private String awsSecret;

        @Parameter(names = {"--ro", "--s3-role"}
                , description = "S3 Role used for STSAssumeRoleSessionCredentialsProvider")
        private String s3Role;

        @Parameter(names = {"--s3-role-session-name", "-rsn"}
                , description = "S3 role session name used for STSAssumeRoleSessionCredentialsProvider")
        private String s3RoleSessionName;

        @Parameter(names = {"-m", "--maxBlockSizeInBytes"},
                description = "ManagedLedger offload max block Size in bytes,"
                        + "s3 and google-cloud-storage requires this parameter")
        private int maxBlockSizeInBytes;

        @Parameter(names = {"-rb", "--readBufferSizeInBytes"},
                description = "ManagedLedger offload read buffer size in bytes,"
                        + "s3 and google-cloud-storage requires this parameter")
        private int readBufferSizeInBytes;

        @Parameter(names = {"-t", "--offloadThresholdInBytes"}
                , description = "ManagedLedger offload threshold in bytes", required = true)
        private long offloadThresholdInBytes;

        @Parameter(names = {"-dl", "--offloadDeletionLagInMillis"}
                , description = "ManagedLedger offload deletion lag in bytes")
        private Long offloadDeletionLagInMillis;

        @Parameter(
                names = {"--offloadedReadPriority", "-orp"},
                description = "Read priority for offloaded messages. By default, once messages are offloaded to"
                        + " long-term storage, brokers read messages from long-term storage, but messages can still"
                        + " exist in BookKeeper for a period depends on your configuration. For messages that exist"
                        + " in both long-term storage and BookKeeper, you can set where to read messages from with"
                        + " the option `tiered-storage-first` or `bookkeeper-first`.",
                required = false
        )
        private String offloadReadPriorityStr;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            OffloadedReadPriority offloadedReadPriority = OffloadPoliciesImpl.DEFAULT_OFFLOADED_READ_PRIORITY;

            if (this.offloadReadPriorityStr != null) {
                try {
                    offloadedReadPriority = OffloadedReadPriority.fromString(this.offloadReadPriorityStr);
                } catch (Exception e) {
                    throw new ParameterException("--offloadedReadPriority parameter must be one of "
                            + Arrays.stream(OffloadedReadPriority.values())
                            .map(OffloadedReadPriority::toString)
                            .collect(Collectors.joining(","))
                            + " but got: " + this.offloadReadPriorityStr, e);
                }
            }

            OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.create(driver, region, bucket, endpoint,
                    s3Role, s3RoleSessionName,
                    awsId, awsSecret,
                    maxBlockSizeInBytes,
                    readBufferSizeInBytes, offloadThresholdInBytes, offloadDeletionLagInMillis, offloadedReadPriority);

            getTopicPolicies(isGlobal).setOffloadPolicies(persistentTopic, offloadPolicies);
        }
    }


    @Parameters(commandDescription = "Remove schema compatibility strategy on a topic")
    private class RemoveSchemaCompatibilityStrategy extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getAdmin().topicPolicies().removeSchemaCompatibilityStrategy(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Set schema compatibility strategy on a topic")
    private class SetSchemaCompatibilityStrategy extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--strategy", "-s"}, description = "Schema compatibility strategy: [UNDEFINED, "
                + "ALWAYS_INCOMPATIBLE, ALWAYS_COMPATIBLE, BACKWARD, FORWARD, FULL, BACKWARD_TRANSITIVE, "
                + "FORWARD_TRANSITIVE, FULL_TRANSITIVE]", required = true)
        private SchemaCompatibilityStrategy strategy;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getAdmin().topicPolicies().setSchemaCompatibilityStrategy(persistentTopic, strategy);
        }
    }

    @Parameters(commandDescription = "Get schema compatibility strategy on a topic")
    private class GetSchemaCompatibilityStrategy extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-ap", "--applied"}, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            SchemaCompatibilityStrategy strategy =
                    getAdmin().topicPolicies().getSchemaCompatibilityStrategy(persistentTopic, applied);
            print(strategy == null ? "null" : strategy.name());
        }
    }

    private TopicPolicies getTopicPolicies(boolean isGlobal) {
        return getAdmin().topicPolicies(isGlobal);
    }

}
