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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.util.RelativeTimeUtil;

@Parameters(commandDescription = "Operations on persistent topics")
public class CmdTopicPolicies extends CmdBase {

    public CmdTopicPolicies(Supplier<PulsarAdmin> admin) {
        super("topicPolicies", admin);

        jcommander.addCommand("get-message-ttl", new GetMessageTTL());
        jcommander.addCommand("set-message-ttl", new SetMessageTTL());
        jcommander.addCommand("remove-message-ttl", new RemoveMessageTTL());
        jcommander.addCommand("set-subscription-types-enabled", new SetSubscriptionTypesEnabled());
        jcommander.addCommand("get-subscription-types-enabled", new GetSubscriptionTypesEnabled());
        jcommander.addCommand("remove-subscription-types-enabled", new RemoveSubscriptionTypesEnabled());
        jcommander.addCommand("get-retention", new GetRetention());
        jcommander.addCommand("set-retention", new SetRetention());
        jcommander.addCommand("remove-retention", new RemoveRetention());
        jcommander.addCommand("get-backlog-quota", new GetBacklogQuotaMap());
        jcommander.addCommand("set-backlog-quota", new SetBacklogQuota());
        jcommander.addCommand("remove-backlog-quota", new RemoveBacklogQuota());

        jcommander.addCommand("set-deduplication", new SetDeduplicationStatus());
        jcommander.addCommand("get-deduplication", new GetDeduplicationStatus());
        jcommander.addCommand("remove-deduplication", new RemoveDeduplicationStatus());

        jcommander.addCommand("get-persistence", new GetPersistence());
        jcommander.addCommand("set-persistence", new SetPersistence());
        jcommander.addCommand("remove-persistence", new RemovePersistence());
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

        @Parameter(names = { "-t", "--ttl" }, description = "Message TTL for topic in second, allowed range from 1 to Integer.MAX_VALUE", required = true)
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

        @Parameter(names = { "-lt", "--limitTime" }, description = "Time limit in second, non-positive number for disabling time limit.")
        private int limitTime = -1;

        @Parameter(names = { "-p", "--policy" }, description = "Retention policy to enforce when the limit is reached. "
                + "Valid options are: [producer_request_hold, producer_exception, consumer_backlog_eviction]", required = true)
        private String policyStr;

        @Parameter(names = {"-t", "--type"}, description = "Backlog quota type to set. Valid options are: " +
                "destination_storage and message_age. " +
                "destination_storage limits backlog by size (in bytes). " +
                "message_age limits backlog by time, that is, message timestamp (broker or publish timestamp). " +
                "You can set size or time to control the backlog, or combine them together to control the backlog. ")
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
            getTopicPolicies(isGlobal).removeBacklogQuota(persistentTopic, BacklogQuota.BacklogQuotaType.valueOf(backlogQuotaType));
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
                "--bookkeeper-ensemble" }, description = "Number of bookies to use for a topic", required = true)
        private int bookkeeperEnsemble;

        @Parameter(names = { "-w",
                "--bookkeeper-write-quorum" }, description = "How many writes to make of each entry", required = true)
        private int bookkeeperWriteQuorum;

        @Parameter(names = { "-a",
                "--bookkeeper-ack-quorum" }, description = "Number of acks (guaranteed copies) to wait for each entry", required = true)
        private int bookkeeperAckQuorum;

        @Parameter(names = { "-r",
                "--ml-mark-delete-max-rate" }, description = "Throttling rate of mark-delete operation (0 means no throttle)", required = true)
        private double managedLedgerMaxMarkDeleteRate;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously", arity = 0)
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
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

    private TopicPolicies getTopicPolicies(boolean isGlobal) {
        return getAdmin().topicPolicies(isGlobal);
    }

}
