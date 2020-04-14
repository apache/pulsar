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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.CommaParameterSplitter;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.util.RelativeTimeUtil;

@Parameters(commandDescription = "Operations on persistent topics")
public class CmdTopics extends CmdBase {
    private final Topics topics;

    public CmdTopics(PulsarAdmin admin) {
        super("topics", admin);
        topics = admin.topics();

        jcommander.addCommand("list", new ListCmd());
        jcommander.addCommand("list-partitioned-topics", new PartitionedTopicListCmd());
        jcommander.addCommand("permissions", new Permissions());
        jcommander.addCommand("grant-permission", new GrantPermissions());
        jcommander.addCommand("revoke-permission", new RevokePermissions());
        jcommander.addCommand("lookup", new Lookup());
        jcommander.addCommand("bundle-range", new GetBundleRange());
        jcommander.addCommand("delete", new DeleteCmd());
        jcommander.addCommand("unload", new UnloadCmd());
        jcommander.addCommand("subscriptions", new ListSubscriptions());
        jcommander.addCommand("unsubscribe", new DeleteSubscription());
        jcommander.addCommand("create-subscription", new CreateSubscription());

        jcommander.addCommand("stats", new GetStats());
        jcommander.addCommand("stats-internal", new GetInternalStats());
        jcommander.addCommand("info-internal", new GetInternalInfo());

        jcommander.addCommand("partitioned-stats", new GetPartitionedStats());
        jcommander.addCommand("partitioned-stats-internal", new GetPartitionedStatsInternal());

        jcommander.addCommand("skip", new Skip());
        jcommander.addCommand("clear-backlog", new ClearBacklog());

        jcommander.addCommand("expire-messages", new ExpireMessages());
        jcommander.addCommand("expire-messages-all-subscriptions", new ExpireMessagesForAllSubscriptions());

        jcommander.addCommand("create-partitioned-topic", new CreatePartitionedCmd());
        jcommander.addCommand("create-missed-partitions", new CreateMissedPartitionsCmd());
        jcommander.addCommand("create", new CreateNonPartitionedCmd());
        jcommander.addCommand("update-partitioned-topic", new UpdatePartitionedCmd());
        jcommander.addCommand("get-partitioned-topic-metadata", new GetPartitionedTopicMetadataCmd());

        jcommander.addCommand("delete-partitioned-topic", new DeletePartitionedCmd());
        jcommander.addCommand("peek-messages", new PeekMessages());
        jcommander.addCommand("reset-cursor", new ResetCursor());
        jcommander.addCommand("terminate", new Terminate());
        jcommander.addCommand("compact", new Compact());
        jcommander.addCommand("compaction-status", new CompactionStatusCmd());
        jcommander.addCommand("offload", new Offload());
        jcommander.addCommand("offload-status", new OffloadStatusCmd());
        jcommander.addCommand("last-message-id", new GetLastMessageId());
    }

    @Parameters(commandDescription = "Get the list of topics under a namespace.")
    private class ListCmd extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(topics.getList(namespace));
        }
    }

    @Parameters(commandDescription = "Get the list of partitioned topics under a namespace.")
    private class PartitionedTopicListCmd extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(topics.getPartitionedTopicList(namespace));
        }
    }

    @Parameters(commandDescription = "Grant a new permission to a client role on a single topic.")
    private class GrantPermissions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--role", description = "Client role to which grant permissions", required = true)
        private String role;

        @Parameter(names = "--actions", description = "Actions to be granted (produce,consume)", required = true, splitter = CommaParameterSplitter.class)
        private List<String> actions;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            topics.grantPermission(topic, role, getAuthActions(actions));
        }
    }

    @Parameters(commandDescription = "Revoke permissions on a topic \n "
            + "\t\t\t   Revoke permissions to a client role on a single topic. If the permission \n"
            + "\t\t\t   was not set at the topic level, but rather at the namespace level, this \n"
            + "\t\t\t   operation will return an error (HTTP status code 412).")
    private class RevokePermissions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--role", description = "Client role to which revoke permissions", required = true)
        private String role;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            topics.revokePermissions(topic, role);
        }
    }

    @Parameters(commandDescription = "Get the permissions on a topic\n"
            + "\t\t     Retrieve the effective permissions for a topic. These permissions are defined \n"
            + "\t\t     by the permissions set at the namespace level combined (union) with any eventual \n"
            + "\t\t     specific permission set on the topic.")
    private class Permissions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            print(topics.getPermissions(topic));
        }
    }

    @Parameters(commandDescription = "Lookup a topic from the current serving broker")
    private class Lookup extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            print(admin.lookups().lookupTopic(topic));
        }
    }

    @Parameters(commandDescription = "Get Namespace bundle range of a topic")
    private class GetBundleRange extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            print(admin.lookups().getBundleRange(topic));
        }
    }

    @Parameters(commandDescription = "Create a partitioned topic. \n"
            + "\t\tThe partitioned topic has to be created before creating a producer on it.")
    private class CreatePartitionedCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-p",
                "--partitions" }, description = "Number of partitions for the topic", required = true)
        private int numPartitions;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            topics.createPartitionedTopic(topic, numPartitions);
        }
    }

    @Parameters(commandDescription = "Try to create partitions for partitioned topic. \n"
            + "\t\t     The partitions of partition topic has to be created, can be used by repair partitions when \n"
            + "\t\t     topic auto creation is disabled")
    private class CreateMissedPartitionsCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            topics.createMissedPartitions(topic);
        }
    }

    @Parameters(commandDescription = "Create a non-partitioned topic.")
    private class CreateNonPartitionedCmd extends CliCommand {
    	
    	@Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
    	private java.util.List<String> params;
    	
    	@Override
    	void run() throws Exception {
    		String topic = validateTopicName(params);
    		topics.createNonPartitionedTopic(topic);
    	}
    }
    
    @Parameters(commandDescription = "Update existing non-global partitioned topic. \n"
            + "\t\tNew updating number of partitions must be greater than existing number of partitions.")
    private class UpdatePartitionedCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-p",
                "--partitions" }, description = "Number of partitions for the topic", required = true)
        private int numPartitions;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            topics.updatePartitionedTopic(topic, numPartitions);
        }
    }

    @Parameters(commandDescription = "Get the partitioned topic metadata. \n"
            + "\t\tIf the topic is not created or is a non-partitioned topic, it returns empty topic with 0 partitions")
    private class GetPartitionedTopicMetadataCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            print(topics.getPartitionedTopicMetadata(topic));
        }
    }

    @Parameters(commandDescription = "Delete a partitioned topic. \n"
            + "\t\tIt will also delete all the partitions of the topic if it exists.")
    private class DeletePartitionedCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-f",
                "--force" }, description = "Close all producer/consumer/replicator and delete topic forcefully")
        private boolean force = false;

        @Parameter(names = { "-d",
                "--deleteSchema" }, description = "Delete schema while deleting topic")
        private boolean deleteSchema = false;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            topics.deletePartitionedTopic(topic, force);
            if (deleteSchema) {
                admin.schemas().deleteSchema(topic);
            }
        }
    }

    @Parameters(commandDescription = "Delete a topic. \n"
            + "\t\tThe topic cannot be deleted if there's any active subscription or producers connected to it.")
    private class DeleteCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-f",
                "--force" }, description = "Close all producer/consumer/replicator and delete topic forcefully")
        private boolean force = false;

        @Parameter(names = { "-d",
                "--deleteSchema" }, description = "Delete schema while deleting topic")
        private boolean deleteSchema = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            topics.delete(topic, force);
            if (deleteSchema) {
                admin.schemas().deleteSchema(topic);
            }
        }
    }

    @Parameters(commandDescription = "Unload a topic. \n")
    private class UnloadCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            topics.unload(topic);
        }
    }

    @Parameters(commandDescription = "Get the list of subscriptions on the topic")
    private class ListSubscriptions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            print(topics.getSubscriptions(topic));
        }
    }

    @Parameters(commandDescription = "Delete a durable subscriber from a topic. \n"
            + "\t\tThe subscription cannot be deleted if there are any active consumers attached to it \n")
    private class DeleteSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-f",
            "--force" }, description = "Disconnect and close all consumers and delete subscription forcefully")
        private boolean force = false;

        @Parameter(names = { "-s", "--subscription" }, description = "Subscription to be deleted", required = true)
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            topics.deleteSubscription(topic, subName, force);
        }
    }

    @Parameters(commandDescription = "Get the stats for the topic and its connected producers and consumers. \n"
            + "\t       All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.")
    private class GetStats extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-gpb",
            "--get-precise-backlog" }, description = "Set true to get precise backlog")
        private boolean getPreciseBacklog = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            print(topics.getStats(topic, getPreciseBacklog));
        }
    }

    @Parameters(commandDescription = "Get the internal stats for the topic")
    private class GetInternalStats extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            print(topics.getInternalStats(topic));
        }
    }

    @Parameters(commandDescription = "Get the internal metadata info for the topic")
    private class GetInternalInfo extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            JsonObject result = topics.getInternalInfo(topic);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(result));
        }
    }

    @Parameters(commandDescription = "Get the stats for the partitioned topic and its connected producers and consumers. \n"
            + "\t       All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.")
    private class GetPartitionedStats extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--per-partition", description = "Get per partition stats")
        private boolean perPartition = false;

        @Parameter(names = { "-gpb",
            "--get-precise-backlog" }, description = "Set true to get precise backlog")
        private boolean getPreciseBacklog = false;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            print(topics.getPartitionedStats(topic, perPartition, getPreciseBacklog));
        }
    }

    @Parameters(commandDescription = "Get the internal stats for the partitioned topic and its connected producers and consumers. \n"
            + "\t       All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.")
    private class GetPartitionedStatsInternal extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            print(topics.getPartitionedInternalStats(topic));
        }
    }

    @Parameters(commandDescription = "Skip all the messages for the subscription")
    private class ClearBacklog extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s", "--subscription" }, description = "Subscription to be cleared", required = true)
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            topics.skipAllMessages(topic, subName);
        }
    }

    @Parameters(commandDescription = "Skip some messages for the subscription")
    private class Skip extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to be skip messages on", required = true)
        private String subName;

        @Parameter(names = { "-n", "--count" }, description = "Number of messages to skip", required = true)
        private long numMessages;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            topics.skipMessages(topic, subName, numMessages);
        }
    }

    @Parameters(commandDescription = "Expire messages that older than given expiry time (in seconds) for the subscription")
    private class ExpireMessages extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to be skip messages on", required = true)
        private String subName;

        @Parameter(names = { "-t", "--expireTime" }, description = "Expire messages older than time in seconds", required = true)
        private long expireTimeInSeconds;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            topics.expireMessages(topic, subName, expireTimeInSeconds);
        }
    }

    @Parameters(commandDescription = "Expire messages that older than given expiry time (in seconds) for all subscriptions")
    private class ExpireMessagesForAllSubscriptions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-t", "--expireTime" }, description = "Expire messages older than time in seconds", required = true)
        private long expireTimeInSeconds;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            topics.expireMessagesForAllSubscriptions(topic, expireTimeInSeconds);
        }
    }

    @Parameters(commandDescription = "Create a new subscription on a topic")
    private class CreateSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to reset position on", required = true)
        private String subscriptionName;

        @Parameter(names = { "--messageId",
                "-m" }, description = "messageId where to create the subscription. It can be either 'latest', 'earliest' or (ledgerId:entryId)", required = false)
        private String messageIdStr = "latest";

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            MessageId messageId;
            if (messageIdStr.equals("latest")) {
                messageId = MessageId.latest;
            } else if (messageIdStr.equals("earliest")) {
                messageId = MessageId.earliest;
            } else {
                messageId = validateMessageIdString(messageIdStr);
            }

            topics.createSubscription(topic, subscriptionName, messageId);
        }
    }

    @Parameters(commandDescription = "Reset position for subscription to a position that is closest to timestamp or messageId.")
    private class ResetCursor extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to reset position on", required = true)
        private String subName;

        @Parameter(names = { "--time",
                "-t" }, description = "time in minutes to reset back to (or minutes, hours,days,weeks eg: 100m, 3h, 2d, 5w)", required = false)
        private String resetTimeStr;

        @Parameter(names = { "--messageId",
                "-m" }, description = "messageId to reset back to (ledgerId:entryId)", required = false)
        private String resetMessageIdStr;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            if (isNotBlank(resetMessageIdStr)) {
                MessageId messageId = validateMessageIdString(resetMessageIdStr);
                topics.resetCursor(persistentTopic, subName, messageId);
            } else if (isNotBlank(resetTimeStr)) {
                long resetTimeInMillis = TimeUnit.SECONDS
                        .toMillis(RelativeTimeUtil.parseRelativeTimeInSeconds(resetTimeStr));
                // now - go back time
                long timestamp = System.currentTimeMillis() - resetTimeInMillis;
                topics.resetCursor(persistentTopic, subName, timestamp);
            } else {
                throw new PulsarAdminException(
                        "Either Timestamp (--time) or Position (--position) has to be provided to reset cursor");
            }
        }
    }

    @Parameters(commandDescription = "Terminate a topic and don't allow any more messages to be published")
    private class Terminate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            try {
                MessageId lastMessageId = topics.terminateTopicAsync(persistentTopic).get();
                System.out.println("Topic succesfully terminated at " + lastMessageId);
            } catch (InterruptedException | ExecutionException e) {
                throw new PulsarAdminException(e);
            }
        }
    }

    @Parameters(commandDescription = "Peek some messages for the subscription")
    private class PeekMessages extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to get messages from", required = true)
        private String subName;

        @Parameter(names = { "-n", "--count" }, description = "Number of messages (default 1)", required = false)
        private int numMessages = 1;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            List<Message<byte[]>> messages = topics.peekMessages(persistentTopic, subName, numMessages);
            int position = 0;
            for (Message<byte[]> msg : messages) {
                if (++position != 1) {
                    System.out.println("-------------------------------------------------------------------------\n");
                }
                if (msg.getMessageId() instanceof BatchMessageIdImpl) {
                    BatchMessageIdImpl msgId = (BatchMessageIdImpl) msg.getMessageId();
                    System.out.println("Batch Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId() + ":" + msgId.getBatchIndex());
                } else {
                    MessageIdImpl msgId = (MessageIdImpl) msg.getMessageId();
                    System.out.println("Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId());
                }
                if (msg.getProperties().size() > 0) {
                    System.out.println("Tenants:");
                    print(msg.getProperties());
                }
                ByteBuf data = Unpooled.wrappedBuffer(msg.getData());
                System.out.println(ByteBufUtil.prettyHexDump(data));
            }
        }
    }

    @Parameters(commandDescription = "Compact a topic")
    private class Compact extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            topics.triggerCompaction(persistentTopic);
            System.out.println("Topic compaction requested for " + persistentTopic);
        }
    }

    @Parameters(commandDescription = "Status of compaction on a topic")
    private class CompactionStatusCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-w", "--wait-complete" },
                   description = "Wait for compaction to complete", required = false)
        private boolean wait = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            try {
                LongRunningProcessStatus status = topics.compactionStatus(persistentTopic);
                while (wait && status.status == LongRunningProcessStatus.Status.RUNNING) {
                    Thread.sleep(1000);
                    status = topics.compactionStatus(persistentTopic);
                }

                switch (status.status) {
                case NOT_RUN:
                    System.out.println("Compaction has not been run for " + persistentTopic
                                       + " since broker startup");
                    break;
                case RUNNING:
                    System.out.println("Compaction is currently running");
                    break;
                case SUCCESS:
                    System.out.println("Compaction was a success");
                    break;
                case ERROR:
                    System.out.println("Error in compaction");
                    throw new PulsarAdminException("Error compacting: " + status.lastError);
                }
            } catch (InterruptedException e) {
                throw new PulsarAdminException(e);
            }
        }
    }

    static MessageId findFirstLedgerWithinThreshold(List<PersistentTopicInternalStats.LedgerInfo> ledgers,
                                                    long sizeThreshold) {
        long suffixSize = 0L;

        ledgers = Lists.reverse(ledgers);
        long previousLedger = ledgers.get(0).ledgerId;
        for (PersistentTopicInternalStats.LedgerInfo l : ledgers) {
            suffixSize += l.size;
            if (suffixSize > sizeThreshold) {
                return new MessageIdImpl(previousLedger, 0L, -1);
            }
            previousLedger = l.ledgerId;
        }
        return null;
    }

    @Parameters(commandDescription = "Trigger offload of data from a topic to long-term storage (e.g. Amazon S3)")
    private class Offload extends CliCommand {
        @Parameter(names = { "-s", "--size-threshold" },
                   description = "Maximum amount of data to keep in BookKeeper for the specified topic (e.g. 10M, 5G).",
                   required = true)
        private String sizeThresholdStr;

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            long sizeThreshold = validateSizeString(sizeThresholdStr);
            String persistentTopic = validatePersistentTopic(params);

            PersistentTopicInternalStats stats = topics.getInternalStats(persistentTopic);
            if (stats.ledgers.size() < 1) {
                throw new PulsarAdminException("Topic doesn't have any data");
            }

            LinkedList<PersistentTopicInternalStats.LedgerInfo> ledgers = new LinkedList(stats.ledgers);
            ledgers.get(ledgers.size()-1).size = stats.currentLedgerSize; // doesn't get filled in now it seems
            MessageId messageId = findFirstLedgerWithinThreshold(ledgers, sizeThreshold);

            if (messageId == null) {
                System.out.println("Nothing to offload");
                return;
            }

            topics.triggerOffload(persistentTopic, messageId);
            System.out.println("Offload triggered for " + persistentTopic + " for messages before " + messageId);
        }
    }

    @Parameters(commandDescription = "Check the status of data offloading from a topic to long-term storage")
    private class OffloadStatusCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-w", "--wait-complete" },
                   description = "Wait for offloading to complete", required = false)
        private boolean wait = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            try {
                LongRunningProcessStatus status = topics.offloadStatus(persistentTopic);
                while (wait && status.status == LongRunningProcessStatus.Status.RUNNING) {
                    Thread.sleep(1000);
                    status = topics.offloadStatus(persistentTopic);
                }

                switch (status.status) {
                case NOT_RUN:
                    System.out.println("Offload has not been run for " + persistentTopic
                                       + " since broker startup");
                    break;
                case RUNNING:
                    System.out.println("Offload is currently running");
                    break;
                case SUCCESS:
                    System.out.println("Offload was a success");
                    break;
                case ERROR:
                    System.out.println("Error in offload");
                    throw new PulsarAdminException("Error offloading: " + status.lastError);
                }
            } catch (InterruptedException e) {
                throw new PulsarAdminException(e);
            }
        }
    }

    @Parameters(commandDescription = "get the last commit message id of topic")
    private class GetLastMessageId extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(topics.getLastMessageId(persistentTopic));
        }
    }
}
