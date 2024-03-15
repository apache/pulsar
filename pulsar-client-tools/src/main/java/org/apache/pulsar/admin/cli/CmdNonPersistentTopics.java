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
package org.apache.pulsar.admin.cli;

import java.util.function.Supplier;
import org.apache.pulsar.client.admin.NonPersistentTopics;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@SuppressWarnings("deprecation")
@Command(description = "Operations on non-persistent topics", hidden = true)
public class CmdNonPersistentTopics extends CmdBase {
    private NonPersistentTopics nonPersistentTopics;

    public CmdNonPersistentTopics(Supplier<PulsarAdmin> admin) {
        super("non-persistent", admin);

        addCommand("create-partitioned-topic", new CreatePartitionedCmd());
        addCommand("lookup", new Lookup());
        addCommand("stats", new GetStats());
        addCommand("stats-internal", new GetInternalStats());
        addCommand("get-partitioned-topic-metadata", new GetPartitionedTopicMetadataCmd());
        addCommand("list", new GetList());
        addCommand("list-in-bundle", new GetListInBundle());
    }

    private NonPersistentTopics getNonPersistentTopics() {
        if (nonPersistentTopics == null) {
            nonPersistentTopics = getAdmin().nonPersistentTopics();
        }
        return nonPersistentTopics;
    }

    @Command(description = "Lookup a topic from the current serving broker")
    private class Lookup extends CliCommand {
        @Parameters(description = "non-persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            print(getAdmin().lookups().lookupTopic(topic));
        }
    }

    @Command(description = "Get the stats for the topic and its connected producers and consumers. "
            + "All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.")
    private class GetStats extends CliCommand {
        @Parameters(description = "non-persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validateNonPersistentTopic(topicName);
            print(getNonPersistentTopics().getStats(persistentTopic));
        }
    }

    @Command(description = "Get the internal stats for the topic")
    private class GetInternalStats extends CliCommand {
        @Parameters(description = "non-persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validateNonPersistentTopic(topicName);
            print(getNonPersistentTopics().getInternalStats(persistentTopic));
        }
    }

    @Command(description = "Create a partitioned topic. "
            + "The partitioned topic has to be created before creating a producer on it.")
    private class CreatePartitionedCmd extends CliCommand {

        @Parameters(description = "non-persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-p",
                "--partitions" }, description = "Number of partitions for the topic", required = true)
        private int numPartitions;

        @Override
        void run() throws Exception {
            String persistentTopic = validateNonPersistentTopic(topicName);
            getNonPersistentTopics().createPartitionedTopic(persistentTopic, numPartitions);
        }
    }

    @Command(description = "Get the partitioned topic metadata. "
            + "If the topic is not created or is a non-partitioned topic, it returns empty topic with 0 partitions")
    private class GetPartitionedTopicMetadataCmd extends CliCommand {

        @Parameters(description = "non-persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws Exception {
            String nonPersistentTopic = validateNonPersistentTopic(topicName);
            print(getNonPersistentTopics().getPartitionedTopicMetadata(nonPersistentTopic));
        }
    }

    @Command(description = "Get list of non-persistent topics present under a namespace")
    private class GetList extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespace;

        @Override
        void run() throws PulsarAdminException {
            print(getNonPersistentTopics().getList(namespace));
        }
    }

    @Command(description = "Get list of non-persistent topics present under a namespace bundle")
    private class GetListInBundle extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespace;

        @Option(names = { "-b",
                "--bundle" }, description = "bundle range", required = true)
        private String bundleRange;

        @Override
        void run() throws PulsarAdminException {
            print(getNonPersistentTopics().getListInBundle(namespace, bundleRange));
        }
    }
}
