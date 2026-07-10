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

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.ScalableTopics;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(description = "Operations on scalable topics")
public class CmdScalableTopics extends CmdBase {

    private ScalableTopics scalableTopics() {
        return getAdmin().scalableTopics();
    }

    @Command(description = "Get the list of scalable topics under a namespace, optionally"
            + " filtered to those whose properties contain every given key=value pair")
    private class ListCmd extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespace;

        @Option(names = {"-p", "--property"},
                description = "Filter to topics whose properties contain this key=value pair."
                        + " Repeat to AND multiple filters together.",
                arity = "0..*")
        private List<String> properties;

        @Override
        void run() throws Exception {
            String ns = validateNamespace(namespace);
            Map<String, String> filters = parseListKeyValueMap(properties);
            if (filters == null || filters.isEmpty()) {
                print(scalableTopics().listScalableTopics(ns));
            } else {
                print(scalableTopics().listScalableTopicsByProperties(ns, filters));
            }
        }
    }

    @Command(description = "Create a new scalable topic")
    private class CreateCmd extends CliCommand {
        @Parameters(description = "tenant/namespace/topic", arity = "1")
        private String topic;

        @Option(names = {"-s", "--segments"},
                description = "Number of initial segments", defaultValue = "1")
        private int numInitialSegments;

        @Option(names = {"-p", "--property"}, description = "Key-value properties (key=value)",
                arity = "0..*")
        private List<String> properties;

        @Override
        void run() throws Exception {
            Map<String, String> props = parseListKeyValueMap(properties);
            if (props != null) {
                scalableTopics().createScalableTopic(topic, numInitialSegments, props);
            } else {
                scalableTopics().createScalableTopic(topic, numInitialSegments);
            }
            print("Created scalable topic " + topic + " with " + numInitialSegments + " segment(s)");
        }
    }

    @Command(description = "Migrate an existing regular (partitioned or non-partitioned) topic"
            + " to a scalable topic. Fails if legacy v4 clients are still connected unless"
            + " --force is set.")
    private class MigrateCmd extends CliCommand {
        @Parameters(description = "tenant/namespace/topic", arity = "1")
        private String topic;

        @Option(names = {"-f", "--force"},
                description = "Migrate even if legacy v4 clients are still connected")
        private boolean force;

        @Override
        void run() throws Exception {
            scalableTopics().migrateToScalable(topic, force);
            print("Migrated topic " + topic + " to a scalable topic");
        }
    }

    @Command(description = "Get scalable topic metadata")
    private class GetMetadataCmd extends CliCommand {
        @Parameters(description = "tenant/namespace/topic", arity = "1")
        private String topic;

        @Override
        void run() throws Exception {
            prettyPrint(scalableTopics().getMetadata(topic));
        }
    }

    @Command(description = "Get aggregated stats for a scalable topic")
    private class GetStatsCmd extends CliCommand {
        @Parameters(description = "tenant/namespace/topic", arity = "1")
        private String topic;

        @Override
        void run() throws Exception {
            prettyPrint(scalableTopics().getStats(topic));
        }
    }

    @Command(description = "Delete a scalable topic and all its segments")
    private class DeleteCmd extends CliCommand {
        @Parameters(description = "tenant/namespace/topic", arity = "1")
        private String topic;

        @Option(names = {"-f", "--force"},
                description = "Force deletion even if topic has active subscriptions")
        private boolean force;

        @Override
        void run() throws Exception {
            scalableTopics().deleteScalableTopic(topic, force);
            print("Deleted scalable topic " + topic);
        }
    }

    @Command(description = "Split a segment into two halves")
    private class SplitSegmentCmd extends CliCommand {
        @Parameters(description = "tenant/namespace/topic", arity = "1")
        private String topic;

        @Option(names = {"-s", "--segment-id"},
                description = "ID of the segment to split", required = true)
        private long segmentId;

        @Override
        void run() throws Exception {
            scalableTopics().splitSegment(topic, segmentId);
            print("Split segment " + segmentId + " of topic " + topic);
        }
    }

    @Command(description = "Reset a subscription's cursor on every segment to a given"
            + " point in time. Pass --time as a relative offset (e.g. 1h, 5d) — the cursor"
            + " is reset to (now - offset).")
    private class SeekSubscriptionCmd extends CliCommand {
        @Parameters(description = "tenant/namespace/topic", arity = "1")
        private String topic;

        @Option(names = {"-s", "--subscription"},
                description = "Subscription name", required = true)
        private String subscription;

        @Option(names = {"-t", "--time"},
                description = "Relative offset in the past to seek to (e.g. 1h, 5d, 30m)",
                required = true,
                converter = org.apache.pulsar.cli.converters.picocli.TimeUnitToMillisConverter.class)
        private Long offsetMillis;

        @Override
        void run() throws Exception {
            long target = System.currentTimeMillis() - offsetMillis;
            scalableTopics().seekSubscription(topic, subscription, target);
            print("Reset subscription " + subscription + " on topic " + topic
                    + " to timestamp " + target + " (" + offsetMillis + "ms ago)");
        }
    }

    @Command(description = "Skip every undelivered message on the subscription, across every"
            + " segment in the DAG.")
    private class ClearBacklogCmd extends CliCommand {
        @Parameters(description = "tenant/namespace/topic", arity = "1")
        private String topic;

        @Option(names = {"-s", "--subscription"},
                description = "Subscription name", required = true)
        private String subscription;

        @Override
        void run() throws Exception {
            scalableTopics().clearBacklog(topic, subscription);
            print("Cleared backlog of subscription " + subscription + " on topic " + topic);
        }
    }

    @Command(description = "Merge two adjacent segments into one")
    private class MergeSegmentsCmd extends CliCommand {
        @Parameters(description = "tenant/namespace/topic", arity = "1")
        private String topic;

        @Option(names = {"--segment-id-1"},
                description = "First segment ID to merge", required = true)
        private long segmentId1;

        @Option(names = {"--segment-id-2"},
                description = "Second segment ID to merge", required = true)
        private long segmentId2;

        @Override
        void run() throws Exception {
            scalableTopics().mergeSegments(topic, segmentId1, segmentId2);
            print("Merged segments " + segmentId1 + " and " + segmentId2 + " of topic " + topic);
        }
    }

    public CmdScalableTopics(Supplier<PulsarAdmin> admin) {
        super("scalable-topics", admin);
        addCommand("list", new ListCmd());
        addCommand("create", new CreateCmd());
        addCommand("migrate", new MigrateCmd());
        addCommand("get-metadata", new GetMetadataCmd());
        addCommand("stats", new GetStatsCmd());
        addCommand("delete", new DeleteCmd());
        addCommand("split-segment", new SplitSegmentCmd());
        addCommand("merge-segments", new MergeSegmentsCmd());
        addCommand("seek", new SeekSubscriptionCmd());
        addCommand("clear-backlog", new ClearBacklogCmd());
    }
}
