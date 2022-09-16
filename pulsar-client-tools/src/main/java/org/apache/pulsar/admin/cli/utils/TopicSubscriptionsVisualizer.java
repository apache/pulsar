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
package org.apache.pulsar.admin.cli.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Generate a static html file showing subscriptions cursor positions for a given topic.
 * It helps analyze the number of entries needed by a subscription to be up-to-date.
 * The visualization helps to understand which subscriptions are slows compared to the other ones for the same topic.
 */
public class TopicSubscriptionsVisualizer {

    static class EntryWidthMapping {
        final Map<Long, Integer> map = new HashMap<>();

        Integer put(long entry, int width) {
            return map.put(entry, width);
        }
    }

    static class LedgerSegmentProgress {
        final Map<Long, EntryWidthMapping> ledgersMapping = new HashMap<>();
        int currentWidth = 0;
    }

    static class EntriesStatsByStatus {
        long totalAcked;
        long totalAhead;
        long totalIndividuallyDeleted;
    }


    enum EntryStatus {
        ACKED,
        INDIVIDUALLLY_DELETED,
        AHEAD
    }

    @AllArgsConstructor
    private static class Position {
        public long ledgerId;
        public long entryId;

        static Position fromString(String str) {
            final String[] split = str.split(":");
            return new Position(Long.parseLong(split[0]), Long.parseLong(split[1]));
        }
    }

    public static String createHtml(String topic, ManagedLedgerInternalStats persistentTopicInternalStats,
                             TopicStats persistentTopicStats) throws Exception {
        return new TopicSubscriptionsVisualizer().internalCreateHtml(topic, persistentTopicInternalStats,
                persistentTopicStats);
    }

    private TopicSubscriptionsVisualizer() {
    }

    private String internalCreateHtml(String topic, ManagedLedgerInternalStats persistentTopicInternalStats,
                             TopicStats persistentTopicStats) throws Exception {
        StringBuilder builder = new StringBuilder();

        startBody(builder);
        final Position lastConfirmedEntry = Position.fromString(persistentTopicInternalStats.lastConfirmedEntry);
        long totalEntries = 0;
        for (ManagedLedgerInternalStats.LedgerInfo ledger : persistentTopicInternalStats.ledgers) {
            if (ledger.entries == 0 && ledger.ledgerId == lastConfirmedEntry.ledgerId) {
                ledger.entries = lastConfirmedEntry.entryId + 1;
            }
            totalEntries += ledger.entries;
        }
        generateTopicLine(topic, persistentTopicInternalStats, builder, totalEntries);
        generateSubscriptionLines(persistentTopicInternalStats, persistentTopicStats, builder, totalEntries);
        endBody(builder);
        return builder.toString();
    }

    private static void endBody(StringBuilder builder) {
        builder.append("</body></html>");
    }

    private static void startBody(StringBuilder builder) throws IOException {
        builder.append("<html><head><style>");
        try (final InputStream css = TopicSubscriptionsVisualizer.class.getResourceAsStream("topic-visualizer.css")) {
            builder.append(IOUtils.toString(css, StandardCharsets.UTF_8));
        }
        builder.append("</style></head><body>");
    }

    private static void generateSubscriptionLines(ManagedLedgerInternalStats persistentTopicInternalStats,
                                           TopicStats persistentTopicStats,
                                           StringBuilder builder,
                                           long totalEntries) {
        List<Pair<EntriesStatsByStatus, StringBuilder>> subscriptionsLines = new ArrayList<>();
        persistentTopicInternalStats.cursors.forEach((name, cursor) -> {
            StringBuilder subBuilder = new StringBuilder();
            final EntriesStatsByStatus entriesStatsByStatus =
                    generateSubscriptionLine(subBuilder, persistentTopicInternalStats,
                    persistentTopicStats, totalEntries, name, cursor);
            subscriptionsLines.add(Pair.of(entriesStatsByStatus, subBuilder));
        });
        subscriptionsLines.sort(Comparator
                .comparing((Pair<EntriesStatsByStatus, StringBuilder> pair) -> pair.getLeft().totalAhead)
                .reversed());

        subscriptionsLines.forEach(pair -> builder.append(pair.getRight()));
    }

    private static void generateTopicLine(String topic,
                                          ManagedLedgerInternalStats persistentTopicInternalStats,
                                          StringBuilder builder,
                                          long totalEntries) {
        builder.append("<div class=\"line-container\"><h4>");
        builder.append(topic);
        builder.append("</h4><div class=\"topic\">");
        final LedgerSegmentProgress ledgerSegmentProgress = new LedgerSegmentProgress();
        boolean first = true;
        for (ManagedLedgerInternalStats.LedgerInfo ledger : persistentTopicInternalStats.ledgers) {
            builder.append(TopicSubscriptionsVisualizerHtmlUtil.genLedgerSegment(ledger,
                    totalEntries, ledgerSegmentProgress, first));
            first = false;
        }
        builder.append("</div></div>");
    }

    private static EntriesStatsByStatus generateSubscriptionLine(
            StringBuilder builder,
            ManagedLedgerInternalStats persistentTopicInternalStats,
            TopicStats persistentTopicStats,
            long totalEntries,
            String name,
            ManagedLedgerInternalStats.CursorStats cursor) {
        Map<Long, Map<Long, EntryStatus>> entryStatuses = new HashMap<>();
        final Position markDeletePos = Position.fromString(cursor.markDeletePosition);
        final Position readPos = Position.fromString(cursor.readPosition);

        final ObjectMapper mapper = ObjectMapperFactory.getThreadLocal();

        for (ManagedLedgerInternalStats.LedgerInfo ledger : persistentTopicInternalStats.ledgers) {
            Map<Long, EntryStatus> entryStatusesForLedger = new HashMap<>();
            entryStatuses.put(ledger.ledgerId, entryStatusesForLedger);

            if (markDeletePos.ledgerId > ledger.ledgerId) {
                for (long i = 0; i < ledger.entries; i++) {
                    entryStatusesForLedger.put(i, EntryStatus.ACKED);
                }
            } else if (markDeletePos.ledgerId == ledger.ledgerId) {
                for (long i = 0; i < markDeletePos.entryId + 1; i++) {
                    entryStatusesForLedger.put(i, EntryStatus.ACKED);
                }
            } else {
                for (long i = 0; i < ledger.entries; i++) {
                    entryStatusesForLedger.put(i, EntryStatus.AHEAD);
                }
            }

            if (readPos.ledgerId > ledger.ledgerId) {
                for (long i = 0; i < ledger.entries; i++) {
                    if (!entryStatusesForLedger.containsKey(i)) {
                        entryStatusesForLedger.put(i, EntryStatus.AHEAD);
                    }
                }
            } else if (readPos.ledgerId == ledger.ledgerId) {
                for (long i = 0; i < readPos.entryId - 1; i++) {
                    if (!entryStatusesForLedger.containsKey(i)) {
                        entryStatusesForLedger.put(i, EntryStatus.AHEAD);
                    }
                }
            }

        }
        parseRanges(cursor.individuallyDeletedMessages).forEach(range -> {
            for (long i = range.getLeft().entryId; i < range.getRight().entryId; i++) {
                final long ledgerId = range.getLeft().ledgerId;
                final Map<Long, EntryStatus> entryStatusMap = entryStatuses
                        .computeIfAbsent(ledgerId, l -> new HashMap<>());
                entryStatusMap.put(i, EntryStatus.INDIVIDUALLLY_DELETED);
            }
        });
        EntriesStatsByStatus entriesStatsByStatus = new EntriesStatsByStatus();

        final String line = TopicSubscriptionsVisualizerHtmlUtil.genSubscriptionLine(entryStatuses,
                persistentTopicInternalStats.ledgers, totalEntries, entriesStatsByStatus);

        Map<String, Map<String, Object>> details = new LinkedHashMap<>();
        details.put("Subscription", mapper.convertValue(persistentTopicStats.getSubscriptions().get(name), Map.class));
        details.put("Cursor", mapper.convertValue(cursor, Map.class));
        final String description = TopicSubscriptionsVisualizerHtmlUtil.genSubscriptionDescription(name,
                entriesStatsByStatus.totalAhead, details);
        builder.append("<div class=\"line-container\">");
        builder.append(description);
        builder.append(line);
        builder.append("</div>");
        return entriesStatsByStatus;
    }

    private static List<Pair<Position, Position>> parseRanges(String str) {
        List<Pair<Position, Position>> result = new ArrayList<>();
        str = str
                .replace("[", "")
                .replace("]", "")
                .replace("(", "")
                .replace(")", "");
        if (str.isEmpty()) {
            return result;
        }
        final String[] splitByComma = str.split(",");
        for (String range : splitByComma) {
            final String[] splitRange = range.split("\\.\\.");
            result.add(Pair.of(Position.fromString(splitRange[0]),
                            Position.fromString(splitRange[1])));
        }
        return result;
    }
}
