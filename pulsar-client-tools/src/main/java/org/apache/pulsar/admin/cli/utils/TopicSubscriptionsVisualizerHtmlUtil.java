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

import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;

class TopicSubscriptionsVisualizerHtmlUtil {

    static String genSubscriptionLine(Map<Long, Map<Long, TopicSubscriptionsVisualizer.EntryStatus>> entryStatuses,
                                             List<ManagedLedgerInternalStats.LedgerInfo> ledgers,
                                             long totalEntries,
                                             TopicSubscriptionsVisualizer.EntriesStatsByStatus entriesStatsByStatus) {

        StringBuilder builder = new StringBuilder("<div class=\"subscription\">");
        final float singleEntryPercent = (float) 100 / totalEntries;

        TopicSubscriptionsVisualizer.EntryStatus currentStatus = null;
        long currentCount = 0;
        for (ManagedLedgerInternalStats.LedgerInfo ledger : ledgers) {
            for (long i = 0; i < ledger.entries; i++) {
                TopicSubscriptionsVisualizer.EntryStatus entryStatus = entryStatuses.get(ledger.ledgerId).get(i);
                if (entryStatus == null) {
                    entryStatus = TopicSubscriptionsVisualizer.EntryStatus.AHEAD;
                }

                if (currentStatus == null) {
                    currentStatus = entryStatus;
                }

                if (entryStatus == currentStatus) {
                    currentCount++;
                    continue;
                }
                if (currentStatus == TopicSubscriptionsVisualizer.EntryStatus.ACKED
                        || currentStatus == TopicSubscriptionsVisualizer.EntryStatus.INDIVIDUALLLY_DELETED) {
                    String cssClass;
                    if (currentStatus == TopicSubscriptionsVisualizer.EntryStatus.ACKED) {
                        entriesStatsByStatus.totalAcked += currentCount;
                        cssClass = "acked";
                    } else {
                        entriesStatsByStatus.totalIndividuallyDeleted += currentCount;
                        cssClass = "idel";
                    }
                    builder.append("<div class=\"segment-" + cssClass + "\" style=\"width: ");
                    builder.append(singleEntryPercent * currentCount);
                    builder.append("%;\"></div>");
                } else {
                    entriesStatsByStatus.totalAhead += currentCount;
                    builder.append("<div class=\"segment-ahead\" style=\"width: ");
                    builder.append(singleEntryPercent * currentCount);
                    builder.append("%;\"></div>");
                }
                currentCount = 1;
                currentStatus = entryStatus;
            }
        }
        if (currentCount > 0) {

            if (currentStatus == TopicSubscriptionsVisualizer.EntryStatus.ACKED
                    || currentStatus == TopicSubscriptionsVisualizer.EntryStatus.INDIVIDUALLLY_DELETED) {
                String cssClass;
                if (currentStatus == TopicSubscriptionsVisualizer.EntryStatus.ACKED) {
                    entriesStatsByStatus.totalAcked += currentCount;
                    cssClass = "acked";
                } else {
                    entriesStatsByStatus.totalIndividuallyDeleted += currentCount;
                    cssClass = "idel";
                }
                builder.append("<div class=\"segment-" + cssClass + "\" style=\"width: ");
                builder.append(singleEntryPercent * currentCount);
                builder.append("%;\"></div>");
            } else {
                entriesStatsByStatus.totalAhead += currentCount;
                builder.append("<div class=\"segment-ahead\" style=\"width: ");
                builder.append(singleEntryPercent * currentCount);
                builder.append("%;\"></div>");
            }

        }
        builder.append("</div>");
        return builder.toString();
    }

     static String genLedgerSegment(ManagedLedgerInternalStats.LedgerInfo ledger,
                                          long totalEntries,
                                          TopicSubscriptionsVisualizer.LedgerSegmentProgress ledgerSegmentProgress,
                                          boolean addFirstClass) {
        String tooltip = genLedgerInfoDescription(ledger);
        double percentWidth = (double) 100 / totalEntries * ledger.entries;
        ledgerSegmentProgress.ledgersMapping.put(ledger.ledgerId, new TopicSubscriptionsVisualizer.EntryWidthMapping());
        for (long i = 0; i < ledger.entries; i++) {
            ledgerSegmentProgress.ledgersMapping.get(ledger.ledgerId)
                    .put(i, ledgerSegmentProgress.currentWidth++);
        }

        return "<div class=\"ledger"
                + (addFirstClass ? " first" : "")
                + " tooltip\" style=\"width: "
                + percentWidth + "%\">"
                + "<span class=\"tooltiptext\">"
                + tooltip
                + "</span></div>";
    }


    static String genLedgerInfoDescription(ManagedLedgerInternalStats.LedgerInfo ledger) {
        return genDetailsDescription(ObjectMapperFactory.getThreadLocal()
                .convertValue(ledger, Map.class));

    }

    static String genDetailsDescription(Map<String, Object> details) {
        StringBuilder builder = new StringBuilder("<div class=\"description-details\">");
        details.forEach((k, v) -> {
            builder.append("<div class=\"detail-item\"><span class=\"detail-item-key\">");
            builder.append(escapeHtml(k));
            builder.append("</span><span class=\"detail-item-value\">");
            builder.append(v == null ? "" : escapeHtml(v.toString()));
            builder.append("</span></div>");
        });
        builder.append("</div>");
        return builder.toString();
    }

    static String genSubscriptionDescription(String name, long distance, Map<String, Map<String, Object>> details) {
        StringBuilder builder = new StringBuilder("<div class=\"description\"><details><summary class=\"title\">");
        builder.append(escapeHtml(name));
        builder.append("</summary><div class=\"details-sections-container\">");
        details.forEach((k, data) -> {
            builder.append("<div class=\"details-section\"><span class=\"title\">");
            builder.append(escapeHtml(k));
            builder.append("</span>");
            builder.append(genDetailsDescription(data));
            builder.append("</div>");
        });
        builder.append("</div></details><span>" + (distance == 0 ? "0" : "-" + distance) + "</span>");
        builder.append("</div>");
        return builder.toString();
    }

    private static String escapeHtml(String str) {
        return StringEscapeUtils.escapeHtml(str);
    }
}
