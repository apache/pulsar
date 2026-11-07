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
package org.apache.pulsar.broker.stats.prometheus;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespaceStatsAggregatorLastPublishTimeTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] includeTopicMetrics() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "includeTopicMetrics")
    public void testScrapeDoesNotReadLastPublishTime(boolean includeTopicMetrics) throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        // Keep a backlog without an active consumer, so reloading retains the message without dispatch reads.
        admin.topics().createSubscription(topicName, "subscription", MessageId.earliest);
        long beforePublish = System.currentTimeMillis();
        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false).create()) {
            producer.send(new byte[] {1});
        }
        long afterPublish = System.currentTimeMillis();

        admin.topics().unload(topicName);
        PersistentTopic topic = (PersistentTopic) getTopicIfExists(topicName)
                .get(30, TimeUnit.SECONDS).orElseThrow();
        ManagedLedger ledger = topic.getManagedLedger();
        assertThat(ledger.getLastAddEntryTime()).as("reloaded topic has not received new writes").isZero();
        assertThat(ledger.getLastConfirmedEntry().getEntryId()).as("reloaded topic retains its message")
                .isGreaterThanOrEqualTo(0);
        long readsBeforeScrape = ledger.getStats().getEntriesReadTotalCount();

        @Cleanup("releaseAll")
        PrometheusMetricStreams stream = new PrometheusMetricStreams();
        NamespaceStatsAggregator.generate(getPulsar(), includeTopicMetrics, false, false, false, stream);

        assertThat(ledger.getStats().getEntriesReadTotalCount())
                .as("metrics must not read the unused last publish timestamp")
                .isEqualTo(readsBeforeScrape);

        TopicStats adminStats = admin.topics().getStats(topicName);
        assertThat(adminStats.getLastPublishTimeStamp()).as("Admin stats still include the last publish timestamp")
                .isBetween(beforePublish, afterPublish);
        assertThat(ledger.getStats().getEntriesReadTotalCount()).as("Admin stats read the timestamp from storage")
                .isEqualTo(readsBeforeScrape + 1);
        NamespaceStatsAggregator.generate(getPulsar(), includeTopicMetrics, false, false, false, stream);
        assertThat(admin.topics().getStats(topicName).getLastPublishTimeStamp())
                .isEqualTo(adminStats.getLastPublishTimeStamp());
        assertThat(ledger.getStats().getEntriesReadTotalCount()).as("scraping preserves the cached Admin timestamp")
                .isEqualTo(readsBeforeScrape + 1);
    }
}
