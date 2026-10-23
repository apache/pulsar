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
package org.apache.pulsar.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ConsumerAcknowledgmentGroupSizeTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] acknowledgmentGroups() {
        return new Object[][] {
            {true, true, true},
            {true, false, true},
            {true, true, false},
            {false, false, true},
            {false, true, false}
        };
    }

    @Test(dataProvider = "acknowledgmentGroups", timeOut = 60000)
    public void testFlushAtMaxGroupSize(boolean listAck, boolean firstBatch, boolean secondBatch) throws Exception {
        String topic = newTopicName();
        try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topic)
                     .subscriptionName("sub")
                     .subscriptionType(SubscriptionType.Shared)
                     .enableBatchIndexAcknowledgment(true)
                     .isAckReceiptEnabled(true)
                     .maxAcknowledgmentGroupSize(2)
                     .acknowledgmentGroupTime(1, TimeUnit.HOURS)
                     .subscribe();
             Producer<String> batchProducer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topic)
                     .enableBatching(true)
                     .batchingMaxMessages(2)
                     .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                     .create();
             Producer<String> individualProducer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topic)
                     .enableBatching(false)
                     .create()) {
            List<MessageId> toAcknowledge = new ArrayList<>();
            Set<String> unacknowledged = new HashSet<>();
            boolean[] batches = {firstBatch, secondBatch};
            for (int entry = 0; entry < batches.length; entry++) {
                String value = "entry-" + entry;
                if (batches[entry]) {
                    CompletableFuture<MessageId> first = batchProducer.sendAsync(value + "-0");
                    CompletableFuture<MessageId> second = batchProducer.sendAsync(value + "-1");
                    batchProducer.flush();
                    CompletableFuture.allOf(first, second).get(10, TimeUnit.SECONDS);
                    unacknowledged.add(value + "-1");
                } else {
                    individualProducer.send(value + "-0");
                }
                Message<String> first = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(first).isNotNull();
                assertThat(first.getValue()).isEqualTo(value + "-0");
                MessageIdAdv firstId = (MessageIdAdv) first.getMessageId();
                assertThat(firstId.getBatchIndex()).isEqualTo(batches[entry] ? 0 : -1);
                toAcknowledge.add(firstId);
                if (batches[entry]) {
                    Message<String> second = consumer.receive(10, TimeUnit.SECONDS);
                    assertThat(second).isNotNull();
                    assertThat(second.getValue()).isEqualTo(value + "-1");
                    MessageIdAdv secondId = (MessageIdAdv) second.getMessageId();
                    assertThat(secondId.getLedgerId()).isEqualTo(firstId.getLedgerId());
                    assertThat(secondId.getEntryId()).isEqualTo(firstId.getEntryId());
                    assertThat(secondId.getBatchIndex()).isEqualTo(1);
                    assertThat(firstId.getBatchSize()).isEqualTo(2);
                }
            }
            MessageIdAdv firstId = (MessageIdAdv) toAcknowledge.get(0);
            MessageIdAdv secondId = (MessageIdAdv) toAcknowledge.get(1);
            assertThat(firstId.getEntryId()).isNotEqualTo(secondId.getEntryId());

            CompletableFuture<Void> acknowledgment;
            if (listAck) {
                acknowledgment = consumer.acknowledgeAsync(toAcknowledge);
            } else {
                CompletableFuture<Void> first = consumer.acknowledgeAsync(toAcknowledge.get(0));
                assertThat(first.isDone()).as("one pending entry stays below the group size limit").isFalse();
                acknowledgment = CompletableFuture.allOf(first, consumer.acknowledgeAsync(toAcknowledge.get(1)));
            }
            // This completes only after the broker returns an ACK receipt. The one-hour timer cannot help.
            acknowledgment.get(10, TimeUnit.SECONDS);

            consumer.redeliverUnacknowledgedMessages();
            int remaining = unacknowledged.size();
            for (int i = 0; i < remaining; i++) {
                Message<String> redelivered = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(redelivered).isNotNull();
                assertThat(unacknowledged.remove(redelivered.getValue()))
                        .as("only the unacknowledged batch indexes are redelivered").isTrue();
            }
            assertThat(unacknowledged).isEmpty();
            assertThat(consumer.receive(500, TimeUnit.MILLISECONDS)).isNull();
        }
    }
}
