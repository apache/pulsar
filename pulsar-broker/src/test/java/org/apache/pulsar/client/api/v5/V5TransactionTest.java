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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import java.time.Duration;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for the V5 transaction wire: opening a transaction on a transaction-enabled
 * client, sending transactional messages through the V5 producer, and committing or
 * aborting the transaction.
 *
 * <p><b>Known broker gap (not exercised here):</b> subscribing to a {@code segment://}
 * scalable-topic segment when the client has transactions enabled hangs in the broker —
 * the per-segment TransactionBuffer setup doesn't compose with the scalable-topic
 * segment domain. Until that's fixed, V5 transactional consumer flows can't be tested
 * end-to-end on scalable topics.
 */
public class V5TransactionTest extends V5ClientBaseTest {

    @Test
    public void testAbortMovesTransactionToAbortedState() throws Exception {
        // Open a transaction-enabled V5 client, send transactional messages through the
        // V5 producer to a scalable topic, then abort. The transaction state must move
        // OPEN → ABORTED — exercises the V5 newTransaction / message-builder
        // transaction / Transaction.abort path end-to-end without going through the
        // broker subscribe-with-TB path (see class-level "known gap" note).
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .transactionPolicy(new TransactionPolicy(Duration.ofMinutes(1)))
                .build();
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();

        Transaction txn = client.newTransaction();
        assertEquals(txn.state(), Transaction.State.OPEN);
        for (int i = 0; i < 5; i++) {
            producer.newMessage().transaction(txn).value("v-" + i).send();
        }
        txn.abort();
        assertEquals(txn.state(), Transaction.State.ABORTED);
    }
}
