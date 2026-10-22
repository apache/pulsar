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
package org.apache.pulsar.broker.service.schema;

import static org.testng.Assert.assertEquals;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.MultiBrokerTestZKBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SchemaMetadataMultiBrokerTest extends MultiBrokerTestZKBaseTest {

    @Override
    protected int numberOfAdditionalBrokers() {
        return 1;
    }

    @Test
    public void testSchemaMetadataChangeIsNotifiedToAllBrokers() throws Exception {
        String schemaId = "public/default/schema-metadata-sync";
        AtomicInteger notifications = new AtomicInteger();
        List<PulsarService> brokers = List.of(getPulsar(), additionalBrokers.get(0));
        List<Runnable> cancelListeners = brokers.stream()
                .map(broker -> registerSchemaMetadataListener(broker, schemaId, notifications))
                .toList();
        try {
            SchemaData schema = SchemaData.builder()
                    .type(SchemaType.STRING)
                    .data(new byte[0])
                    .user("test")
                    .build();
            getPulsar().getSchemaRegistryService()
                    .putSchemaIfAbsent(schemaId, schema, SchemaCompatibilityStrategy.FULL)
                    .get();

            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertEquals(notifications.get(), brokers.size()));
        } finally {
            cancelListeners.forEach(Runnable::run);
        }
    }

    private static Runnable registerSchemaMetadataListener(PulsarService broker, String schemaId,
                                                            AtomicInteger notifications) {
        AtomicBoolean notified = new AtomicBoolean();
        return broker.getLocalMetadataStore().registerCancellableListener(notification -> {
            if (isSchemaMetadataChange(notification, schemaId) && notified.compareAndSet(false, true)) {
                notifications.incrementAndGet();
            }
        });
    }

    private static boolean isSchemaMetadataChange(Notification notification, String schemaId) {
        return BookkeeperSchemaStorage.schemaIdFromPath(notification.getPath()).filter(schemaId::equals).isPresent()
                && (notification.getType() == NotificationType.Created
                || notification.getType() == NotificationType.Modified
                || notification.getType() == NotificationType.Deleted);
    }
}
