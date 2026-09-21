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
package org.apache.pulsar.metadata.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class PulsarLedgerManagerTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl", timeOut = 30000)
    public void testDuplicateCreationPreservesExistingMetadata(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (var store = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
             var manager = new PulsarLedgerManager(store, "/ledgers-" + UUID.randomUUID())) {
            long ledgerId = 123L;
            var original = manager.createLedgerMetadata(ledgerId, metadata(ledgerId, "original"))
                    .get(10, TimeUnit.SECONDS);

            assertThatThrownBy(() -> manager.createLedgerMetadata(ledgerId, metadata(ledgerId, "replacement"))
                    .get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKLedgerExistException.class);

            var persisted = manager.readLedgerMetadata(ledgerId).get(10, TimeUnit.SECONDS);
            assertThat(persisted.getVersion()).isEqualTo(original.getVersion());
            assertThat(persisted.getValue().getCustomMetadata().get("value"))
                    .isEqualTo("original".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void testStaleVersionsRemainMetadataVersionErrors(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (var store = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
             var manager = new PulsarLedgerManager(store, "/ledgers-" + UUID.randomUUID())) {
            long ledgerId = 123L;
            var original = manager.createLedgerMetadata(ledgerId, metadata(ledgerId, "original"))
                    .get(10, TimeUnit.SECONDS);
            manager.writeLedgerMetadata(ledgerId, metadata(ledgerId, "updated"), original.getVersion())
                    .get(10, TimeUnit.SECONDS);

            assertThatThrownBy(() -> manager.writeLedgerMetadata(ledgerId, original.getValue(), original.getVersion())
                    .get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKMetadataVersionException.class);
            assertThatThrownBy(() -> manager.removeLedgerMetadata(ledgerId, original.getVersion())
                    .get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKMetadataVersionException.class);

            assertThat(manager.readLedgerMetadata(ledgerId).get(10, TimeUnit.SECONDS)
                    .getValue().getCustomMetadata().get("value"))
                    .isEqualTo("updated".getBytes(StandardCharsets.UTF_8));
        }
    }

    private static LedgerMetadata metadata(long ledgerId, String value) {
        return LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0])
                .withCustomMetadata(Map.of("value", value.getBytes(StandardCharsets.UTF_8)))
                .newEnsembleEntry(0L, List.of(BookieId.parse("bookie:3181")))
                .build();
    }
}
