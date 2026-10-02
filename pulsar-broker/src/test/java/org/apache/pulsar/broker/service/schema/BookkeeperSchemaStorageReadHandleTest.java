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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker", timeOut = 30000)
public class BookkeeperSchemaStorageReadHandleTest {
    @DataProvider(name = "readResults")
    public Object[][] readResults() {
        return new Object[][] {
                {BKException.Code.OK, false, BKException.Code.OK},
                {BKException.Code.ReadException, false, BKException.Code.OK},
                {BKException.Code.OK, true, BKException.Code.OK},
                {BKException.Code.ReadException, false, BKException.Code.UnexpectedConditionException},
                {BKException.Code.OK, false, BKException.Code.UnexpectedConditionException}
        };
    }

    @Test(dataProvider = "readResults")
    public void testReadHandleClosedOnEveryOutcome(int readRc, boolean parseFailure, int closeRc) throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create("memory:schema-read-handles",
                MetadataStoreConfig.builder().build())) {
            BookKeeper bookKeeper = mock(BookKeeper.class);
            LedgerHandle handle = mock(LedgerHandle.class);
            when(handle.getId()).thenReturn(1L);
            doAnswer(invocation -> {
                AsyncCallback.OpenCallback callback = invocation.getArgument(3);
                callback.openComplete(BKException.Code.OK, handle, invocation.getArgument(4));
                return null;
            }).when(bookKeeper).asyncOpenLedger(eq(1L), any(), any(), any(), isNull(), eq(true));

            byte[] schemaData = new byte[] {1, 2, 3};
            LedgerEntry entry = mock(LedgerEntry.class);
            if (parseFailure) {
                when(entry.getEntry()).thenThrow(new IllegalStateException("schema decode failed"));
            } else {
                when(entry.getEntry()).thenReturn(new SchemaEntry().setSchemaData(schemaData).toByteArray());
            }
            doAnswer(invocation -> {
                AsyncCallback.ReadCallback callback = invocation.getArgument(2);
                callback.readComplete(readRc, handle, Collections.enumeration(List.of(entry)),
                        invocation.getArgument(3));
                return null;
            }).when(handle).asyncReadEntries(eq(0L), eq(0L), any(), isNull());
            doAnswer(invocation -> {
                AsyncCallback.CloseCallback callback = invocation.getArgument(0);
                callback.closeComplete(closeRc, handle, invocation.getArgument(1));
                return null;
            }).when(handle).asyncClose(any(), isNull());

            SchemaLocator locator = new SchemaLocator();
            locator.setInfo().setVersion(0).setHash(new byte[0]);
            locator.getInfo().setPosition().setLedgerId(1).setEntryId(0);
            locator.addIndex().copyFrom(locator.getInfo());
            store.put("/schemas/test", locator.toByteArray(), Optional.empty()).get(5, TimeUnit.SECONDS);

            BookKeeperClientFactory clientFactory = mock(BookKeeperClientFactory.class);
            when(clientFactory.create(any(), eq(store), isNull(), eq(Optional.empty()), isNull()))
                    .thenReturn(CompletableFuture.completedFuture(bookKeeper));
            PulsarService pulsar = mock(PulsarService.class);
            when(pulsar.getLocalMetadataStore()).thenReturn(store);
            when(pulsar.getConfiguration()).thenReturn(new ServiceConfiguration());
            when(pulsar.getBookKeeperClientFactory()).thenReturn(clientFactory);
            BookkeeperSchemaStorage storage = new BookkeeperSchemaStorage(pulsar);
            storage.start();
            try {
                CompletableFuture<StoredSchema> read = storage.getAll("test").get(5, TimeUnit.SECONDS).get(0);
                if (readRc != BKException.Code.OK) {
                    assertThatThrownBy(() -> read.get(5, TimeUnit.SECONDS))
                            .hasRootCauseMessage(BookkeeperSchemaStorage.bkException("Failed to read entry", readRc,
                                    1, 0, false).getMessage());
                } else if (parseFailure) {
                    assertThatThrownBy(() -> read.get(5, TimeUnit.SECONDS))
                            .hasRootCauseMessage("schema decode failed");
                } else if (closeRc != BKException.Code.OK) {
                    assertThatThrownBy(() -> read.get(5, TimeUnit.SECONDS))
                            .hasRootCauseMessage(BookkeeperSchemaStorage.bkException("Failed to close ledger", closeRc,
                                    1, -1, false).getMessage());
                } else {
                    assertThat(read.get(5, TimeUnit.SECONDS).data).containsExactly(schemaData);
                }
                verify(handle).asyncClose(any(), isNull());
            } finally {
                storage.close();
            }
        }
    }
}
