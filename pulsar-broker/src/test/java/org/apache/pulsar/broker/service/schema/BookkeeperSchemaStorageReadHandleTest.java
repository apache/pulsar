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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker", timeOut = 30000)
public class BookkeeperSchemaStorageReadHandleTest {
    @DataProvider(name = "readResults")
    public Object[][] readResults() {
        return new Object[][] {
                {"success"}, {"read-failure"}, {"decode-failure"}, {"read-and-close-failure"},
                {"decode-and-close-failure"}, {"close-failure"}, {"read-throws"}, {"close-throws"}, {"open-failure"}
        };
    }

    @Test(dataProvider = "readResults")
    public void testReadHandleClosedOnEveryOutcome(String outcome) throws Exception {
        boolean readFailure = outcome.equals("read-failure") || outcome.equals("read-and-close-failure");
        boolean parseFailure = outcome.startsWith("decode-");
        int readRc = readFailure ? BKException.Code.ReadException : BKException.Code.OK;
        int closeRc = outcome.endsWith("close-failure")
                ? BKException.Code.UnexpectedConditionException : BKException.Code.OK;
        try (MetadataStoreExtended store = MetadataStoreExtended.create("memory:schema-read-handles",
                MetadataStoreConfig.builder().build())) {
            BookKeeper bookKeeper = mock(BookKeeper.class);
            LedgerHandle handle = mock(LedgerHandle.class);
            when(handle.getId()).thenReturn(1L);
            doAnswer(invocation -> {
                AsyncCallback.OpenCallback callback = invocation.getArgument(3);
                if (outcome.equals("open-failure")) {
                    callback.openComplete(BKException.Code.NoSuchLedgerExistsException, null,
                            invocation.getArgument(4));
                } else {
                    callback.openComplete(BKException.Code.OK, handle, invocation.getArgument(4));
                }
                return null;
            }).when(bookKeeper).asyncOpenLedger(eq(1L), any(), any(), any(), isNull(), eq(true));

            byte[] schemaData = new byte[] {1, 2, 3};
            byte[] malformedData = new byte[] {(byte) 0xff};
            LedgerEntry entry = mock(LedgerEntry.class);
            if (parseFailure) {
                when(entry.getEntry()).thenReturn(malformedData);
            } else {
                when(entry.getEntry()).thenReturn(new SchemaEntry().setSchemaData(schemaData).toByteArray());
            }
            CompletableFuture<AsyncCallback.ReadCallback> readStarted = new CompletableFuture<>();
            CompletableFuture<AsyncCallback.CloseCallback> closeStarted = new CompletableFuture<>();
            doAnswer(invocation -> {
                if (outcome.equals("read-throws")) {
                    throw new IllegalStateException("read threw synchronously");
                }
                readStarted.complete(invocation.getArgument(2));
                return null;
            }).when(handle).asyncReadEntries(eq(0L), eq(0L), any(), isNull());
            doAnswer(invocation -> {
                if (outcome.equals("close-throws")) {
                    throw new IllegalStateException("close threw synchronously");
                }
                closeStarted.complete(invocation.getArgument(0));
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
                if (outcome.equals("open-failure")) {
                    Throwable error = read.handle((ignored, failure) -> FutureUtil.unwrapCompletionException(failure))
                            .get(5, TimeUnit.SECONDS);
                    assertThat(error).hasMessage(BookkeeperSchemaStorage.bkException("Failed to open ledger",
                            BKException.Code.NoSuchLedgerExistsException, 1, -1, false).getMessage());
                    verify(handle, never()).asyncReadEntries(anyLong(), anyLong(), any(), any());
                    verify(handle, never()).asyncClose(any(), any());
                    return;
                }
                if (!outcome.equals("read-throws")) {
                    AsyncCallback.ReadCallback callback = readStarted.get(5, TimeUnit.SECONDS);
                    assertThat(read).isNotDone();
                    verify(handle, never()).asyncClose(any(), any());
                    callback.readComplete(readRc, handle, Collections.enumeration(List.of(entry)), null);
                }
                if (!outcome.equals("close-throws")) {
                    AsyncCallback.CloseCallback callback = closeStarted.get(5, TimeUnit.SECONDS);
                    assertThat(read).isNotDone();
                    callback.closeComplete(closeRc, handle, null);
                }
                Throwable error = read.handle((ignored, failure) -> FutureUtil.unwrapCompletionException(failure))
                        .get(5, TimeUnit.SECONDS);
                if (readRc != BKException.Code.OK) {
                    assertThat(error)
                            .hasMessage(BookkeeperSchemaStorage.bkException("Failed to read entry", readRc,
                                    1, 0, false).getMessage());
                } else if (parseFailure) {
                    Throwable parseError = catchThrowable(() -> new SchemaEntry().parseFrom(malformedData));
                    assertThat(parseError).isNotNull();
                    assertThat(error).isExactlyInstanceOf(parseError.getClass()).hasMessage(parseError.getMessage());
                } else if (outcome.endsWith("throws")) {
                    assertThat(error).isInstanceOf(IllegalStateException.class)
                            .hasMessage(outcome.equals("read-throws")
                                    ? "read threw synchronously" : "close threw synchronously");
                } else if (closeRc != BKException.Code.OK) {
                    assertThat(error)
                            .hasMessage(BookkeeperSchemaStorage.bkException("Failed to close ledger", closeRc,
                                    1, -1, false).getMessage());
                } else {
                    assertThat(error).isNull();
                    assertThat(read.get(5, TimeUnit.SECONDS).data).containsExactly(schemaData);
                }
                if (error != null) {
                    if ((readFailure || parseFailure) && closeRc != BKException.Code.OK) {
                        assertThat(error.getSuppressed()).singleElement().satisfies(suppressed ->
                                assertThat(suppressed).hasMessage(BookkeeperSchemaStorage.bkException(
                                        "Failed to close ledger", closeRc, 1, -1, false).getMessage()));
                    } else {
                        assertThat(error.getSuppressed()).isEmpty();
                    }
                }
                verify(handle).asyncClose(any(), isNull());
            } finally {
                storage.close();
            }
        }
    }
}
