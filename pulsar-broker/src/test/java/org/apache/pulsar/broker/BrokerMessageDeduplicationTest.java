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
package org.apache.pulsar.broker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.MessageDeduplication;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BrokerMessageDeduplicationTest {

    private ManagedLedger managedLedger;
    private MessageDeduplication deduplication;
    private ScheduledExecutorService executor;

    @BeforeMethod
    public void setUp() {
        final var pulsarService = mock(PulsarService.class);
        final var configuration = new ServiceConfiguration();
        configuration.setBrokerDeduplicationEntriesInterval(10);
        doReturn(configuration).when(pulsarService).getConfiguration();
        executor = Executors.newScheduledThreadPool(1, new ExecutorProvider.ExtendedThreadFactory("pulsar"));
        doReturn(executor).when(pulsarService).getExecutor();
        managedLedger = mock(ManagedLedger.class);
        final var mockTopic = mock(PersistentTopic.class);
        doReturn(true).when(mockTopic).isDeduplicationEnabled();
        deduplication = spy(new MessageDeduplication(pulsarService, mockTopic, managedLedger));
        doReturn(true).when(deduplication).isEnabled();
    }

    @AfterMethod
    public void tearDown() {
        executor.shutdown();
    }

    @Test
    public void markerMessageNotDeduplicated() {
        Topic.PublishContext context = mock(Topic.PublishContext.class);
        doReturn(true).when(context).isMarkerMessage();
        MessageDeduplication.MessageDupStatus status = deduplication.isDuplicate(context, null);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
    }

    @Test
    public void markerMessageNotRecordPersistent() {
        Topic.PublishContext context = mock(Topic.PublishContext.class);
         // marker message don't record message persisted.
        doReturn(true).when(context).isMarkerMessage();
        deduplication.recordMessagePersisted(context, null);

        // if is not a marker message, we will get NPE. because context is mocked with null value fields.
        doReturn(false).when(context).isMarkerMessage();
        try {
            deduplication.recordMessagePersisted(context, null);
            fail();
        } catch (Exception npe) {
            assertTrue(npe instanceof NullPointerException);
        }
    }

    @Test
    public void checkStatusFail() throws Exception {
        final var cursor = mock(ManagedCursor.class);
        doAnswer(invocation -> {
            ((AsyncCallbacks.OpenCursorCallback) invocation.getArgument(1)).openCursorComplete(cursor, null);
            return null;
        }).when(managedLedger).asyncOpenCursor(any(), any(), any());
        doReturn(true).when(cursor).hasMoreEntries();
        doReturn(Map.of()).when(cursor).getProperties();
        try {
            doAnswer(invocation -> {
                throw new RuntimeException("asyncReadEntries failed");
            }).when(cursor).asyncReadEntries(anyInt(), any(), any(), any());
            deduplication.checkStatus().get(3, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertTrue(e.getMessage().contains("asyncReadEntries failed"));
        }
    }
}
